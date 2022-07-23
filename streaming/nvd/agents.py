from ..main import app
from ..config import config
from datetime import datetime, timedelta
from typing import List, Dict
from .utils.api import NVD
from .utils.transforms import CVE
from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import aiohttp

raw_cve_results = app.topic('streaming-nvd-response')
cve_results_enriched = app.topic('streaming-nvd-response-enriched')

context = create_default_context(cafile=config['elasticsearch']['ca'])
es_handler = Elasticsearch([config['elasticsearch']['uri']], ssl_context=context)

async def get_entities(base_uri, text):
    uri = f'{base_uri}/extract'
    async with aiohttp.ClientSession() as session:
        async with session.post(uri, json={'text': text}) as resp:
            return await resp.json()

@app.agent(cve_results_enriched)
async def process_nvd_enriched(cves):
    async for cve in cves:
        elastic_doc = {**cve, '_index': config['nvd']['index'], '_id': cve['fingerprint']}
        helpers.bulk(es_handler, [elastic_doc], chunk_size=1000)
  
@app.agent(raw_cve_results)
async def process(entries):
    ''' Process each CVE and split the impacted products and references, index afterwards '''
    async for entry in entries:
        cve_description = '\n'.join([desc['value'] for desc in entry['cve']['description']['description_data']])
        extract_nlp = await get_entities(config['nlp']['uri'], cve_description)
        cve = CVE(cve={**entry, **extract_nlp})
        references = [{**doc, '_index': config['nvd']['index'], '_id': doc['fingerprint']}
            for doc in cve.references]
        impacted = [{**doc, '_index': config['nvd']['index'], '_id': doc['fingerprint']}
            for doc in cve.impacted]
        documents = references + impacted
        helpers.bulk(es_handler, documents, chunk_size=1000)
        await cve_results_enriched.send(value=cve.details)

@app.timer(interval=10.0)
async def get_entries() -> List[Dict]:
    ''' Get the latest CVEs from the NVD API '''
    init_date = datetime.utcnow() - timedelta(days=config['nvd']['days'])
    init_date_fmt = init_date.strftime('%Y-%m-%dT%H:%M:%S:000 UTC-00:00')
    end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S:000 UTC-00:00')
    params = { 'resultsPerPage': config['nvd']['max_results'],
        'modStartDate': init_date_fmt, 'modEndDate':end_date }

    with NVD(params) as nvd:
        all_cves = nvd.cves
        for cve in all_cves:
            await raw_cve_results.send(value=cve)


