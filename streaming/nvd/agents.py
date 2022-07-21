from ..main import app
from ..config import config
from datetime import datetime, timedelta
from typing import List, Dict
from .utils.api import NVD
from .utils.transforms import CVE
from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context

raw_cve_results = app.topic('streaming-nvd-response')

context = create_default_context(cafile=config['elasticsearch']['ca'])
es_handler = Elasticsearch([config['elasticsearch']['uri']], ssl_context=context)

@app.agent(raw_cve_results)
async def process(entries):
    ''' Process each CVE and split the impacted products and references, index afterwards '''
    async for entry in entries:
        cve = CVE(cve=entry)
        cve_details = cve.details
        references = [{**doc, '_index': config['nvd']['index'], '_id': doc['fingerprint']}
            for doc in cve.references]
        impacted = [{**doc, '_index': config['nvd']['index'], '_id': doc['fingerprint']}
            for doc in cve.impacted]
        documents = references + impacted
        documents.append({**cve_details, '_index': config['nvd']['index'], '_id': cve_details['fingerprint']})
        helpers.bulk(es_handler, documents, chunk_size=1000)

@app.timer(interval=30.0)
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


