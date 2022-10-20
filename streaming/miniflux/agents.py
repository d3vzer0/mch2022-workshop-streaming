from ..main import app
from ..config import config
from .utils.records import MinifluxRecord
from .utils.transforms import Transform
from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
from tweepy.asynchronous.streaming import AsyncStreamingClient
import aiohttp
import miniflux

miniflux_topics = app.topic('streaming-miniflux', value_type=MinifluxRecord)
miniflux_topics_enriched = app.topic('streaming-miniflux-enriched')

es_handler = Elasticsearch([config['elasticsearch']['uri']])

class Entries:
    def __init__(self, entries):
        self.entries = entries

    @property
    def ids(self):
        return [entry['id'] for entry in self.entries]

    @property
    def to_dict(self):
        return [{
            'title': entry['title'],
            'id': entry['id'],
            'url': entry['url'],
            'source': entry['feed']['title'],
            'site': entry['feed']['site_url'],
            'author': entry['author'],
            'hash': entry['hash'],
            'content': entry['content'],
            'published_at': entry['published_at']
        } for entry in self.entries]


class MinifluxRss:
    def __init__(self, api_key, host):
        self.api_key = api_key
        self.host = host

    @property
    def client(self):
        handler = miniflux.Client(self.host,
            api_key=self.api_key)
        return handler

    def update(self, entries: list, status='read') -> None:
        self.client.update_entries(entries, status)

    def entries(self, limit: int = None, status: str = 'unread') -> Entries:
        return Entries(
            self.client.get_entries(status=status, limit=limit)['entries']
        )

async def clean_content(base_uri, text):
    async with aiohttp.ClientSession() as session:
        async with session.post(f'{base_uri}/clean', json={'text': text}) as resp:
            clean_content = await resp.json()
            return clean_content


async def get_entities(base_uri, text):
    async with aiohttp.ClientSession() as session:
        async with session.post( f'{base_uri}/extract', json={'text': text}) as resp:
            return await resp.json()


# This can be done much more efficiently by 
# buffering documends and bulk uploading
@app.agent(miniflux_topics_enriched)
async def process_enriched_articles(entries):
    async for entry in entries:
        parsed_article = Transform(entry=entry).to_dict
        elastic_doc = {**parsed_article, '_index': config['miniflux']['index'], '_id': parsed_article['fingerprint']}
        helpers.bulk(es_handler, [elastic_doc], chunk_size=1000)

# @app.agent(miniflux_topics_enriched)
# async def process_enriched_articles_print(entries):
#     async for entry in entries:
#         parsed = Transform(entry=entry).to_dict
#         print(parsed)


@app.agent(miniflux_topics)
async def process(entries):
    async for entry in entries:
        strip_content = await clean_content(config['nlp']['uri'], entry.content)
        extract_nlp = await get_entities(config['nlp']['uri'], strip_content['text'])
        entry_as_dict = entry.asdict()
        enriched_article = {**entry_as_dict, **extract_nlp, 'clean_content': strip_content}
        await miniflux_topics_enriched.send(value=enriched_article)


@app.timer(interval=10.0)
async def get_entries():
    rss = MinifluxRss(config['miniflux']['key'],
        config['miniflux']['host'])

    entries = rss.entries(limit=10)
    for entry in entries.to_dict:
        await miniflux_topics.send(value=entry)
        print(entry)
    rss.update(entries.ids, status='read')

