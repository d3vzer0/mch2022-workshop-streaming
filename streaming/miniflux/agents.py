from ..main import app
from ..config import config
from .utils.records import MinifluxRecord
import miniflux
from tweepy.asynchronous.streaming import AsyncStreamingClient

miniflux_topics = app.topic('streaming-miniflux', value_type=MinifluxRecord)

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


@app.agent(miniflux_topics)
async def process(entries):
    async for entry in entries:
        print(entry)


@app.timer(interval=6.0)
async def get_entries():
    rss = MinifluxRss(config['miniflux']['key'],
        config['miniflux']['host'])

    entries = rss.entries()
    for entry in entries.to_dict:
        await miniflux_topics.send(value=entry)
    rss.update(entries.ids, status='read')


