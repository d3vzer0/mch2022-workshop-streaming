from datetime import datetime
import faust

class MinifluxRecord(faust.Record, serializer='json'):
    title: str
    id: int
    source: str
    site: str
    author: str
    hash: str
    published_at: str
    url: str 
    content: str
