import logging
import faust
from .config import config

# logging.config.fileConfig("streaming/logging.conf")
# logger = logging.getLogger("streamio")

app = faust.App(
    config['faust']['name'], 
    broker=config['faust']['broker'],
    autodiscover=[config['faust']['app']],
    stream_wait_empty=False,
    store=config['faust']['store'],
    partitions=config['faust']['partitions']
)

if __name__ == '__main__':
    app.main()