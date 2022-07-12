import os

config = {
    'miniflux': {
        'host': os.getenv('MINIFLUX_HOST', 'https://reader.miniflux.app'),
        'key': os.getenv('MINIFLUX_KEY', None)
    },
    'twitter': {
        'keywords': os.getenv('TWITTER_KEYWORDS', 'CVE,vulnerability').split(','),
        'bearer': os.getenv('TWITTER_BEARER', None),
    },
    'faust': {
        'app': os.getenv('STREAM_TYPE', 'streaming.miniflux'),
        'name': os.getenv('STREAM_NAME', 'streaming.miniflux'),
        'broker':os.getenv('STREAM_BROKER', 'kafka://127.0.0.1:9092'),
        'partitions': os.getenv('STREAM_PARTITIONS', 4),
        'store': os.getenv('STREAM_STORE', 'memory://')
    }
}