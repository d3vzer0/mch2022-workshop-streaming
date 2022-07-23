import os

config = {
    'nlp': {
        'uri': os.getenv('NLP_URI', 'http://')
    },
    'elasticsearch': {
        'uri': os.getenv('ELASTIC_URI', 'https://localhost:9200'),
        'ca': os.getenv('ELASTIC_CA', './http_ca.crt')
    },
    'nvd': {
        'days': 3,
        'max_results': 2000,
        'index': 'mch2022-nvd'
    },
    'miniflux': {
        'host': os.getenv('MINIFLUX_HOST', 'http://localhost:8080'),
        'key': os.getenv('MINIFLUX_KEY', None),
        'index': 'mch2022-miniflux'
    },
    'twitter': {
        'keywords': os.getenv('TWITTER_KEYWORDS', 'CVE,vulnerability').split(','),
        'bearer': os.getenv('TWITTER_BEARER', None),
        'index': 'mch2022-twitter'
    },
    'faust': {
        'app': os.getenv('STREAM_TYPE', 'streaming.nvd'),
        'name': os.getenv('STREAM_NAME', 'streaming.nvd'),
        'broker':os.getenv('STREAM_BROKER', 'kafka://127.0.0.1:9092'),
        'partitions': os.getenv('STREAM_PARTITIONS', 4),
        'store': os.getenv('STREAM_STORE', 'memory://')
    }
}