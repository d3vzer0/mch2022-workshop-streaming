import re
import hashlib

FINGERPRINT_PREFIX = 'RSSENTRY'

class Transform:
    def __init__(self, entry=None):
        self.entry = entry

    # @staticmethod
    # def fingerprint(original_date, unique_value):
    #     ''' Generates a sorteable document ID for ES'''
    #     hex_date = str(original_date).encode('utf-8').hex()
    #     hash = hashlib.blake2b(f'{FINGERPRINT_PREFIX}{unique_value}'.encode('utf-8'),
    #         digest_size=20).hexdigest()
    #     return  f'{hex_date}{hash}'

    @property
    def to_dict(self):
        ''' Denormalizes RSS Entry with ECS schema '''
    
        return {
            'event.provider': 'miniflux',
            'rss.title': self.entry['title'],
            'rss.id': self.entry['id'],
            'rss.source': self.entry['feed']['title'],
            'rss.site': self.entry['feed']['site_url'],
            'rss.url': self.entry['url'],
            'rss.author': self.entry['author'],
            'rss.hash': self.entry['hash'],
            'rss.content': self.entry['content'],
            'publish_date': self.entry['published_at'],
            # 'fingerprint': Transform.fingerprint(self.entry['published_at'], self.entry['hash'])
        }