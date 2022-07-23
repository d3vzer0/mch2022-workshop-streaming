import re
import hashlib

FINGERPRINT_PREFIX = 'RSSENTRY'

class Transform:
    def __init__(self, entry=None):
        self.entry = entry

    @staticmethod
    def fingerprint(original_date, unique_value):
        ''' Generates a sorteable document ID for ES'''
        hex_date = str(original_date).encode('utf-8').hex()
        hash = hashlib.blake2b(f'{FINGERPRINT_PREFIX}{unique_value}'.encode('utf-8'),
            digest_size=20).hexdigest()
        return  f'{hex_date}{hash}'

    @property
    def to_dict(self):
        ''' Denormalizes RSS Entry with ECS schema '''
        return {
            'event.provider': 'miniflux',
            'rss.title': self.entry['title'],
            'rss.id': self.entry['id'],
            'rss.source': self.entry['source'],
            'rss.site': self.entry['site'],
            'rss.url': self.entry['url'],
            'rss.author': self.entry['author'],
            'rss.hash': self.entry['hash'],
            'rss.content': self.entry['clean_content'],
            'rss.content_raw': self.entry['content'],
            'publish_date': self.entry['published_at'],
            'nlp.products': self.entry['entities']['PRODUCT'], 
            'nlp.org': self.entry['entities']['ORG'],
            'nlp.person': self.entry['entities']['PERSON'],
            'nlp.polarity': self.entry['sentiment']['polarity'],
            'nlp.subjectivity': self.entry['sentiment']['subjectivity'],
            'nlp.assessments': self.entry['sentiment']['assessments'],
            'vulnerability.id': self.entry['entities']['CVE'],
            'nlp.props': self.entry['props'],
            'tags': self.entry['props'] + self.entry['entities']['PERSON'] + self.entry['entities']['CVE'] + self.entry['entities']['ORG'] + self.entry['entities']['PRODUCT'],
            'fingerprint': Transform.fingerprint(self.entry['published_at'], self.entry['hash'])
        }