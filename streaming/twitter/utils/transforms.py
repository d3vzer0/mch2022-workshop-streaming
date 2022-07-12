import hashlib

FINGERPRINT_PREFIX = 'TWEETCVE'

class TweetECS:
    def __init__(self, tweet=None):
        self.tweet = tweet

    @staticmethod
    def fingerprint(original_date: str, unique_value: str) -> str:
        ''' Generates a sorteable document ID for ES'''
        hex_date = str(original_date).encode('utf-8').hex()
        hash = hashlib.blake2b(f'{FINGERPRINT_PREFIX}{unique_value}'.encode('utf-8'),
            digest_size=20).hexdigest()
        return  f'{hex_date}{hash}'

    @property
    def to_dict(self) -> dict:
        ''' Denormalizes Tweet with ECS schema '''
        return {
            'tweet.id': self.tweet.id,
            'tweet.username': self.tweet.user.name,
            'tweet.full_name': self.tweet.user.screen_name,
            'tweet.retweet_count': self.tweet.retweet_count,
            'tweet.content': self.tweet.full_text,
            'publish_date': self.tweet.created_at,
            'tweet.favourite_count': self.tweet.favorite_count,
            'tweet.tags': [hashtag['text'].lower() for hashtag in self.tweet.entities['hashtags']],
            'tweet.statuses_count': self.tweet.user.statuses_count,
            'tweet.follower_count': self.tweet.user.followers_count,
            'tweet.product': self.tweet.source,
            'event.provider': 'twitter',
            'tweet.dataset': 'CVE',
            'fingerprint': TweetECS.fingerprint(self.tweet.created_at, self.tweet.id)
        }