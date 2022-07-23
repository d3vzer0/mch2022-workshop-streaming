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
            'tweet.id': self.tweet['id'],
            'tweet.username': self.tweet['author_username'],
            'tweet.user_created_at': self.tweet['author_metrics']['created_at'],
            'tweet.user_total_tweets': self.tweet['author_metrics']['tweet_count'],
            # 'tweet.full_name': self.tweet['author_usernmae'],
            'tweet.retweet_count': self.tweet['public_metrics']['retweet_count'] if self.tweet['public_metrics'] else None,
            'tweet.content': self.tweet['text'],
            'publish_date': self.tweet['created_at'],
            'tweet.like_count': self.tweet['public_metrics']['like_count'] if self.tweet['public_metrics'] else None,
            # 'tweet.tags': [hashtag['text'].lower() for hashtag in self.tweet.entities['hashtags']],
            # 'tweet.statuses_count': self.tweet.user.statuses_count,
            'tweet.follower_count': self.tweet['author_metrics']['followers_count'],
            'tweet.following_count': self.tweet['author_metrics']['following_count'],
            # 'tweet.product': self.tweet.source,
            'event.provider': 'twitter',
            'tweet.dataset': 'CVE',
            'nlp.products': self.tweet['entities']['PRODUCT'], 
            'nlp.org': self.tweet['entities']['ORG'],
            'nlp.person': self.tweet['entities']['PERSON'],
            'nlp.polarity': self.tweet['sentiment']['polarity'],
            'nlp.subjectivity': self.tweet['sentiment']['subjectivity'],
            'nlp.assessments': self.tweet['sentiment']['assessments'],
            'vulnerability.id': self.tweet['entities']['CVE'],
            'nlp.props': self.tweet['props'],
            'tags': self.tweet['props'] + self.tweet['entities']['PERSON'] + self.tweet['entities']['CVE'] + self.tweet['entities']['ORG'] + self.tweet['entities']['PRODUCT'],
            'fingerprint': TweetECS.fingerprint(self.tweet['created_at'], self.tweet['id'])
        }