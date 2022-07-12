# import tweepy
# import os

# class Tweets:
#     def __init__(self, consumer_key=None, consumer_secret=None):
#         auth = tweepy.OAuth2BearerHandler( os.getenv('TWITTER_BEARER', None))
#         self.api = tweepy.API(auth, wait_on_rate_limit=True)

#     def query(self, query, since=None, result_type='recent'):
#         return [tweet for tweet in \
#             tweepy.Cursor(self.api.search_tweets, q=query, since_id=since,
#             count=20, tweet_mode='extended').items(20)]


# for tweet in Tweets().query('RT CVE', since='1546889307071332354'):
#     content = tweet._json
#     print()
#     print(tweet._json)
