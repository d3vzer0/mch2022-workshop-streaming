from ..main import app
from ..config import config
from .utils.transforms import TweetECS
from .utils.records import TweetRecord
from faust.cli import option
from datetime import datetime
import tweepy
import faust
from tweepy.asynchronous.streaming import AsyncStreamingClient

tweets_topic = app.topic('streaming-tweets', value_type=TweetRecord)

class Tweet:
    def __init__(self, tweet, includes=None):
        self.tweet = tweet
        self.includes = includes

    @property
    def user_lookup(self) -> dict:
        return { user.id: user for user in self.includes['users'] }

    @property
    def to_dict(self) -> dict:
        user_lookup = self.user_lookup[self.tweet.author_id]
        return {
            'created_at': self.tweet.created_at,
            'text': self.tweet.text,
            'id': self.tweet.id,
            'author_id': self.tweet.author_id,
            'author_username': user_lookup.username,
            'author_metrics': {
                'followers_count': user_lookup.public_metrics['followers_count'],
                'following_count': user_lookup.public_metrics['following_count'],
                'tweet_count': user_lookup.public_metrics['tweet_count'],
                'created_at': user_lookup.created_at
            }
        }


class Tweets(AsyncStreamingClient):

    async def on_response(self, response):
        is_retweet = True if 'referenced_tweets' in response.data \
            and response.data.referenced_tweets[0].type == 'retweeted' else False
        original_tweet = response.data if is_retweet == False else response.includes['tweets'][0]
        tweet = Tweet(original_tweet, includes=response.includes).to_dict
        await tweets_topic.send(value=tweet)


@app.agent(tweets_topic)
async def process(tweets):
    async for tweet in tweets:
        print(tweet)


@app.command(option('--filters', type=str, help='Comma seperated list of tweet keywords'))
async def get_tweets(self, filters: str):
    tweets = Tweets(config['twitter']['bearer'])
    for filter in filters.split(','):
        await tweets.add_rules(tweepy.StreamRule(filter))

    await tweets.filter(
        tweet_fields=['public_metrics', 'created_at', 'author_id', 'referenced_tweets'],
        user_fields=['username', 'created_at', 'public_metrics'],
        expansions=['author_id','referenced_tweets.id','referenced_tweets.id.author_id'],
    )
