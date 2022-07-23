from ..main import app
from ..config import config
from .utils.transforms import TweetECS
from .utils.records import TweetRecord
from faust.cli import option
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
from tweepy.asynchronous.streaming import AsyncStreamingClient
import tweepy
import faust
import aiohttp
import json

tweets_topic = app.topic('streaming-tweets', value_type=TweetRecord)
tweets_enriched_topic = app.topic('streaming-tweets-enriched')

context = create_default_context(cafile=config['elasticsearch']['ca'])
es_handler = Elasticsearch([config['elasticsearch']['uri']], ssl_context=context)

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

async def get_entities(base_uri, text):
    uri = f'{base_uri}/extract'
    async with aiohttp.ClientSession() as session:
        async with session.post(uri, json={'text': text}) as resp:
            return await resp.json()


# @app.agent(tweets_enriched_topic)
# async def process_enriched_tweets(tweets):
#     async for tweet in tweets:
#         parsed_tweet = TweetECS(tweet=tweet).to_dict
#         elastic_doc = {**parsed_tweet, '_index': config['twitter']['index'], '_id': parsed_tweet['fingerprint']}
#         helpers.bulk(es_handler, [elastic_doc], chunk_size=1000)


@app.agent(tweets_enriched_topic)
async def process_enriched_tweets_print(tweets):
    async for tweet in tweets:
        parsed = TweetECS(tweet=tweet)
        print(parsed.to_dict)


@app.agent(tweets_topic)
async def process(tweets):
    async for tweet in tweets:
        extract_nlp = await get_entities(config['nlp']['uri'], tweet.text)
        tweet_as_dict = tweet.asdict()
        enriched_tweet = {**tweet_as_dict, **extract_nlp}
        await tweets_enriched_topic.send(value=enriched_tweet)


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
