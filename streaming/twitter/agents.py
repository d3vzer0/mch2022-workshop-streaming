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
es_handler = Elasticsearch([config['elasticsearch']['uri']])

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
    ''' Get extracted properties from NLP API '''
    uri = f'{base_uri}/extract'
    async with aiohttp.ClientSession() as session:
        async with session.post(uri, json={'text': text}) as resp:
            return await resp.json()


@app.agent(tweets_enriched_topic)
async def process_enriched_tweets(tweets):
    ''' Bulk upload matching tweets '''
    async for tweet_bulk in tweets.take(100, within=3):
        elastic_docs = [
             {**tweet, '_index': config['twitter']['index'], '_id': tweet['fingerprint']}
             for tweet in tweet_bulk
        ]
        helpers.bulk(es_handler, elastic_docs, chunk_size=100)


# @app.agent(tweets_enriched_topic)
# async def process_enriched_tweets_print(tweets):
#     ''' Simple print of tweets '''
#     async for tweet in tweets:
#         print(tweet)


@app.agent(tweets_topic)
async def process(tweets):
    async for tweet in tweets:
        ''' Enrich tweet with basic NLP '''
        extract_nlp = await get_entities(config['nlp']['uri'], tweet.text)
        tweet_as_dict = tweet.asdict()
        enriched_tweet = {**tweet_as_dict, **extract_nlp}
        enriched_tweet['author_metrics'] = enriched_tweet['author_metrics'].asdict()
        if enriched_tweet['public_metrics']:
            enriched_tweet['public_metrics'] = enriched_tweet['public_metrics'].asdict() 
        parsed_tweet = TweetECS(tweet=enriched_tweet).to_dict
        await tweets_enriched_topic.send(value=parsed_tweet)


@app.command(option('--filters', type=str, help='Comma seperated list of tweet keywords'))
async def get_tweets(self, filters: str):
    ''' Live stream tweets based on rules '''
    tweets = Tweets(config['twitter']['bearer'])
    for filter in filters.split(','):
        await tweets.add_rules(tweepy.StreamRule(filter))

    await tweets.filter(
        tweet_fields=['public_metrics', 'created_at', 'author_id', 'referenced_tweets'],
        user_fields=['username', 'created_at', 'public_metrics'],
        expansions=['author_id','referenced_tweets.id','referenced_tweets.id.author_id'],
    )
