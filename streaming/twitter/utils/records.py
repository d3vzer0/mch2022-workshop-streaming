from datetime import datetime
import faust

class AuthorMetrics(faust.Record, serializer='json'):
    followers_count: int 
    following_count: int
    tweet_count: int
    created_at: datetime


class PublicMetrics(faust.Record, serializer='json'):
    retweet_count: int
    reply_count: int
    like_count: int
    quote_count: int


class TweetRecord(faust.Record, serializer='json'):
    text: str
    author_id: int
    created_at: datetime
    id: int
    author_username: str
    public_metrics: PublicMetrics = None
    author_metrics: AuthorMetrics = None