import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time

from kafka import KafkaProducer

from get_authentication import get_authentication

auth_dict = get_authentication()
auth = OAuthHandler(auth_dict.get('CONSUMER_KEY'), auth_dict.get('CONSUMER_SECRET'))
auth.set_access_token(auth_dict.get('ACCESS_TOKEN'), auth_dict.get('ACCESS_SECRET'))

topic_name = 'TWEETS'
print("HELLO")
producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'])
# print("Producer created")
# class Listener(StreamListener):
#     def on_data(self, raw_data):
#         print(raw_data)
#         producer.send(topic, str.encode(raw_data))
#         return True

# def get_tweets():
#     while True:
#         print("HELLO")
#         listener = Listener()
#         stream = Stream(auth, listener)
#         stream.filter(track=["Apple"], stall_warnings=True, languages=['en'])

# if __name__ == "__main__":
#     get_tweets()

class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(auth_dict.get('CONSUMER_KEY'), auth_dict.get('CONSUMER_SECRET'))
        auth.set_access_token(auth_dict.get('ACCESS_TOKEN'), auth_dict.get('ACCESS_SECRET'))
        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter()


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            print("HI")
            print(raw_data)
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()