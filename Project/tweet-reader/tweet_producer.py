import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time

from kafka import KafkaProducer

from get_authentication import get_authentication

auth_dict = get_authentication()
auth = OAuthHandler(auth_dict['CONSUMER_KEY'], auth_dict['CONSUMER_SECRET'])
auth.set_access_token(auth_dict['ACCESS_TOKEN'], auth_dict['ACCESS_SECRET'])

topic_name = 'TWEETS'

producer = KafkaProducer(
        bootstrap_servers=['broker:9092'])
print("Producer created")

class Listener(StreamListener):
    def on_data(self, raw_data):
        producer.send(topic_name, str.encode(raw_data))
        return True

def get_tweets():
    while True:
        listener = Listener()
        stream = Stream(auth, listener)
        stream.filter(track=["#"], stall_warnings=True, languages=['en'])

if __name__ == "__main__":
    get_tweets()