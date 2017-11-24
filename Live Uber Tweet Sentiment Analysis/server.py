# required packages
import zmq
import time
from twython import TwythonStreamer
import datetime
import json
import pprint
import pymongo
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

# twitter keys
CONSUMER_KEY = #
CONSUMER_SECRET = #
ACCESS_TOKEN = #
ACCESS_TOKEN_SECRET = #

# set up server parameters
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind('tcp://10.50.0.119:4444')

# connect to mongo deamon
connection = pymongo.MongoClient("mongodb://localhost")

# connect to the collection called uber_tweets in the kubrick db
db = connection.kubrick.uber_tweets

# twitter streaming class
class MyStreamer(TwythonStreamer):
    counter = 0

    def on_success(self, data):
        try:
            if data['lang'] in ['en']:
                MyStreamer.counter += 1
                # get sentiment of tweet
                sent = SentimentIntensityAnalyzer().polarity_scores(data['text'])['compound']
                for name in filter_name:
                    # find which filter name is contained within tweet
                    if name in data['text'].lower():
                        # send filter name and sentiment to server
                        print('Tweet: {c} | Sent: {s} | {n} | {lang}'.format(c=MyStreamer.counter, s=sent, n=name, lang=data['lang']))
                        msg = 'Tweet: {c} | Sent: {s} | {n}' \
                            .format(c=MyStreamer.counter, s=sent, n=name)
                        socket.send_string(msg)
                        # store the tweet and sentiment value in the mongodb
                        db.insert_many([{"tweet": data['text'], "sentiment": sent}])

        except:
            pass

        #msg = str(SentimentIntensityAnalyzer().polarity_scores(str(data['text']))['compound'])
        #socket.send_string(msg)
        #print(msg)

    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()

stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# import words to filter on
t = pd.read_csv(r'filter_names.csv')
filter_name = t['names'].tolist()
t = ','.join(filter_name)
print(t)
timeout = time.time() + 60

# continuously filter tweets based on names defined
while True:
    if time.time() > timeout:
        break
    stream.statuses.filter(track=t)

# types_of_jokes = ['dadjoke', 'joke']

# while True:
#     stream.statuses.filter(track=types_of_jokes)
#     #time.sleep(0.5)