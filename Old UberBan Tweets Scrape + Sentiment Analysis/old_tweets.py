import got3
import pymongo
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# connect to mongo deamon
connection = pymongo.MongoClient("mongodb://localhost")

# connect to the collection called uber_tweets in the kubrick db
db = connection.kubrick.uberban_tweets

count = 0

try:
    while True:
        tweetCriteria = got3.manager.TweetCriteria().setSince("2017-09-22").setQuerySearch("uberban")
        #tweetCriteria = got3.manager.TweetCriteria().setQuerySearch("uberban")
        tweet = got3.manager.TweetManager.getTweets(tweetCriteria)[count]
        sent = SentimentIntensityAnalyzer().polarity_scores(tweet.text)['compound']
        print(tweet.text)
        print(sent)
        print(tweet.date)
        db.insert_many([{"tweet": tweet.text, "sentiment": sent}])
        count += 1
except:
    print("tweet scrape ended with {no_tweets} tweets".format(no_tweets = count))

