import io
import tweepy
import credentials
import json
import re
import requests
import pymongo
from tweepy.streaming import StreamListener, Stream

# creating mongodb connection
connectionstring = "mongodb://dbuser:dbpass@datacluster-shard-00-00.qnva0.mongodb.net:27017,datacluster-shard-00-01.qnva0.mongodb.net:27017,datacluster-shard-00-02.qnva0.mongodb.net:27017/RawDb?ssl=true&replicaSet=atlas-kxawjc-shard-0&authSource=admin&retryWrites=true&w=majority"
client = pymongo.MongoClient(connectionstring)
print(client.list_database_names())
db = client.get_database("RawDb")
collection = db.get_collection("RawDbCollection")
db1 = client.get_database("ProcessedDb")
collection1 = db1.get_collection("ProcessedDbCollection")
global tweetCount
tweetCount = 0
class twitter_listen(StreamListener):

    # creating connection
    def createConnection(self):
        auth = tweepy.OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET)
        api = tweepy.API(auth)
        auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
        return auth
    def on_data(self, data):

        try:
            #clean tweets
            data = re.sub(r'@[A-Za-z0-9]+', '', data)
            data = re.sub(r'#(\w+)', '', data)
            data = re.sub(r'RT[\s]+', '', data)
            data = re.sub(r'http:\/\/\S*', '', data)
            data = re.sub(r'https:\/\/\S*', '', data)

            json_load = json.loads(data)
            text = json_load['text']
            text = re.sub(r':(\s+)', '', text)
            text = re.sub(r'https:\/\/\S*', '', text)
            #emoji pattern is taken from https://stackoverflow.com/questions/13729638/how-can-i-filter-emoji-characters-from-my-input-so-i-can-save-in-mysql-5-5
            RE_EMOJI = re.compile(u'([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])')
            text=RE_EMOJI.sub(r'', text)
            words = text.split()
            counts = dict()

            for word in words:
                if word in counts:
                    counts[word] += 1
                else:
                    counts[word] = 1
            global tweetCount
            tweetCount += 1
            print("TWEET=", tweetCount)
            print("MESSAGE=", counts)

            sumBefore=sum(counts.values())
            with open('negative.txt') as file:
                with open('positive.txt') as file2:

                    contents = file.read()
                    content = file2.read()
                    for w in counts:
                        search_word = w

                        if search_word in contents.split():  #search word in negative list
                            print('NEGATIVE MATCH=', search_word)
                            counts[search_word]=counts.get(search_word)-(counts.get(search_word))
                        elif search_word in content.split():  #search word in positive list
                            print('POSITIVE MATCH=', search_word)
                            counts[search_word]=counts.get(search_word)+(counts.get(search_word))
            sumAfter = sum(counts.values())
            #calculate polarity
            if sumAfter<sumBefore:
                print("POLARITY=negative")
            if sumAfter>sumBefore:
                print("POLARITY=positive")
            if sumAfter==sumBefore:
                print("POLARITY=neutral")
            print("-----------------")
            return True

        except BaseException as e:
            print(e)
        return True

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        if status_code == 420:
            return False
    def do_stream(self):

        listener = twitter_listen()
        auth = tweepy.OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET)
        api = tweepy.API(auth)
        auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)
        #filter tweets
        stream.filter(languages=["en"], track=['Storm', 'Winter', 'Canada', 'Temprature', 'Flu', 'Snow', 'Indoor', 'Safety'])


if __name__ == '__main__':
    t=twitter_listen()
    t.do_stream()
