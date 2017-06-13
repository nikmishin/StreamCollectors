from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import pymongo
from pymongo import MongoClient
import json

authData = [["TwitterData1988",
             "225906230-N7ycw6rtFqkyPa4bamn1WUJThubVUM56NLz3fFbN",
             "SkWSOPVoYqJXPOBYGjGxXbUlf8aNGINHwIE3RQzwLbGjl",
             "H30OW7zK2guGBr97tF4RpTDSk",
             "ZjHN9vSgcKsMPsaHcWCgnmCeigbHa03trWwbPWG62cc0tYQLp6"],
            ["DataCollectionNikMishin",
             "225906230-zMK4JOgNcWScTEkrnwWpG1AJkiu7RXBIUfRLnWCx",
             "vXBebtVuklSiqN6NYS9ssTt2ry4xqWcUPcavDFuQ4Rlp8",
             "TH7cGplHwPsVDvi7BCvNPYMr3",
             "4d9DQP3WLSj6AJR0VPHVSWwpvlCXn4BqfT8B9XS7EUlyOndVBL"
             ]]

appId = 1  # select which app to use for collection

access_token = authData[appId][1]
access_token_secret = authData[appId][2]
consumer_key = authData[appId][3]
consumer_secret = authData[appId][4]

print("Collecting tweets through app: " + authData[appId][0])


class StdOutListener(StreamListener):
    def on_status(self, status):
        client = MongoClient('localhost', 27017)
        db = client['tweetsNik']
        collection = db['tweets2']
        tweet = {}
        tweet['id_str'] = status.id_str
        tweet['text'] = status.text
        tweet['created_at'] = status.created_at
        tweet['source'] = status.source
        tweet['coordinates'] = status.coordinates
        tweet['lang'] = status.lang
        tweet['user_id_str'] = status.user.id_str
        tweet['user_time_zone'] = status.user.time_zone
        tweet['user_location'] = status.user.location
        tweet['user_lang'] = status.user.lang
        # tweet['entities_hashtags'] = status.entities.hashtags
        # tweet['entities_urls'] = status.entities.urls
        # tweet['entities_user_mentions'] = status.entities.user_mentions

        if status.place:
            tweet['place_country'] = status.place.country
            tweet['place_full_name'] = status.place.full_name
            tweet['place_name'] = status.place.name
            tweet['place_type'] = status.place.place_type
            tweet['place_box_coordinates'] = status.place.bounding_box.coordinates

        else:
            tweet['place_country'] = None
            tweet['place_full_name'] = None
            tweet['place_name'] = None
            tweet['place_type'] = None
            tweet['place_box_coordinates'] = None

        #print(tweet)

        if status.coordinates and status.lang == "en":
            # print(tweet)
            collection.insert_one(tweet)

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(
        track={"the", "i", "to", "a", "and", "is", "in", "it", "you", "of", "for", "on", "my", "'s", "that", "at",
               "with", "me", "do", "have", "just", "this", "be", "n't", "so", "are", "'m", "not", "was", "but", "out",
               "up", "what", "now", "new", "from", "your", "like", "good", "no", "get", "all", "about", "we", "if",
               "time", "as", "day", "will", "one", "twitter"})  # World in English using top 51 words on twitter according
    # to http://techland.time.com/2009/06/08/the-500-most-frequently-used-words-on-twitter/
