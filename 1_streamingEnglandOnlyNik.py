from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import pymongo
from pymongo import MongoClient
import json

authData = [
["1_EnglandOnly", "225906230-4KcghcYXxfamdEz49z1IqipqPA020VOv3PiCJ99X", "58dZf6P5YMj6NIotBreXQPy0nFTW2AP8R0HTwnzvdBw2I",
 "jqBZJ7qvk5a46IQwUaUhf6fTO", "6E9FRZrQGLNsxL5PiEtGu1Ip3G5Q2osqZSqor6CBx70OlhBYEA"],
["2_EnglishWorld", "225906230-2tZHBO3GQ6nAT8fiHH4CYyp55hMN2wCr0c2fkYq3",
 "nApnypwiXxnmdQXQCn2dyy0EI2gKq3Xb8hSBrtXZW28Od", "liXQvePUY5tRUm8sytTdSWQVa",
 "K90IZ0nhjS30awJD1Au9K2l42AgzfVsg9obh65qSaiZXhHXH0X"],
["3_CountriesOfInterest", "225906230-kjf2cQSbYLtleB4kGPqNSzrFybmAdp2nBm7m10EX",
 "MTox3lpAGKjcEK3wHNZ6bdYg52R3cGJh8DatOwM2dF4wU", "Gmx2xbLJmL51KRPpndy8YmiY3",
 "QZird3dHjzJa1t0RRyVLLk2qdl31lp88PO9BTYKbzlX7s8YG3D"],
["4_USAOnly", "225906230-zMK4JOgNcWScTEkrnwWpG1AJkiu7RXBIUfRLnWCx", "vXBebtVuklSiqN6NYS9ssTt2ry4xqWcUPcavDFuQ4Rlp8",
 "TH7cGplHwPsVDvi7BCvNPYMr3", "4d9DQP3WLSj6AJR0VPHVSWwpvlCXn4BqfT8B9XS7EUlyOndVBL"],
["5_6_KeywordsEnglish", "225906230-N7ycw6rtFqkyPa4bamn1WUJThubVUM56NLz3fFbN",
 "SkWSOPVoYqJXPOBYGjGxXbUlf8aNGINHwIE3RQzwLbGjl", "H30OW7zK2guGBr97tF4RpTDSk",
 "ZjHN9vSgcKsMPsaHcWCgnmCeigbHa03trWwbPWG62cc0tYQLp6"]]

appId = 0  # select which app to use for collection

access_token = authData[appId][1]
access_token_secret = authData[appId][2]
consumer_key = authData[appId][3]
consumer_secret = authData[appId][4]

print("Collecting tweets through app: " + authData[appId][0])
class StdOutListener(StreamListener):

    def on_status(self, status):
        client = MongoClient('localhost', 27017)
        db = client['tweetsNik']
        collection = db['tweets1']
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
        #tweet['entities_hashtags'] = status.entities.hashtags
        #tweet['entities_urls'] = status.entities.urls
        #tweet['entities_user_mentions'] = status.entities.user_mentions

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

        if status.coordinates and status.lang=="en":
            #print(tweet)
            collection.insert_one(tweet)

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    #stream.filter(locations=[-0.58, 51.24, 0.32, 51.8, 0, 52.14, 0.24, 52.25]) #London and Cambridge
    #stream.filter(locations=[0, 52.14, 0.24, 52.25]) #Cambridge
    #stream.filter(locations=[1.178, 52.583, 1.382, 52.683]) #Norwich
    stream.filter(locations=[-6.6138,49.8911,0.7251,51.3615,
                             0.7251, 50.851, 1.5491, 51.3615,
                             -3.0103, 51.3615, 1.5491, 51.5258,
                             -2.6727, 51.5257, 2.0275, 55.1858,
                             -3.1935, 51.8386, -2.6562, 54.9686,
                             -3.6219, 54.04, -3.1893, 54.937,
                             -4.84, 54.0105, -4.2643, 54.4609,
                             -2.6497, 54.3057, -0.2158, 55.8074
                             ])  # England
