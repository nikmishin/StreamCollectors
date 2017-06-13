from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import pymongo
from pymongo import MongoClient
import json

access_token = "225906230-N7ycw6rtFqkyPa4bamn1WUJThubVUM56NLz3fFbN"
access_token_secret = "SkWSOPVoYqJXPOBYGjGxXbUlf8aNGINHwIE3RQzwLbGjl"
consumer_key = "H30OW7zK2guGBr97tF4RpTDSk"
consumer_secret = "ZjHN9vSgcKsMPsaHcWCgnmCeigbHa03trWwbPWG62cc0tYQLp6"


class StdOutListener(StreamListener):
    def on_status(self, status):
        client = MongoClient('localhost', 27017)
        db = client['tweetsNik']
        collection = db['tweets6']
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

        if status.coordinates and status.lang == "en":
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
    stream.filter(
        track={"Diarrhoea", "Diarrhea", "Diarrea", "the squits", "the shits", "the runs", "Delhi belly", "Cairo 2 step",
               "Cairo two step", "the trots", "Gippy stomach", "dodgy stomach", "dodgy tummy", "dodgy belly",
               "Bad belly", "bad stomach", "Belly ache", "stomach ache", "Stomach flu", "Vomit", "vomited", "vomiting",
               "Be sick", "been sick", "being sick", "got sick", "am sick", "Threw up", "throw up", "throwing up",
               "Spew", "spewed", "Chuck up", "chucked up", "Puke", "puked", "Heave", "heaved", "retch", "barf",
               "barfed", "hurl", "hurled", "upchuck", "upchucked", "ralph", "ralf", "ralphed", "ralfed", "technicolour",
               "Technicolor yawn", "the skitters", "Montezumas revenge", "Montezuma's revenge", "Aztec revenge",
               "Aztec two step", "turista", "dysentery", "lientery", "flux", "wasting", "loose bowels"})
