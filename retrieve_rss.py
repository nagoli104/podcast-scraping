import pickle
import pymongo
import json
import requests
import time


# get connection string

mongo_db_login_string = pickle.load(open("mongo_db_login_string.pickle", "rb"))


while True:

    # get connection to data base and point to podcast collection
    client = pymongo.MongoClient(mongo_db_login_string)
    db = client.podcasts
    collection = db.podcast

    # get 20 entries without rss feed and make list
    query_results = collection.find({"rss_feed": None}).limit(20)
    id_list = [item["itunes_id"] for item in query_results]

    # use itunes API to retrieve the RSS feeds and insert them in dictionary
    rss_dict = {}
    for id in id_list:

        try:

            itunes_search_link = "https://itunes.apple.com/lookup?id=" + id.replace(
                "id", ""
            )

            search_request = requests.get(itunes_search_link).json()

            # insert feed into dict
            rss_dict[id] = search_request["results"][0]["feedUrl"]
        except:
            print(id)
            rss_dict[id] = "failed"

    for key in rss_dict:

        result = collection.find_one_and_update(
            {"itunes_id": key}, {"$set": {"rss_feed": rss_dict[key]}}, upsert=True
        )

    # let loop rest for a minute to not exceed number of allowed API calls per minute
    time.sleep(60)
    client.close()
