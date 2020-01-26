import pandas as pd
import re
from pymongo import MongoClient
import numpy as np
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df

def get_gender(s, gender):
    res = 0
    for i in s:
        if i["gender"] == gender:
            res += 1
    return res



mdf = read_mongo(db="hearthbeat", collection="meetup")

print(mdf.head())
print(mdf["event_attendees"])

mdf["attendees_number"] = mdf['event_attendees'].map(lambda event_attendees: len(event_attendees))
print(mdf.head())

mdf["males"] = mdf['event_attendees'].map(lambda event_attendees: get_gender(event_attendees, "male"))
mdf["females"] = mdf['event_attendees'].map(lambda event_attendees: get_gender(event_attendees, "female"))

print(mdf.head())

common = pd.Series(' '.join(mdf['event_title'].apply(lambda i: ' '.join(filter(lambda j: len(j) > 4, i.split())))).lower().split()).value_counts()[:100]

print(common)

hour_mode = mdf.event_time.mode()
addr_mode = mdf.event_address.mode()

print(hour_mode)
print(addr_mode)


n = 5
print(mdf['event_time'].value_counts()[:n].index.tolist())
print(mdf['event_address'].value_counts()[:n].index.tolist())

##mdf["event_title"]=mdf.event_title.apply(lambda i: ' '.join(filter(lambda j: len(j) > 4, i.split())))


text = " ".join(review for review in mdf.event_title)
wordcloud = WordCloud().generate(text)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()