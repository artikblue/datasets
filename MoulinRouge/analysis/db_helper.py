from pymongo import MongoClient
import pymongo
import pandas as pd

class MongoHelper:

    def __init__(self):

        self._connect_mongo(host="localhost", port=27017, username=None, password=None, db="heartbeat")

    # AUX mongo functions    
    def _connect_mongo(self, host, port, username, password, db):
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, port)


        self.db = conn[db]

    def read_dashboard_data(self, collection="meetup_result"):
        cursor = self.db[collection].find_one(
            sort=[( '_id', pymongo.DESCENDING )]
        )   
        return cursor

    def save_dashboard_data(self, collection="meetup_result",dashboard_data={}):
        
        self.db[collection].insert_one(dashboard_data)

    def read_mongo(self,collection="meetup", query={}, no_id=True):
        """ Read from Mongo and Store into DataFrame """

        # Make a query to the specific DB and Collection
        cursor = self.db[collection].find(query)

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))

        # Delete the _id
        if no_id:
            del df['_id']

        return df

    