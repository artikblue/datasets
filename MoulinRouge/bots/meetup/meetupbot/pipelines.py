# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#import pymongo
import logging
#from kafka import KafkaProducer
import json
from pymongo import MongoClient



class MeetupbotPipeline(object):
    collection_name = 'meetup'
    
    def __init__(self, mongo_curi, mongo_database):
        self.client = MongoClient(mongo_curi)
        self.mongo_db = self.client[mongo_database]
        
    @classmethod
    def from_crawler(cls, crawler):
        ## pull in information from settings.py
        return cls(
            mongo_curi=crawler.settings.get('MONGO_URI'),
            mongo_database=crawler.settings.get('MONGO_DATABASE')
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection
        self.collection = self.mongo_db['meetup']
    def close_spider(self, spider):
        ## clean up when spider is closed
        pass
        # self producer.close
    def process_item(self, item, spider):
        #self.db[self.collection_name].insert(item)
        self.collection.insert_one(item)
        logging.debug("Post sent to mongo")
        return item
