import requests
import json
import scrapy
import pymongo
import time
from datetime import datetime

from time import gmtime, strftime
import os

class WallaPopStreamer():

    def __init__(self, dbhost, db, request_interval, category, category_name):
        self.collection = "wallapop_all"

        self.category = category
        self.category_name = category_name
        self.base_url = "https://api.wallapop.com/api/v3/cars/wall?category_ids="+str(category)+"&latitude=40.418965&longitude=-3.711781&num_results=50"
        self.item_url = "https://es.wallapop.com/item/"
        self.request_interval = request_interval
        self.init_db(dbhost,db)
        self.r = requests.Session()


        self.headers = {
                "User-Agent":"Mozilla/3.0",
                "Host":"es.wallapop.com",
                "Accept-Encoding":"Accept-Encoding",
                "X-Signature":"84RwnTvntM7l4QkhsDSnD1kLmUY9grgbPm8hMlrZY90=",
                "Referer":"https://es.wallapop.com/search?category_ids="+str(self.category)+"&latitude=40.418965&longitude=-3.711781",
                "Timestamp":"1598951807879",
                "Cookie":""
        }

    def __del__(self):
        self.dbclient.close()

    def init_db(self, dbhost, db):
        self.dbclient = pymongo.MongoClient(dbhost)
        self.db = self.dbclient[db]
        self.col = self.db["wallapop_all"]
    def parse_items(self,items):
        for item in items:
            try:
                item_object = {
                    "title":item["content"]["title"],
                    "description":item["content"]["storytelling"],
                    "price":item["content"]["price"],
                    "images":item["content"]["images"],
                    "user":item["content"]["user"],
                    "user_id":item["content"]["user"]["id"],
                    "category":item["content"]["category_id"],
                    "object_data":item
                }
            except:
                item_object = {
                    "title":"unknown",
                    "description":"unknown",
                    "price":"unknown",
                    "images":"unknown",
                    "user":"unknown",
                    "user_id":"unknown",
                    "category":"unknown",
                    "object_data":"unknown",
                }

            url = item["content"]["web_slug"]
            self.inspect_item(url, item_object)

    def inspect_item(self, url, item):
        url = self.item_url + url
        r = self.r.get(url = url, headers = self.headers)
        text = r.text
        try:
            item["postalcode"] = self.get_postalcode(text)
        except:
            item["postalcode"] = "unknown"
        try:
            item["rating"] = self.get_item_rating(text)
        except:
            item["rating"] = "unknown"
        try:
            item["ratingnum"] = self.get_item_rating_num(text)
        except:
            item["ratingnum"] = "unknown"
        try:
            item["username"] = self.get_username(text)
        except:
            item["username"] = "unknown"
        try:
            item["parsing_date"] =  strftime("%Y-%m-%d %H:%M:%S", gmtime())
        except:
            item["parsing_date"] = "unknown"
        try:
            item["city"] = self.get_city(text)
        except:
            item["city"] = "unknown"
        try:
            item["publish_date"] = self.get_date(text)
        except:
            item["publish_date"] = "unknown"
        item["category"] = self.category
        
        self.col.insert_one(item)

    def get_username(self, rc):
        
        get_username = '//h2[@class="card-user-detail-name"]/span/text()'
        res = scrapy.Selector(text=rc).xpath(get_username).extract()[0].strip()
        return res

    def get_date(self, rc):
        get_date = '//div[@class="card-product-detail-user-stats-published"]/text()'
        res = scrapy.Selector(text=rc).xpath(get_date).extract()[0].strip()
        return res

    def get_city(self, rc):
        get_city = '//div[@class="card-product-detail-location"]/a/text()'
        res = scrapy.Selector(text=rc).xpath(get_city).extract()[0]
        return res
    def get_postalcode(self,rc):
        
        get_postalcode = '//div[@class="card-product-detail-location"]/text()'
        res = scrapy.Selector(text=rc).xpath(get_postalcode).extract()[0].strip()[:-1]
        return res
    
    def get_item_rating(self,rc):

        get_itemrate = '//div[@class="card-profile-rating"]/@data-score'

        res = scrapy.Selector(text=rc).xpath(get_itemrate).extract()[0].strip()
        return res
        
    def get_item_rating_num(self,rc):

        get_itemrate = '//span[@class="recived-reviews-count"]/text()'
        res = scrapy.Selector(text=rc).xpath(get_itemrate).extract()[0]
        return res

    def parse_page(self, page=0):

        url = self.base_url + "&density_type=30&start="+str(page)

        r = self.r.get(url = url, headers = self.headers)

        r = r.json()
        items = r["search_objects"]

        if items:
            try:        
                self.parse_items(items)
            except:
                print("error parsing items")
            page += 50
            self.parse_page(page)

        time.wait(2)


    def start(self):
        self.parse_page()



#lat = "40,25"
#lon = "25,022"
#server = "localhost:9092"
#queue = "wallapop"
dbhost = "mongodb://localhost:27017/"
db = "hearthbeat"

# SECOND HAND CARS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=100, category_name="cars")
wallapopstreamer.start()

# MOTORBIKES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=14000, category_name="motorbikes")
wallapopstreamer.start()

# MOTOR RELATED ITEMS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12800, category_name="motor_items")
wallapopstreamer.start()

# STYLE AND CLOTHING
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12465, category_name="clothing")
wallapopstreamer.start()

# REAL ESTATE
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=200, category_name="real_estate")
wallapopstreamer.start()

# TVS AUDIO AND PHOTO DEVICES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12545, category_name="tv_audio_photo")
wallapopstreamer.start()

# PHONES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=16000, category_name="phones")
wallapopstreamer.start()

# COMPUTERS AND ELECTRONICS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=15000, category_name="computers_electronics")
wallapopstreamer.start()

# SPORT AND HOBBIES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12579, category_name="sport")
wallapopstreamer.start()

# BIKES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=17000, category_name="bikes")
wallapopstreamer.start()

# CONSOLES AND GAMES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12900, category_name="consoles_games")
wallapopstreamer.start()

# HOME AND GARDEM ITEMS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12467, category_name="home_garden")
wallapopstreamer.start()

# HOME (ELECTRONIC) ITEMS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=13100, category_name="home_items")
wallapopstreamer.start()

# FILM, BOOKS, MUSIC
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12463, category_name="cinema_books_music")
wallapopstreamer.start()

# BABIES AND CHILDREN
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12461, category_name="children")
wallapopstreamer.start()

# COLLECTIONIST ITEMS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=18000, category_name="collectionist_items")
wallapopstreamer.start()

# CONSTRUCTION
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=19000, category_name="construction")
wallapopstreamer.start()

# INDUSTRY AND AGRICULTURE
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=20000, category_name="industry_agriculture")
wallapopstreamer.start()

# JOB MARKET RELATED ITEMS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=21000, category_name="job_market")
wallapopstreamer.start()

# SERVICES
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=13200, category_name="services")
wallapopstreamer.start()

# OTHERS
wallapopstreamer = WallaPopStreamer(dbhost=dbhost, db=db, request_interval=0, category=12485, category_name="others")
wallapopstreamer.start()