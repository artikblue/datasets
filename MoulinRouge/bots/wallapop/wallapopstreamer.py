import requests
import json
import scrapy
import pymongo
import time
from datetime import datetime

from time import gmtime, strftime
import os

class WallaPopStreamer():

    def __init__(self, city, dbhost, db, request_interval):
        self.collection = "wallapop"
        self.city = city
        self.base_url = "https://es.wallapop.com/rest/list/"
        self.item_url = "https://es.wallapop.com/item/"
        self.request_interval = request_interval
        self.init_db(dbhost,db)
        self.r = requests.Session()


        self.headers = {
                "User-Agent":"Mozilla/3.0",
                "Host":"es.wallapop.com",
                "Accept-Encoding":"Accept-Encoding",
                "Cookie":""
        }

    def __del__(self):
        self.dbclient.close()

    def init_db(self, dbhost, db):
        self.dbclient = pymongo.MongoClient(dbhost)
        self.db = self.dbclient[db]
        self.col = self.db["wallapop"]
    def parse_items(self,items):
        for item in items:
            item_object = {
                "title":item["title"],
                "description":item["description"],
                "price":item["salePrice"],
                "publish_date":datetime.fromtimestamp(item["publishDate"] / 1000.0 ),
                "seller":item["sellerId"],
                "images":item["images"]
            }

            url = item["url"]
            self.inspect_item(url, item_object)

    def inspect_item(self, url, item):
        url = self.item_url + url
        r = self.r.get(url = url, headers = self.headers)
        text = r.text

        item["postalcode"] = self.get_postalcode(text)
        item["rating"] = self.get_item_rating(text)
        item["ratingnum"] = self.get_item_rating_num(text)
        item["username"] = self.get_username(text)
        item["parsing_date"] =  strftime("%Y-%m-%d %H:%M:%S", gmtime())
        self.col.insert_one(item)

    def get_username(self, rc):
        
        get_username = '//h2[@class="card-user-detail-name"]/span/text()'
        res = scrapy.Selector(text=rc).xpath(get_username).extract()[0].strip()
        print(res)
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
        
        url = self.base_url + self.city
        url += "?_p="+str(page)

        r = self.r.get(url = url, headers = self.headers)
        r = r.json()
        items = r["items"]
        self.parse_items(items)
        has_more = r["moreResults"]
        
        if has_more:
            print("More results")
            next_start = int(r["nextStart"])
            print(next_start)
            time.sleep(self.request_interval)
            self.parse_page(next_start)
        


    def start(self):
        self.parse_page()



#lat = "40,25"
#lon = "25,022"
#server = "localhost:9092"
#queue = "wallapop"
city = os.environ["CITY"]
#dbhost = "mongodb://localhost:27017/"
dbhost = os.environ["MONGO_URL"]
db = os.environ["DB"]
#db = "hearthbeat"
request_interval = int(os.environ["REQUEST_INTERVAL"])


wallapopstreamer = WallaPopStreamer(city, dbhost, db, request_interval)
wallapopstreamer.start()
