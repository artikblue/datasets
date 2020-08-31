import scrapy
import time
import requests
import re
import json
import os
from scrapy.http import FormRequest
from datetime import datetime
from time import gmtime, strftime
#from geopy.geocoders import Nominatim
from dateutil import parser
import timestring

class EventbriteSpider(scrapy.Spider):
    name = "eventbritebot"

    request_interval = 0
    city = ""
    def start_requests(self):
        urls = []
        urls.append(os.environ["URL"])
        self.request_interval = int(os.environ["REQUEST_INTERVAL"])
        self.city = os.environ["CITY"]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        events = '//a[@class="eds-event-card-content__action-link"]/@href'

        events_content = response.xpath(events).extract()
        for e in events_content:
            yield scrapy.Request(url=e, callback=self.parse_event)
            time.sleep(self.request_interval)

        next_page = '//a[@aria-label="Go to next page"]/@href'
        next_page_content = response.xpath(next_page).extract()
        next_url = "https://eventbrite.com/" +next_page_content[0]
        yield scrapy.Request(url=next_url, callback=self.parse)
    
    def get_start_date(self, response):
        body = response.body.decode("utf8")
        start = "'event_date': \""
        end = '",'
        result = re.search(''+start+'(.*)'+end+'', body)
        return result.group(1)
    def parse_event(self, response):
        r= response.text
        # bottom map already offers the coords inside the url
        match = re.findall('&markers=(.*?)"', r)
        event_coords = match[0]
        coords = event_coords.split(',')
        # alternative using geolocator
        #geolocator = Nominatim(user_agent="eventbrite")
        #coords = geolocator.geocode(address)
        #coords = [coords.latitude, coords.longitude]

        price = '//div[@class="js-display-price"]/text()'
        title = '//h1[@class="listing-hero-title"]/text()'
        location = '//div[@class="event-details__data"]/p/text()'
        group_name = '//a[@class="js-d-scroll-to listing-organizer-name text-default"]/text()'
        desc = '//div[@class="structured-content-rich-text structured-content__module l-align-left l-mar-vert-6 l-sm-mar-vert-4 text-body-medium"]/p/text()'
        
        # general data
        price_content = response.xpath(price).extract()
        title_content = response.xpath(title).extract()[0]
        location_content = response.xpath(location).extract()
        desc_content = response.xpath(desc).extract()
        desc_content = ' '.join(desc_content)
        group_name_content =  response.xpath(group_name).extract()[0].strip().lower()[3:]
        event_url = response.url

        # price parsing can be problematic
        try:
            if price_content:
                price = price_content[0].strip().lower()
                
                if "free" in price or "grat" in price:
                    price = 0
                else:
                    price = price[0:4]
                    if "," in price:
                        price = price.split(',')
                        price = price[0]
                    if "'" in price:
                        price = price.split("'")
                        price = price[0]
                        price = int(''.join(filter(str.isdigit, price)))
                    # eliminate $ € signs and spaces
                    extract_price =''.join(ch for ch in price if ch.isdigit())
                    price = int(extract_price)
            else:
                price = 0
        except:
            price = 0
        
        address = location_content[1] + ","+ location_content[2]
        start_date = self.get_start_date(response)
        parsing_date = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        # event object
        event_object = {
            "title": title_content,
            "organizer": group_name_content,
            "price": price,
            "address" : address,
            "start_date" : start_date,
            "coords" : coords,
            "parsing_date" : parsing_date,
            "content" : desc_content,
            "event_url" : event_url
        }
        # to pipeline
        yield event_object
        
