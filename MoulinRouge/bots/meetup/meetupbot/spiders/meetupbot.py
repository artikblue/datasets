import requests
import os
import re
import json
import time
import scrapy
from time import gmtime, strftime
from datetime import datetime
from geopy.geocoders import Nominatim
import gender_guesser.detector as gender

class MeetupSpider(scrapy.Spider):
    name = "meetupbot"
    allowed_domains = ['meetup.com']
    country = ""
    city = ""
    request_interval = 0
    countrycode = ""
    def start_requests(self):
        self.city = os.environ["CITY"]
        self.country = os.environ["COUNTRY"]
        self.countrycode = os.environ["COUNTRYCODE"]
        self.request_interval = int(os.environ["REQUEST_INTERVAL"])
        urls = []
        urls.append(os.environ["URL"])
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        items = '//h3[@class="padding-none inline-block loading"][@itemprop="name"]/text()'
        urls = '//a[@itemprop="url"]/@href'
        items_content = response.xpath(items).extract()
        urls_content = response.xpath(urls).extract()
        
        
        for url in urls_content:
            yield scrapy.Request(url=url, callback=self.inspect_item)
            
    def inspect_item(self,response):
        
        description = '//p[@class="group-description margin--bottom"]/text()'
        description_content = response.xpath(description).extract()
        description_string = ''.join(description_content)

        event_url = '//a[@class="groupHome-eventsList-upcomingEventsLink link"]/@href'
        event_url_content = response.xpath(event_url)
        if event_url_content:
            event_url_content = response.xpath(event_url).extract()[0]
            
            yield scrapy.Request(url="https://www.meetup.com"+event_url_content, callback=self.inspect_events)
            time.sleep(2)
    def inspect_events(self,response):
        event_url = '//a[@class="eventCard--link"]/@href'
        event_url_content = response.xpath(event_url).extract()
        for u in event_url_content:
            u = "https://www.meetup.com"+u
            yield scrapy.Request(url=u, callback=self.inspect_single_event)
            time.sleep(self.request_interval)

    
    def inspect_single_event(self,response):
        event_url = str(response.url)
        match = re.findall("events/(.*?)/", event_url)
        event_id = match[0]
        match = re.findall(""+self.countrycode+"/(.*?)/", event_url)
        group_name = match[0]
        try:
            event_attendees = self.get_event_attendees(event_id,group_name)
        except:
            event_attendees = []
        event_title = '//h1[@class="pageHead-headline text--pageTitle"]/text()'
        event_desc = '//div[@class="event-description runningText"]/p/text()'
        event_location = '//address/p[@class="venueDisplay-venue-address text--secondary text--small"]/text()'
        event_location_place = '//address/p[@class="wrap--singleLine--truncate"]/text()'
        event_timestmap_start = '//div[@class="eventTimeDisplay eventDateTime--hover"]/time/@datetime'
        event_hour_end = '//span[@class="eventTimeDisplay-startDate-time"]/span/text()'
        event_price = '//span[@class="fee-description"]/span/text()'


        event_title_content = response.xpath(event_title).extract()[0]
        event_desc_content = response.xpath(event_desc).extract()[0]
        try:
            event_location_content = response.xpath(event_location).extract()[0]
        except:
            event_location_content = "unknown"

        try:
            event_location_place_content = response.xpath(event_location_place).extract()[0]
            
        except:
            event_location_place_content = "unknown"

        event_timestamp_content = response.xpath(event_timestmap_start).extract()
        event_hour_end_content = response.xpath(event_hour_end).extract()[0]
        event_price_content = response.xpath(event_price).extract()

        if event_price_content:
            event_price_content = event_price_content[1]
        else:
            event_price_content = "0"

        try:
            if len(event_timestamp_content[0]) > 10:
                edt_object = datetime.fromtimestamp(int(event_timestamp_content[0][0:10]))
            else:
                edt_object = datetime.fromtimestamp(int(event_timestamp_content[0]))
            edate = str(edt_object).split(' ')[0]
            etime = str(edt_object).split(' ')[1]
        except:
            edate =""
            etime =""

        
        event_comments = self.get_event_comments(event_id, group_name)
        parsing_date = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        try:
            geolocator = Nominatim(user_agent="meetup")

            coords = geolocator.geocode(event_location_place_content)
            

            coords = [coords.latitude, coords.longitude]
        except:
            coords = []
        event_info = {"event_title":event_title_content,
                        "event_desc":event_desc_content,
                        "event_id":event_id,
                        "group_name":group_name,
                        "event_address":event_location_content,
                        "event_location":event_location_place_content,
                        "event_timestamp":event_timestamp_content[0],
                        "event_date":edate,
                        "event_time":etime,
                        "coords":coords,
                        "event_hour_end":event_hour_end_content,
                        "event_price":int(event_price_content),
                        "event_attendees":event_attendees,
                        "event_comments":event_comments,
                        "event_url":event_url,
                        "parse_date":str(datetime.now()),
                        "city":self.city}
        yield event_info
    def get_user_gender(self, name):
        d = gender.Detector(case_sensitive=False)
        name = name.split(' ')[0]
        name = ''.join([i for i in name if not i.isdigit()])
        user_gender = ""
        if "female" in d.get_gender(u''+name, self.country):
            user_gender = "female"
        elif "male" in d.get_gender(u''+name, self.country):
            user_gender = "male"
        else:
            if "female" in d.get_gender(u''+name):
                user_gender = "female"
            elif "male" in d.get_gender(u''+name):
                user_gender = "male"
            else:
                user_gender = "unknown"
        return user_gender
    def get_event_comments(self,event_id, group_name):
        ecoments = []
        
        try:
            base_url = "https://meetup.com/mu_api/urlname/events/eventId?queries=(endpoint:"+group_name+"/events/"+event_id+"/comments,meta:(method:get),params:(fields:'self,web_actions'),ref:eventComments_"+group_name+"_"+event_id+",type:comments)"
            rc = requests.get(base_url, verify=False)
            response_json = json.loads(rc.content)
        
            for r in response_json["responses"][0]["value"]:
                event_commenter = r["member"]["name"]
                event_uid = r["member"]["id"]
                event_comment = r["comment"]
                event_gender= str(self.get_user_gender(event_commenter))
                ejson = {'member_name':event_commenter,
                    'memeber_uid':event_uid,
                    "event_comment":event_comment,
                    "event_gender": event_gender}
                ecoments.append(ejson)
        except Exception as e:
            print(e)
            print("error parsing comments")

        return ecoments
    def get_event_attendees(self,event_id, group_name):

        base_url = "https://meetup.com/mu_api/urlname/events/eventId/attendees?queries=(endpoint:"+group_name+"/events/"+event_id+"/rsvps,meta:(method:get),params:(desc:!t,fields:'answers,pay_status,self,web_actions,attendance_status',only:'answers,response,attendance_status,guests,member,pay_status,updated',order:time),ref:eventAttendees_"+group_name+""+event_id+",type:attendees)"
        rc = requests.get(base_url, verify=False)
        response_json = json.loads(rc.content)
        user_list = []

        attendees = response_json["responses"][0]["value"]
        for a in attendees:
            user_gender = self.get_user_gender(a["member"]["name"])
            user_data = a["member"]
            user_data["gender"] = user_gender
            user_list.append(user_data)
        return user_list
    
    def get_group_members(self,meetup_url):
        group_name = ""
        iteration = 0

        match = re.findall(""+self.countrycode+ "/(.*?)/", meetup_url)

        group_name = match[0]
        base_url = "https://www.meetup.com/mu_api/urlname/members?queries=(endpoint:groups/"+group_name+"/members,list:(dynamicRef:list_groupMembers_diviertete_all,merge:(isReverse:!f)),meta:(method:get),params:(filter:all,page:"+str(iteration)+"),ref:groupMembers_diviertete_all)"

        rc = requests.get(base_url, verify=False)
        response_json = json.loads(rc.content)
        user_list = []
        while not( not response_json["responses"][0]["value"] or not response_json["responses"][0]["value"]["value"]):

            iteration +=1
            base_url = "https://www.meetup.com/mu_api/urlname/members?queries=(endpoint:groups/"+group_name+"/members,list:(dynamicRef:list_groupMembers_diviertete_all,merge:(isReverse:!f)),meta:(method:get),params:(filter:all,page:"+str(iteration)+"),ref:groupMembers_diviertete_all)"
            
            rc = requests.get(base_url, verify=False)
            try:
                response_json = json.loads(rc.content)
                user_list += (response_json["responses"][0]["value"]["value"])
            except Exception as e:
                print(e)
            
        return user_list
            

        
            
        
