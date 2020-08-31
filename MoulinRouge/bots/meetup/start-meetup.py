from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from meetupbot.spiders.meetupbot import MeetupSpider
 
 
process = CrawlerProcess(get_project_settings())
process.crawl(MeetupSpider)
process.start()