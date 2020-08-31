from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from eventbrite.spiders.eventbritebot import EventbriteSpider
 
 
process = CrawlerProcess(get_project_settings())
process.crawl(EventbriteSpider)
process.start()