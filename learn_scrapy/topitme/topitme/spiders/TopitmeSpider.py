import scrapy
from scrapy.selector import Selector
from scrapy.spider import CrawlSpider
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.spiders import CrawlSpider,Rule
from bs4 import BeautifulSoup
import re
from ..items import TopitmeItem,ImageItem

class TopitmeSpider(CrawlSpider):
    name = 'topitme'
    allowed_domains = ['topit.me']
    start_urls = ['http://www.topit.me/user/255686']
    # rules = (
    #     Rule(LinkExtractor(allow=r'http://www.topit.me/user/255686*'),
    #          callback='parse', follow=True),
    # )


    def parse(self,response):
        next_page = response.xpath('//a[@id="page-next"]/@href').extract()[0]
        yield scrapy.Request(next_page, callback=self.parse)

        img_list = response.xpath('//div[@class="e m"]/a/@href').extract()
        for url in img_list:
            yield scrapy.Request(url, callback=self.parse_image)

    def parse_image(self,response):
        img_url = response.xpath('//a[@download]/@href').extract()[0]
        name = response.xpath('//div[@class="pageheader entrypage"]/h2/text()').extract()[0]
        author = response.xpath('//div[@class="pageheader entrypage"]/p[1]/a/span/text()').extract()[0]
        tags = response.xpath('//div[@class="box tags"]/ul/li/a/text()').extract()

        item = TopitmeItem(name=name,author=author,img_url=img_url,tags=tags)
        yield item

        img_item = ImageItem(image_urls = [img_url,])
        yield img_item



