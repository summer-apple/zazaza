# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request
from .items import ImageItem

class TopitmePipeline(object):
    def process_item(self, item, spider):
        if isinstance(item,ImageItem):
            return item

        if spider.name != 'topitme': return item
        if item.get('img_url', None) is None: return item

        with open('result.txt', 'a') as f:
            f.write(str([item['name'],item['author'],item['img_url'],item['tags']])+'\n')

        return item

class MyImagesPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield Request(image_url)


    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item