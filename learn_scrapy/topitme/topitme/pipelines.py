# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request
import requests
import scrapy
from .items import ImageItem, TopitmeItem

class TopitmePipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item,TopitmeItem):
            return item

        if spider.name != 'topitme': return item
        if item.get('images_url', None) is None or : return None

        with open('result.txt', 'a') as f:
            f.write(str([item['name'],item['author'],item['img_url'],item['tags']])+'\n')

class MyImagesPipeline(ImagesPipeline):



    def get_media_requests(self, item, info):

        if item is None or not isinstance(item, ImageItem): return item

        cookies = {'BAIDUID':'CCFEE881EFE7F14E0EE747265F33EA4F:FG=1',
                  'BIDUPSID':'CCFEE881EFE7F14E0EE747265F33EA4F',
                  'PSTM=':'1479799183',
                  'BDUSS':'BaVFdvYXlrVjdaSXJOald2bFhlTlJBTEZjYy1Dc1h3VnBEc25iVzhHZU9SV0ZZSVFBQUFBJCQAAAAAAAAAAAEAAAAo~qdQueDEvrTUuLq2~gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI64OViOuDlYOF',
                  'BAIDUCUID':'++',
                  'MCITY':'-179%3A',
                  'BDRCVFR[feWj1Vr5u3D]':'I67x6TjHwwYf0',
                  'PSINO':'5',
                  'H_PS_PSSID=':'22164_1442_13290_21100_20930',
                  'BDORZ':'B490B5EBF6F3CD402E515D22BCDA1598',
                  'cflag':'15%3A3'}

        for image_url in item['image_urls']:
            yield Request(image_url,cookies=cookies)


    def item_completed(self, results, item, info):
        print(results,item,info,info.spider)
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item