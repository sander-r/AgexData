# -*- coding: utf-8 -*-
import scrapy
from ..items import Scrapy01Item
import pkgutil
import codecs
import os


class OpencompaniesSpider(scrapy.Spider):
    name = "opencompanies"
    allowed_domains = ["opencompanies.nl"]
    
    # Source file
    source_file = "links.txt"
    url_baseline = 'https://www.opencompanies.nl/'
    
    lines = data.split('\n')
    
    start_urls = []
    for l in lines:
    	start_urls.append(l.strip())
        #start_urls.append('{}{}'.format(url_baseline, l.strip()))
    
    def parse(self, response):
        # url_origin = response.request.meta['redirect_urls']
        # print('** {}'.format(url_origin))

        item = Scrapy01Item()
        item['url'] = ''
        item['phonenumber'] = ''
        item['employees'] = ''
        item['foundingdate'] = response.xpath('//span[@fieldname="foundingdate"]/text()').extract_first()
        item['legalform'] = response.xpath('//p[@fieldname="legalform"]/text()').extract_first()
        item['branch'] = response.xpath('normalize-space(//div[@fieldname="branch"]/text())').extract_first()
        
        kvk = response.xpath('//p[@fieldname="cocnumber"]/text()').extract_first()
        if kvk:
            item['kvk_number'] = kvk.strip()

            employees = response.xpath('//div[@fieldname="employees"]/span/text()').extract_first()
            try:
                employees = employees.replace(' werknemers','')
                item['employees'] = int(employees.strip())
            except:
                pass

            links = response.xpath('//div[@class="maps-widget-small__body"]//a/@href').extract()
            for l in links:
                if 'http://' in l.lower() or 'https://' in l.lower():
                    if 'www.linkedin' not in l.lower() and 'www.twitter.com' not in l.lower():
                        item['url'] = l

            # Phone number if exists
            block = response.xpath('//div[@class="maps-widget-small__body"]/p')
            if len(block) > 1:
                item['phonenumber'] = block[1].xpath('normalize-space(./text())').extract_first().strip()

            yield item
