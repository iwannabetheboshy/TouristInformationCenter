import scrapy
import json
import re
import sqlite3
from datetime import datetime
import traceback
import time
from collections import Counter


class Companies(scrapy.Spider):
    name = 'vl.companies'
    allowed_domains = ['vl.ru']

    def __init__(self):
        super(Companies, self).__init__()
        self.start_urls = ['https://www.vl.ru/travel?page=1']

        self.vl_prefix = 'https://www.vl.ru'
        self.result = list()
        self.name = list()

    def parse(self, response, **kwargs):
        self.name.extend(response.xpath("//header[@class='company__header']//h4//a/text()").extract())

        # comments = [i.strip() for i in response.css(".stat-list a::text").extract()]
        # link = [i.strip() for i in response.css(".stat-list a::attr(href)").extract()]
        #
        # self.result.extend(zip(link, comments))
        #
        # for i in self.result:
        #     print(i)
        #

        for i in self.name:
            print(i)

        print(len(self.name))
        next_page = response.css('#link-next::attr(href)').extract_first()
        yield scrapy.Request(response.urljoin(next_page), callback=self.parse)

    def close(self, reason):
        pass
        # connection = sqlite3.connect('db/1.db')
        # cursor = connection.cursor()
        # cursor.execute('''CREATE TABLE IF NOT EXISTS vl
        #                        (company TEXT, review TEXT)''')
        #
        # for item in self.result:
        #     cursor.execute("INSERT INTO vl VALUES (?, ?)",
        #                    (item[0], item[1]))
        #     connection.commit()
        # connection.close()

class row(scrapy.Spider):
    name = 'TA.hotel_companies'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self):
        super(row, self).__init__()
        self.start_urls = ['https://www.tripadvisor.ru/Hotels-g2324040-Primorsky_Krai_Far_Eastern_District-Hotels.html']
        self.result = list()
        self.count = 0

    def parse(self, response, **kwargs):
        #print(response.text)
        link = ['https://www.tripadvisor.ru' + i for i in response.css('.property_title::attr(href)').extract()]

        comments = [i for i in response.css('.review_count::text').extract()]

        self.result.extend(zip(link, comments))

        for i in self.result:
            print(i)

        self.count = self.count + 30


        if len(link) == 30:
            yield scrapy.Request(
                url='https://www.tripadvisor.ru/Hotels-g2324040-oa{}-Primorsky_Krai_Far_Eastern_District-Hotels.html'
                    .format(self.count),
                callback=self.parse, dont_filter=True)


    def close(self, reason):
        connection = sqlite3.connect('db/1.db')
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                               (company TEXT, review TEXT)''')

        for item in self.result:
            cursor.execute("INSERT INTO trip VALUES (?, ?)",
                           (item[0], item[1]))
            connection.commit()
        connection.close()

