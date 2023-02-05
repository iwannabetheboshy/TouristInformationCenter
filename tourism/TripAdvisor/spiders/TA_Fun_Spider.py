import scrapy
import sqlite3
import traceback
import re
from itertools import groupby


class Fun_Companies(scrapy.Spider):
    name = 'TA.fun_companies'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self, category):
        super(Fun_Companies, self).__init__(category)
        self.category = category
        self.start_urls = ['https://www.tripadvisor.ru/Attractions-g2324040-Activities-a_allAttractions.true-Primorsky_Krai_Far_Eastern_District.html']
        self.result = list()
        self.count = 0

    def parse(self, response, **kwargs):
        name = [i for i in response.css('.biGQs .XfVdV::text').extract() if i!= ' ']
        rubric = [i for i in response.css('.C .jemSU .dxkoL .BKifx .biGQs::text').extract()
                  if 'Открыто сейчас' not in i]

        links = [i for i in response.css('.C .VLKGO .alPVI .BMQDV::attr(href)').extract()]
        city = []
        for i in range(len(links)):
            links[i] = 'https://www.tripadvisor.ru' + links[i][:-8]
            city.append(links[i].split('-')[-1].split('_')[0])

        self.result.extend(zip(rubric, name, city, links))

        print(len(self.result))
        self.count = self.count + 30

        if len(rubric) != 0:
            yield scrapy.Request(
                url='https://www.tripadvisor.ru/Attractions-g2324040-Activities-oa{}-Primorsky_Krai_Far_Eastern_District.html'.format(self.count),
                callback=self.parse,
                dont_filter=True)

    def close(self, reason):
        pass
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                                       (rubric TEXT, company TEXT, city TEXT, address TEXT,
                                       phone TEXT, url TEXT NOT NULL UNIQUE)''')
        for item in self.result:
            cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], '', '', item[3]))
        connection.commit()
        connection.close()

#
#
#
#
# class Fun_Review_Spider(scrapy.Spider):
#     name = 'TA.review'
#     allowed_domains = ['tripadvisor.ru']
#
#     def __init__(self, sub_category, company_name, company_link):
#         super(Fun_Review_Spider, self).__init__(sub_category)
#         self.sub_category = sub_category
#         self.company_name = company_name
#         self.company_link = company_link
#         self.company_city = ''
#         self.result = list()
#         self.start_urls = ['https://www.tripadvisor.ru' + self.company_link]
#
#     def parse(self, response, **kwargs):
#         try:
#             review = [re.sub('<[^>]+>', '', i[20:-7]) for i in response.css(".bmUTE .yCeTE").extract()]
#             rating = [int(i[0]) / 5 for i in response.css(".LbPSX .UctUV::attr(aria-label)").extract() if i]
#             created_at = [i[13:i.find(' г')] for i in response.css(".TreSq .ncFvv::text").extract()]
#             created_at = self.date_converter(created_at)
#             self.result.extend(zip(review, rating, created_at))
#
#             try:
#                 self.company_city = response.css(".KCGqk span span::text").extract()[4]
#             except:
#                 self.company_city = response.css("#taplc_trip_planner_breadcrumbs_0 li a span::text").extract()[4]
#
#             try:
#                 next_page = response.css(".ui_pagination .next::attr(href)").extract_first()
#             except:
#                 next_page = response.css(".xkSty a::attr(href)").extract_first()
#
#             if next_page:
#                 print(len(self.result))
#                 yield scrapy.Request(
#                     url='https://www.tripadvisor.ru' + next_page,
#                     callback=self.parse,
#                     dont_filter=True)
#         except:
#             problems(self.sub_category, self.company_name, self.company_link)
#
#     def date_converter(self, date):
#         month = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
#         date = [_.split()[-3:] for _ in date]
#
#         for i in range(len(date)):
#             if date[i][0].isalpha():
#                 pass
#             if len(date[i][0]) == 1:
#                 date[i][0] = '0' + date[i][0]
#             date[i][1] = str(month.index(date[i][1][:3]) + 1)
#             if len(date[i][1]) == 1:
#                 date[i][1] = '0' + date[i][1]
#             date[i] = date[i][2] + '-' + date[i][1] + '-' + date[i][0]
#         return date
#
#     def close(self, reason):
#         for i in self.result:
#             print(i)
#         # connection = sqlite3.connect('db/reviews.db')
#         # cursor = connection.cursor()
#         # cursor.execute('''CREATE TABLE IF NOT EXISTS trip
#         #                 (review TEXT, rating REAL, date TEXT, company TEXT, city TEXT, sub_category TEXT)''')
#         #
#         # for item in self.result:
#         #     cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
#         #                    (item[0], item[1], item[2], self.company_name, self.company_city, self.sub_category))
#         #     connection.commit()
#         # connection.close()
#
#
#






