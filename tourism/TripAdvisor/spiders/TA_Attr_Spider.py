import scrapy
import sqlite3
import traceback
import re
from datetime import datetime, timedelta


class Companies(scrapy.Spider):
    name = 'TA.attr_companies'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self, category, main_rubric):
        super(Companies, self).__init__(category)
        self.category = category
        self.rubric = [main_rubric] * 30
        self.start_urls = ['https://www.tripadvisor.ru/Attractions-g2324040-Activities-oa0-Primorsky_Krai_Far_Eastern_District.html']
        self.result = list()
        self.count = 0

    def parse(self, response, **kwargs):
        print(response.text)
        # link = ['https://www.tripadvisor.ru' + i for i in response.css('.RfBGI span a::attr(href)').extract()]
        # name = [i for i in response.css('.RfBGI span a::text').extract()]
        # name = [name[i] for i in range(2, len(name), 3)]
        # city = [i for i in response.css('.QZaIQ .NKeoZ div::text').extract()]
        # city = [city[i].lower() for i in range(1, len(city), 2)]
        #
        # self.count = self.count + 30
        # self.result.extend(zip(self.rubric, name, city, link))
        #
        # print(len(self.result))
        #
        # if len(self.rubric) == len(link):
        #     yield scrapy.Request(
        #         url='https://www.tripadvisor.ru/RestaurantSearch?Action=PAGE&ajax=1&availSearchEnabled=false&sortOrder=popularity&geo=2324040&itags={}&o=a{}'
        #             .format(self.rubric_id, self.count),
        #         callback=self.parse, dont_filter=True)

    def close(self, reason):
        pass
        # connection = sqlite3.connect('db/{}.db'.format(self.category))
        # cursor = connection.cursor()
        # cursor.execute('''CREATE TABLE IF NOT EXISTS trip
        #                        (rubric TEXT, company TEXT, city TEXT, address TEXT,
        #                        phone TEXT, url TEXT NOT NULL UNIQUE)''')
        # for item in self.result:
        #     try:
        #         cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
        #                        (item[0], item[1], item[2], '', '', item[3]))
        #     except:
        #         cursor.execute("SELECT rubric FROM trip WHERE URL=?", (item[3],))
        #         update_rubric = cursor.fetchone()
        #         if self.rubric[0] in update_rubric[0]:
        #             continue
        #         update_rubric = update_rubric[0] + '; ' + self.rubric[0]
        #         cursor.execute("UPDATE trip SET rubric=? WHERE url=?", (update_rubric, item[3]))
        # connection.commit()
        # connection.close()



class Reviews(scrapy.Spider):
    name = 'TA.eat_reviews'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self, category, row):
        super(Reviews, self).__init__(category)
        self.category = category
        self.company = row[0]
        self.company_link = row[1]
        self.reviews = list()
        self.start_urls = [self.company_link]

    def parse(self, response, **kwargs):
        try:
            review = response.css(".partial_entry::text").extract()

            rating = response.css('.listContainer .ui_bubble_rating').xpath("@class").extract()
            rating = [int(i[-2:]) / 50 for i in rating]

            created_at = response.css(".listContainer .ratingDate::text").extract()
            for i in range(len(created_at)):
                if 'Отзыв написан' in created_at[i]:
                    created_at[i] = created_at[i][14:]

                if 'г.' in created_at[i]:
                    created_at[i] = created_at[i][:-3]

            created_at = self.date_converter([i.strip() for i in created_at])

            self.reviews.extend(zip(review, rating, created_at))
            print(len(self.reviews))

            next_page = response.css(".ui_pagination .next::attr(href)").extract()
            if next_page:
                yield scrapy.Request(
                    url='https://www.tripadvisor.ru/' + next_page[0], callback=self.parse)
        except:
            pass

    def date_converter(self, date):
        month = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
        date = [_.split()[-3:] for _ in date]

        for i in range(len(date)):
            if 'назад' in date[i][-1]:
                if 'нед' in date[i][1]:
                    date[i] = datetime.now().date() - timedelta(days = 7 * int(date[i][0]))
                elif 'дне' in date[i][1]:
                    date[i] = datetime.now().date() - timedelta(days=int(date[i][0]))
                else:
                    date[i] = datetime.now().date()
                continue

            if date[i][0].isalpha():
                date[i] = datetime.now().date()
                continue
            if len(date[i][0]) == 1:
                date[i][0] = '0' + date[i][0]
            date[i][1] = str(month.index(date[i][1][:3]) + 1)
            if len(date[i][1]) == 1:
                date[i][1] = '0' + date[i][1]
            date[i] = date[i][2] + '-' + date[i][1] + '-' + date[i][0]
        return date

    def close(self, reason):
        connection = sqlite3.connect('db/reviews_{}.db'.format('2222'))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                                       (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.reviews:
            cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()