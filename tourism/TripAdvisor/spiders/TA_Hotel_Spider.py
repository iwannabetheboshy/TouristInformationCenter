import scrapy
import sqlite3
import traceback
import re
from datetime import datetime, timedelta


def problems(info):
    with open('logging/TA_HOTELS_ERR.log', 'a') as err_log:
        err_log.write(info + '\n')
        err_log.write(traceback.format_exc() + '\n')

def class_problems(info):
    with open('logging/TA_CLASS.log', 'a') as err_log:
        err_log.write(info + '\n')
        err_log.write(traceback.format_exc() + '\n')

def address_problems(info):
    with open('logging/TA_ADDRESS.log', 'a') as err_log:
        err_log.write(info + '\n')
        err_log.write(traceback.format_exc() + '\n')

def city_problems(info):
    with open('logging/TA_ADDRESS.log', 'a') as err_log:
        err_log.write(info + '\n')
        err_log.write(traceback.format_exc() + '\n')



class Companies(scrapy.Spider):
    name = 'TA.hotel_companies'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self):#, category, main_rubric, rubric_id):
        super(Companies, self).__init__()
        #self.category = category
        #self.rubric = [main_rubric] * 30
        #self.rubric_id = rubric_id
        self.start_urls = ['https://www.tripadvisor.ru/Hotels-g2324040-Primorsky_Krai_Far_Eastern_District-Hotels.html']
        self.result = list()
        self.count = 0

    def parse(self, response, **kwargs):
        #print(response.text)
        link = ['https://www.tripadvisor.ru' + i for i in response.css('.property_title::attr(href)').extract()]

        name = [i for i in response.css('.property_title::text').extract()]
        name = [i[i.find('.') + 2 :] for i in name]

        rub = [i for i in response.css('.info-col').extract() ]
        rub = [re.findall('label">(.*?)</span>', i)[0] if 'label">' in i else 'Гостиницы, отели' for i in rub]

        self.count = self.count + 30

        self.result.extend(zip(rub, name, link))
        print(len((self.result)))

        if len(link) == 30:
            yield scrapy.Request(
                url='https://www.tripadvisor.ru/Hotels-g2324040-oa{}-Primorsky_Krai_Far_Eastern_District-Hotels.html'
                    .format(self.count),
                callback=self.parse, dont_filter=True)


    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format('hotel'))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                               (rubric TEXT, company TEXT, city TEXT, address TEXT,
                               phone TEXT, url TEXT NOT NULL UNIQUE)''')
        counter = 0
        for item in self.result:
            try:
                cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
                                   (item[0], item[1], '', '', '', item[2]))
            except:
                counter += 1
                cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
                               (item[0], item[1], '', '', '', counter))
        connection.commit()
        connection.close()


class Companies_Phone(scrapy.Spider):
    name = 'TA.hotel_companies_address'
    allowed_domains = ['tripadvisor.ru']

    def __init__(self, category, company_link):
        super(Companies_Phone, self).__init__(category)
        self.category = category
        self.company_link = company_link
        self.clear_addr = ['проспект', 'посёлок', 'улица', 'переулок', 'ул.', 'пр.', 'пр-т', 'мыс', 'бульвар', 'площадь', 'бухта', 'д.', 'дом', 'д']


    def start_requests(self):
        yield scrapy.Request(
            url=self.company_link, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            self.city = response.css(".breadcrumbs li span::text").extract()[-1].split()[-1]
            self.rubric = response.css(".dlMOJ::text").extract()

        except:
            city_problems(self.company_link)
            self.rubric = 'Остальное'

        try:
            self.address = ' '.join(response.css(".fHvkI::text").extract_first().split(', ')[:-1])
        except:
            problems(self.company_link)
            try:
                self.address = ' '.join(response.css(".gZwVG span span::text").extract_first().split(', ')[:-1])
            except:
                address_problems(self.company_link)

        for j in self.clear_addr:
            if j in self.address:
                self.address = re.sub(j, '', self.address).strip()
        self.address = re.sub(' +', ' ', self.address)

        try:
            self.class_hotel = response.css(".JXZuC::attr(aria-label)").extract_first()[0]
        except:
            self.class_hotel = 'None'
            class_problems(self.company_link)

    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute("UPDATE trip SET city=?, address=? WHERE url=?",
                       (self.city, self.address.lower(), self.company_link))
        connection.commit()
        connection.close()


class Reviews(scrapy.Spider):
    name = 'TA.hotel_reviews'
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
            review = response.css(".QewHA span::text").extract()

            rating = response.css('.IkECb .ui_bubble_rating').xpath("@class").extract()
            rating = [int(i[-2:]) / 50 for i in rating]

            month = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            created_at = response.css(".teHYY::text").extract()
            for i in range(len(created_at)):
                if 'г.' in created_at[i]:
                    created_at[i] = created_at[i][:-3]

                for j in range(len(month)):
                    if month[j] in created_at[i]:
                        year = created_at[i][-4:] + '-'
                        created_at[i] = year + str(j) + '-00'
                        if j < 10:
                            created_at[i] = year + '0' + str(j) + '-00'
                        break




            self.reviews.extend(zip(review, rating, created_at))
            print(len(self.reviews))

            next_page = response.css(".ui_pagination .next::attr(href)").extract()
            if next_page:
                yield scrapy.Request(
                    url='https://www.tripadvisor.ru/' + next_page[0], callback=self.parse)
        except Exception as e:
            print(e)

    def close(self, reason):
        connection = sqlite3.connect('db/reviews_{}.db'.format('hotel'))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                                       (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.reviews:
            cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()