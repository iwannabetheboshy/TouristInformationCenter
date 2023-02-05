import scrapy
import sqlite3
import traceback
import re
from datetime import datetime, timedelta


class Companies(scrapy.Spider):
    name = 'TA.culture_companies'
    allowed_domains = ['tripadvisor.ru']

    headers = {
        'Host': 'www.tripadvisor.ru',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5359.72 Safari/537.36',
        'Accept': 'text/html, */*',
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    def __init__(self, category, main_rubric):
        super(Companies, self).__init__(category)
        self.category = category
        self.main_rubric = main_rubric
        self.rubric = [main_rubric] * 30
        self.result = list()
        self.clear_addr = ['проспект', 'посёлок', 'улица', 'переулок', 'ул.', 'пр.', 'пр-т', 'мыс', 'бульвар', 'площадь', 'бухта', 'д.', 'дом', 'д']


    def start_requests(self):
        yield scrapy.Request(
            url='https://www.tripadvisor.ru/Search?q=Приморский край {}&searchSessionId=2576EA50BEEFF4945D8A9210EF8008401669971152782ssid&searchNearby=&geo=1&sid=577E9024507605A5830AA908084FF9B61669971407025&blockRedirect=true&ssrc=A&rf=11&o=0&withFilters=true&firstEntry=true'
            .format(self.main_rubric),
            headers=self.headers
)

    def parse(self, response, **kwargs):

        name = [i for i in response.css('.location-meta-block .result-title span::text').extract()]
        full_address = [i for i in response.css('.location-meta-block .address .address-text::text').extract()]

        city = []
        link = [i for i in response.css('.location-meta-block .result-title::attr(onclick)').extract()]
        for i in range(len(link)):
            link[i] = 'https://www.tripadvisor.ru' + link[i].split(', ')[3].strip("'")
            city.append(link[i].split('-')[-1].split('_')[0])

        rubric = [i for i in response.css('.thumbnail').extract()]
        for i in range(len(rubric)):
            i_l = rubric[i].find('overlay-tag">')
            i_r = rubric[i].rfind('</span>')
            rubric[i] = rubric[i][i_l+13 : i_r]
            if self.main_rubric not in rubric[i].lower():
                rubric[i] = ''
            else:
                rubric[i] = self.main_rubric

        union_companies = list(zip(rubric, name, city, full_address, link))

        for item in union_companies:
            if 'Приморский край' in item[3]:
                try:
                    part_address = item[3].split(',')[0:2]
                    for i in range(len(part_address)):
                        for j in self.clear_addr:
                            if j in part_address[i]:
                                part_address[i] = part_address[i].replace(j, '')
                        part_address[i] = part_address[i].lower().strip()
                    part_address = ' '.join(part_address)
                except:
                    part_address = ''

                if item[0] != '':
                    self.result.append([item[0], item[1], item[2], part_address, item[4]])
        # print(len(union_companies))
        print(len(self.result))

        # for i in self.result:
        #     print(i)


        next_page_offset = response.css('.ui_pagination .next::attr(data-offset)').extract_first()
        if next_page_offset:
            yield scrapy.Request(
                url='https://www.tripadvisor.ru/Search?q=Приморский край {}&searchSessionId=2576EA50BEEFF4945D8A9210EF8008401669971152782ssid&searchNearby=&geo=1&sid=577E9024507605A5830AA908084FF9B61669971407025&blockRedirect=true&ssrc=A&rf=11&o={}&withFilters=true&firstEntry=true'
                    .format(self.main_rubric, next_page_offset),
                    headers=self.headers)
        print(next_page_offset)

    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                               (rubric TEXT, company TEXT, city TEXT, address TEXT,
                               phone TEXT, url TEXT NOT NULL UNIQUE)''')
        for item in self.result:
            try:
                cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?, ?)',
                               (item[0], item[1], item[2], item[3], '', item[4]))
            except:
                cursor.execute("SELECT rubric FROM trip WHERE URL=?", (item[4],))
                update_rubric = cursor.fetchone()
                if self.main_rubric in update_rubric[0]:
                    continue
                update_rubric = update_rubric[0] + '; ' + self.main_rubric
                cursor.execute("UPDATE trip SET rubric=? WHERE url=?", (update_rubric, item[3]))
        connection.commit()
        connection.close()


class Companies_Phone(scrapy.Spider):
    name = 'TA.eat_companies_address'
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
            self.phone = response.xpath("//span[@class='AYHFM']//a/@href").extract_first()
            self.phone = re.sub('[^0-9]', '', self.phone)[-7:]

        except:
            self.phone = 'None'


    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute("UPDATE trip SET phone=? WHERE url=?", (self.phone, self.company_link))
        connection.commit()
        connection.close()


class Reviews(scrapy.Spider):
    name = 'TA.culture_reviews'
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
            reviews = response.xpath("//div[@class='C']//div[@class='fIrGe _T bgMZj']//span[@class='yCeTE']").extract()
            reviews = [i[20:-7] for i in reviews if i]

            rating = response.css('.C ._c .UctUV').xpath("@aria-label").extract()
            rating = [int(i[0])/5 for i in rating if i]

            created_at = response.css('.C ._c .TreSq .biGQs::text').extract()
            created_at = [re.sub('[.|,]', '', i[13:-2].strip()) for i in created_at if i.startswith('Опубликовано')]

            month = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            for i in range(len(created_at)):
                if 'г.' in created_at[i]:
                    created_at[i] = created_at[i][:-3]

                for j in range(len(month)):
                    if month[j] in created_at[i]:
                        year = created_at[i][-4:] + '-'
                        created_at[i] = year + str(j+1) + '-00'
                        if j+1 < 10:
                            created_at[i] = year + '0' + str(j+1) + '-00'
                        break

            self.reviews.extend(zip(reviews, rating, created_at))
            print(len(self.reviews))


            next_page = response.css(".lATJZ .xkSty .UCacc a::attr(href)").extract()
            if next_page:
                yield scrapy.Request(
                    url='https://www.tripadvisor.ru' + next_page[0], callback=self.parse)
        except:
            pass


    def close(self, reason):
        connection = sqlite3.connect('db/reviews_{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS trip
                                       (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.reviews:
            cursor.execute('INSERT INTO trip VALUES (?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()