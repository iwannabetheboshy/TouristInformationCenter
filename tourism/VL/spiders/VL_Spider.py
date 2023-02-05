import scrapy
import json
import re
import sqlite3
from datetime import datetime
import traceback
import time
from collections import Counter



def created_at_problems(info):
    with open('logging/VL_error_date.log', 'a') as err_log:
        err_log.write(info + '\n')
        err_log.write(traceback.format_exc() + '\n')

def logging_info(info, name):
    with open('logging/VL_match_companies.log', 'a') as err_log:
        err_log.write(name + '\n')
        err_log.write(info + '\n')

def search_error(name):
    with open('logging/VL_search_error.log', 'a') as err_log:
        err_log.write(name + '\n')
        err_log.write(traceback.format_exc() + '\n')


class Companies(scrapy.Spider):
    name = 'vl.companies'
    allowed_domains = ['vl.ru']

    def __init__(self, category, main_rubric):
        super(Companies, self).__init__(category)
        self.category = category
        self.main_rubric = main_rubric
        self.clear_addr = ['пос. ', 'пер. ', 'пр-т ', 'ул. ']
        self.start_urls = [
            'https://www.vl.ru/primorskij-kraj/{}?page=1'.format(self.main_rubric)
        ]
        self.vl_prefix = 'https://www.vl.ru'
        self.result = list()

    def parse(self, response, **kwargs):
        link = [self.vl_prefix + i for i in response.xpath('''//header[@class='company__header']//h4//a/@href | 
                                                   //div[contains(@class, 'search-title')]/text()''').extract()]
        name = response.xpath("//header[@class='company__header']//h4//a/text()").extract()

        #rubric = response.xpath("//div[@class='company__activity-type text-light is-advt-hide']/text()").extract()

        rubric = []
        for i in response.xpath("//div[@class='company__info']").extract():
            if 'company__activity-type text-light is-advt-hide' in i:
                rubric_ind_l = i.find('company__activity-type text-light is-advt-hide')
                rubric_ind_r = i[rubric_ind_l:].find('</div>')
                rubric.append(i[rubric_ind_l+48 : rubric_ind_l+rubric_ind_r])
            else:
                rubric.append('')

        address = list()
        city = list()
        for i in response.xpath("//div[@class='contacts__row address-row']/text()").extract():
            i = i.strip().lower()
            if i:
                i = i.split(', ')
                if i[0] == 'приморский край':
                    i.pop(0)
                city.append(i[0])
                try:
                    if ' ' not in i[-1]:
                        address.append(i[-2][i[-2].find(' ') + 1 :] + ' ' + i[-1])
                    else:
                        address.append(i[-2][i[-2].find(' ') + 1 :] + ' ' + i[-1][: i[-1].find(' ') + 1])
                except:
                    address.append('')

        if self.vl_prefix + 'Возможно вам подойдёт' in link:
            link = link[: link.index(self.vl_prefix + 'Возможно вам подойдёт')]


        if self.main_rubric == 'travel':
            travel_city = response.xpath("//header[@class='company__header']//h4/text()").extract()
            travel_city = [travel_city[i].strip()[1:-1] for i in range(len(travel_city)) if i % 2 != 0]
            rubric = ['Базы отдыха'] * len(name)
            address = [''] * len(name)
            self.result.extend(zip(rubric, name, travel_city, address, link))
            print(travel_city)
            print(len(travel_city))
        else:
            self.result.extend(zip(rubric, name, city, address, link))

        if len(link) != len(name):
            return

        next_page = response.css('#link-next::attr(href)').extract_first()
        yield scrapy.Request(response.urljoin(next_page), callback=self.parse)


    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS vl
                       (rubric TEXT, company TEXT, city TEXT, address TEXT,
                       phone TEXT, url TEXT NOT NULL UNIQUE)''')
        for item in self.result:
            try:
                cursor.execute("INSERT INTO vl VALUES (?, ?, ?, ?, ?, ?)",
                               (item[0], item[1], item[2], item[3], '', item[4]))
            except:
                cursor.execute("SELECT rubric FROM vl WHERE URL=?", (item[4],))
                update_rubric = cursor.fetchone()
                if item[0] == update_rubric[0]:
                    continue
                update_rubric = update_rubric[0] + '; ' + item[0]
                cursor.execute("UPDATE vl SET rubric=? WHERE url=?", (update_rubric, item[4]))
        connection.commit()
        connection.close()


class Companies_Phone(scrapy.Spider):
    name = 'vl.companies'
    allowed_domains = ['vl.ru']

    def __init__(self, category, company_link):
        super(Companies_Phone, self).__init__(category)
        self.category = category
        self.company_link = company_link
        self.clear_addr = ['пос. ', 'пер. ', 'пр-т ', 'ул. ']

    def start_requests(self):
        yield scrapy.Request(url=self.company_link, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            self.phone = response.css('.phone-wrap__inline span::attr(data-phone)').extract_first()[-7:]
        except:
            self.phone = 'None'
            with open('logging/error_phone_VL.log', 'a') as err_log:
                err_log.write(self.company_link + '\n')
                err_log.write(traceback.format_exc() + '\n')

        try:
            self.address = response.css('.company-contacts__column .company-contacts__address span::text').extract_first()
        except:
            self.address = ''

        self.address = self.address.strip().split(', ')[-2:]
        self.address = ' '.join(self.address)
        self.address = self.address[self.address.find(' ') + 1 :].lower()
        print(self.address)



    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute("UPDATE vl SET address=?, phone=? WHERE url=?", (self.address, self.phone, self.company_link))
        connection.commit()
        connection.close()
        time.sleep(6)


class Reviews(scrapy.Spider):
    name = 'vl.comments'
    allowed_domains = ['vl.ru']

    def __init__(self, category, row):
        super(Reviews, self).__init__(category)
        self.company_link = '/picca-studio'
        self.company = row[0]
        self.company_link = row[1]
        self.category = category
        self.headers = {
            'Host': 'www.vl.ru',
            'Accept': 'application / json, text / javascript, * / *; q = 0.01',
            'Referer': 'https://vl.ru/baza-mayk.ru'
        }
        self.treadId = ''
        self.threadTitle = ''
        self.result = list()

    def start_requests(self):
        yield scrapy.http.Request(
                'https://vl.ru/commentsgate/ajax/thread/company/{0}/embedded?theme=company&appVersion=202261155937&_dc=0.7029149485736903&pastafarian=64297a3a5b3633c9c680bdb77f7f9bc03aa0f9d9209a29484c4ba9b46b78ca20&location=https%3A%2F%2Fwww.vl.ru%2F{0}&moderatorMode=1'
                .format(self.company_link[self.company_link.rfind('/') + 1 : ]),
            headers=self.headers)

    def parse(self, response, **kwargs):

        try:
            json_data = json.loads(response.text)['data']

            comments_json = [i for i in re.findall('cmt-content".*?/blockquote', json_data['content'], re.DOTALL) if
                             'ответ на <a' not in i]

            try:
                created_at = [re.findall('time">(.*?)</span>', i)[0][:-3] for i in comments_json]
            except IndexError:
                created_at = [re.findall('"time"(.*?)</a>', i)[0][:-3] for i in comments_json]
                created_at = [i[i.rfind('>')+1 :] for i in created_at]
                created_at_problems(self.company_link)

            created_at = self.date_converter(created_at)

            reviews = [re.sub('<[^>]+>', '', re.findall('comment-text.*?/p>', i, re.DOTALL)[0]) for i in comments_json]
            rating = [i[i.find('value') + 7: i.find('value') + 10].rstrip(' "') if 'data-value' in i else 'None' for i in
                      comments_json]

            if len(reviews) > 10:
                reviews = reviews[-10:]

            self.result.extend(zip(reviews, rating, created_at))

            try:
                self.threadId = json_data['threadId']
            except KeyError:
                pass
            lastCommentId = json_data['lastCommentId']

            if lastCommentId is None:
                return
            yield scrapy.http.Request('https://vl.ru/commentsgate/ajax/comments/{}/rendered?theme=company&appVersion=202261155937&_dc=0.423971594572383&before={}&pastafarian=64297a3a5b3633c9c680bdb77f7f9bc03aa0f9d9209a29484c4ba9b46b78ca20&moderatorMode=1&commentAttributes%5BcommentType%5D%5B%5D=review'
                                      .format(self.threadId, lastCommentId), headers=self.headers)
        except Exception as e:
            created_at_problems(self.company_link)

    def date_converter(self, date):
        month = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
        date = [_.split()[-3:] for _ in date]

        for i in range(len(date)):
            if 'на' in date[i]:
                date[i] = datetime.now().date()
                continue

            if date[i][0].isalpha():
                pass
            if len(date[i][0]) == 1:
                date[i][0] = '0' + date[i][0]
            date[i][1] = str(month.index(date[i][1][:3]) + 1)
            if len(date[i][1]) == 1:
                date[i][1] = '0' + date[i][1]
            date[i] = date[i][2] + '-' + date[i][1] + '-' + date[i][0]
        return date

    def close(self, reason):
        self.result = [i for i in self.result if 'cmt-collapse-comment' in i[0]]
        connection = sqlite3.connect('db/reviews_{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS vl
                       (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.result:
            cursor.execute('INSERT INTO vl VALUES (?, ?, ?, ?, ?)',
                           (item[0][35:], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()