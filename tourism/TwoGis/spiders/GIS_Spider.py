import scrapy
import json
from itertools import groupby
import urllib
from urllib.parse import quote_plus
import sqlite3
import re
import traceback


class Companies(scrapy.Spider):
    name = 'gis.company'
    allowed_domains = ['2gis.ru', 'catalog.api.2gis.ru']
    cookies = {
        '_2gis_webapi_user': 'a4a10f05-a5a8-4adb-a6e2-e8fcdca99017',
        '_ga': 'GA1.2.876134808.1652712422',
        '_gid': 'GA1.2.460225824.1653565934',
        '_ym_uid': '1652712423363044238',
        '_ym_d': '1652712423',
        '_2gis_webapi_session': '045a1a5e-59e7-4e12-b2ba-5772ebcdb38d',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, category, main_rubric):
        super(Companies, self).__init__(category)
        self.category = category
        self.main_rubric = main_rubric
        self.clear_addr = ['проспект', 'посёлок', 'улица', 'переулок', 'мыс', 'бульвар', 'площадь', 'бухта', 'владивостока']
        self.result = list()
        self.count = 1

    def start_requests(self):
        yield scrapy.Request(
            url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=branch&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                .format(self.count, self.main_rubric),
            cookies=self.cookies,
            callback=self.parse)

    def parse(self, response, **kwargs):
        scraped = json.loads(response.text)

        try:
            real_rubric = []
            address_name = []
            city = []
            company_name = []
            links = []
            try_rub = []

            for i in scraped['result']['items']:
                try:
                    real_rubric.append(i['rubrics'][0]['name'])
                except Exception as e:
                    print(e)
                    real_rubric.append('')

                try:
                    try_rub.append([j['name'] for j in i['rubrics']])
                except:
                    try_rub.append('None')

                try:
                    i['address_name'] = i['address_name'].lower()
                    for clear_addr_item in self.clear_addr:
                        if clear_addr_item in i['address_name']:
                            i['address_name'] = i['address_name'].replace(clear_addr_item, '')

                    i['address_name'] = re.sub('[,|.]', '', i['address_name'])
                    address_name.append(i['address_name'].lower().strip())
                except Exception as e:
                    print(e)
                    address_name.append('')

                try:
                    city.append(i['adm_div'][2]['name'])
                except Exception as e:
                    print(e)
                    city.append('')

                try:
                    company_name.append(i['name_ex']['primary'])
                except Exception as e:
                    print(e)
                    company_name.append('')

                try:
                    links.append('https://2gis.ru/search/{}/firm/{}/'\
                                .format(i['adm_div'][2]['name'],
                                        i['id'][ : i['id'].find('_')])
                                 )
                except Exception as e:
                    print(e)
                    links.append('https://2gis.ru/search/{}/firm/{}/' \
                                 .format(i['adm_div'][2]['name'],
                                         i['id'][: i['id'].find('_')])
                                 )

            self.result.extend(list(zip(try_rub, company_name, city, address_name, links)))
            self.count = self.count + 1

            yield scrapy.Request(
                url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=branch&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                    .format(self.count, self.main_rubric),
                cookies=self.cookies,
                callback=self.parse)

        except Exception as e:
            print(e)
            return


    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS gis
                       (rubric TEXT, company TEXT, city TEXT, address TEXT,
                       phone TEXT, url TEXT NOT NULL UNIQUE)''')
        for item in self.result:
            try:
                cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?, ?)',
                               ('; '.join(item[0]), item[1], item[2], item[3], '', item[4]))
            except Exception as ex:
                print(ex)
                pass
                # cursor.execute("SELECT rubric FROM gis WHERE URL=?", (item[4],))
                # update_rubric = cursor.fetchone()
                # if item[0] in update_rubric[0]:
                #     continue
                # update_rubric = update_rubric[0] + '; ' + item[0]
                # cursor.execute("UPDATE gis SET rubric=? WHERE url=?", (update_rubric, item[4]))
        connection.commit()
        connection.close()


class Companies_Attr(scrapy.Spider):
    name = 'gis.attr.company'
    allowed_domains = ['2gis.ru', 'catalog.api.2gis.ru']
    cookies = {
        '_2gis_webapi_user': 'a4a10f05-a5a8-4adb-a6e2-e8fcdca99017',
        '_ga': 'GA1.2.876134808.1652712422',
        '_gid': 'GA1.2.460225824.1653565934',
        '_ym_uid': '1652712423363044238',
        '_ym_d': '1652712423',
        '_2gis_webapi_session': '045a1a5e-59e7-4e12-b2ba-5772ebcdb38d',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, category, main_rubric):
        super(Companies_Attr, self).__init__(category)
        self.category = category
        self.main_rubric = main_rubric
        self.clear_addr = ['проспект', 'посёлок', 'улица', 'переулок', 'мыс', 'бульвар', 'площадь', 'бухта']
        self.result = list()
        self.count = 1
        print(self.main_rubric)

    def start_requests(self):
        if self.main_rubric != 'Остров':
            yield scrapy.Request(
                url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=attraction&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                    .format(self.count, self.main_rubric),
                cookies=self.cookies,
                callback=self.parse)
        else:
            yield scrapy.Request(
                url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=adm_div&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                    .format(self.count, self.main_rubric),
                cookies=self.cookies,
                callback=self.parse)

    def parse(self, response, **kwargs):
        scraped = json.loads(response.text)

        # try:
        #     real_rubric = []
        #     city = []
        #     company_name = []
        #     links = []
        #
        #     for i in scraped['result']['items']:
        #         try:
        #             real_rubric.append(i['point']['subtype_name'])
        #         except :
        #             try:
        #                 real_rubric.append(i['subtype_name'])
        #             except Exception as e:
        #                 print(e)
        #                 real_rubric.append('')
        #
        #         #real_rubric.append(self.main_rubric)
        #
        #         try:
        #             city.append(i['adm_div'][2]['name'])
        #         except Exception as e:
        #             print(e)
        #             city.append(' ')
        #
        #
        #         try:
        #             i['name'] = re.sub('"', '', i['name'])
        #             company_name.append(i['name'])
        #         except Exception as e:
        #             print(e)
        #             company_name.append('')
        #
        #
        #         try:
        #             links.append('https://2gis.ru/search/{}/firm/{}/'\
        #                         .format(i['adm_div'][2]['name'],
        #                                 i['id'][ : i['id'].find('_')])
        #                          )
        #         except Exception as e:
        #             print(e)
        #             links.append(self.count)
        #
        #     self.result.extend(list(zip(real_rubric, company_name, city, links)))
        #
        #
        #     self.count = self.count + 1
        #     print(len(self.result))
        #
        #     if self.main_rubric != 'Остров':
        #         yield scrapy.Request(
        #             url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=attraction&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
        #                 .format(self.count, self.main_rubric),
        #             cookies=self.cookies,
        #             callback=self.parse)
        #     else:
        #         yield scrapy.Request(
        #             url='https://catalog.api.2gis.ru/3.0/items?page={}&page_size=50&q=Приморский край {}&type=adm_div&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
        #                 .format(self.count, self.main_rubric),
        #             cookies=self.cookies,
        #             callback=self.parse)
        #
        # except Exception as e:
        #     print(e)
        #     return


    def close(self, reason):
        pass
        # connection = sqlite3.connect('db/{}.db'.format(self.category))
        # cursor = connection.cursor()
        #
        # cursor.execute('''CREATE TABLE IF NOT EXISTS gis
        #                (rubric TEXT, company TEXT, city TEXT, address TEXT,
        #                phone TEXT, url TEXT NOT NULL UNIQUE)''')
        # for item in self.result:
        #     try:
        #         cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?, ?)',
        #                        (item[0], item[1], item[2], '', '', item[3]))
        #     except:
        #         cursor.execute("SELECT rubric FROM gis WHERE URL=?", (item[3],))
        #         update_rubric = cursor.fetchone()
        #         if item[0] in update_rubric[0]:
        #             continue
        #         update_rubric = update_rubric[0] + '; ' + item[0]
        #         cursor.execute("UPDATE gis SET rubric=? WHERE url=?", (update_rubric, item[3]))
        # connection.commit()
        # connection.close()


class Companies_Phone(scrapy.Spider):
    name = 'gis.company'
    allowed_domains = ['2gis.ru', 'catalog.api.2gis.ru']

    def __init__(self, category, company_link):
        super(Companies_Phone, self).__init__(category)
        self.category = category
        self.company_link = company_link

    def start_requests(self):
        yield scrapy.Request(
            url=self.company_link, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        try:
            self.phone = response.css('._b0ke8 ._2lcm958::attr(href)').extract_first()[-7:]
        except:
            self.phone = 'None'

    def close(self, reason):
        connection = sqlite3.connect('db/{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute("UPDATE gis SET phone=? WHERE url=?", (self.phone, self.company_link))
        connection.commit()
        connection.close()


class Other_companies(scrapy.Spider):
    name = 'other_gis.company'
    allowed_domains = ['2gis.ru', 'catalog.api.2gis.ru']
    cookies = {
        '_2gis_webapi_user': 'a4a10f05-a5a8-4adb-a6e2-e8fcdca99017',
        '_ga': 'GA1.2.876134808.1652712422',
        '_gid': 'GA1.2.460225824.1653565934',
        '_ym_uid': '1652712423363044238',
        '_ym_d': '1652712423',
        '_2gis_webapi_session': '045a1a5e-59e7-4e12-b2ba-5772ebcdb38d',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, get_name, get_city, get_address, get_phone, from_db):
        super(Other_companies, self).__init__()
        self.get_name = get_name
        self.get_city = get_city
        self.get_address = get_address
        self.get_phone = get_phone
        self.from_db = from_db

        self.clear_addr = ['проспект', 'посёлок', 'улица', 'переулок', 'мыс', 'бульвар', 'площадь', 'бухта']
        self.result = list()
        self.answer = []

    def start_requests(self):
        yield scrapy.Request(
            url='https://catalog.api.2gis.ru/3.0/items?page=1&page_size=50&q=Приморский край {}&type=branch&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                .format(self.get_name),
                cookies=self.cookies,
                callback=self.parse)

    def parse(self, response, **kwargs):
        scraped = json.loads(response.text)
        try:
            address_name = list()
            for i in scraped['result']['items']:
                try:
                    address_name.append(i['address_name'].split(', '))
                except:
                    address_name.append([''])

            for i in range(len(address_name)):
                try:
                    address_name[i][-1] = address_name[i][-1].split()[0]
                except:
                    pass

                for j in self.clear_addr:
                    if j in address_name[i][0]:
                        address_name[i][0] = re.sub(j, '', address_name[i][0]).strip()

            address_name = [' '.join(i.lower() for i in row) for row in address_name]

            scraped_row = scraped['result']['items']
            for i in range(len(scraped_row)):
                if scraped_row[i]['adm_div'][1]['id'] == '1267655302447132':
                    campany_name = scraped_row[i]['name_ex']['primary']
                    if campany_name[0] == '#':
                        company_url_name = campany_name[1:]
                    elif campany_name[-2:] == '?!':
                        company_url_name = campany_name[:-2]
                    else:
                        company_url_name = campany_name

                    self.result.append([
                                        self.from_db,  #rubric
                                        campany_name,  #name
                                        scraped_row[i]['adm_div'][2]['name'],  #city
                                        address_name[i], #address
                                       'https://2gis.ru/search/{}/firm/{}/{},{}'
                                        .format(company_url_name,
                                                scraped_row[i]['id'][ : scraped_row[i]['id'].find('_')],
                                                scraped_row[i]['point']['lon'],
                                                scraped_row[i]['point']['lat'])   #link
                                       ])

            for i in self.result:
                try:
                    if i[3].split()[0] == self.get_address.split()[0]:
                        self.answer = [i[0], self.get_name, i[2], self.get_address, i[4]]
                        break
                except:
                    if i[3] == self.get_address:
                        self.answer = [i[0], self.get_name, i[2], self.get_address, i[4]]
                        break

        except KeyError:
            return

    def close(self, reason):
        print(self.answer)
        if self.answer != []:
            connection = sqlite3.connect('db/{}.db'.format('hotel'))
            cursor = connection.cursor()
            try:
                cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?, ?)',
                                    (self.answer[0], self.answer[1], self.answer[2], self.answer[3], self.get_phone, self.answer[4]))
            except:
                cursor.execute("UPDATE gis SET rubric=?, company=?, city=?, address=?, phone=? WHERE url=?",
                               (self.answer[0], self.answer[1], self.answer[2], self.answer[3], self.get_phone, self.answer[4]))

            connection.commit()
            connection.close()


class Other_attr_companies(scrapy.Spider):
    name = 'other_attr_gis.company'
    allowed_domains = ['2gis.ru', 'catalog.api.2gis.ru']
    cookies = {
        '_2gis_webapi_user': 'a4a10f05-a5a8-4adb-a6e2-e8fcdca99017',
        '_ga': 'GA1.2.876134808.1652712422',
        '_gid': 'GA1.2.460225824.1653565934',
        '_ym_uid': '1652712423363044238',
        '_ym_d': '1652712423',
        '_2gis_webapi_session': '045a1a5e-59e7-4e12-b2ba-5772ebcdb38d',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, get_name, get_city, from_db):
        super(Other_attr_companies, self).__init__()
        self.get_name = get_name
        self.get_city = get_city
        self.from_db = from_db
        self.result = list()
        self.answer = []

    def start_requests(self):
        yield scrapy.Request(
            url='https://catalog.api.2gis.ru/3.0/items?page=1&page_size=50&q=Приморский край {}&type=attraction&fields=items.point,items.locale,items.adm_div,items.city_alias,items.region_id,items.name_ex,items.org,items.address,items.rubrics&key=rurbbn3446&locale=ru_RU&search_device_type=desktop&search_user_hash=4049411583404697145&stat[sid]=161022b4-28d3-4561-9e70-7d17ed7d4fa2&stat[user]=9f0baf28-fe01-45f0-8ebb-0cf3b1c19948&shv=2022-07-14-16&r=1449317575'
                .format(self.get_name),
                cookies=self.cookies,
                callback=self.parse)

    def parse(self, response, **kwargs):
        scraped = json.loads(response.text)
        print('get_name=', self.get_name)

        try:
            real_rubric = []
            city = []
            company_name = []
            links = []

            for i in scraped['result']['items']:
                try:
                    real_rubric.append(i['point']['subtype_name'])
                except:
                    try:
                        real_rubric.append(i['subtype_name'])
                    except Exception as e:
                        print(e)
                        real_rubric.append('')

                try:
                    city.append(i['adm_div'][2]['name'])
                except Exception as e:
                    print(e)
                    city.append(' ')

                try:
                    i['name'] = re.sub('"', '', i['name'])
                    company_name.append(i['name'])
                except Exception as e:
                    print(e)
                    company_name.append('')

                try:
                    links.append('https://2gis.ru/search/{}/firm/{}/' \
                                 .format(i['adm_div'][2]['name'],
                                         i['id'][: i['id'].find('_')])
                                 )
                except Exception as e:
                    print(e)
                    links.append(self.get_name)

            self.result.extend(list(zip(real_rubric, company_name, city, links)))

            print(len(self.result))
            # for i in self.result:
            #     print(i)
            #
            if len(self.result) == 1:
                self.answer = [self.result[0][0], self.get_name, self.get_city, self.result[0][-1]]
            elif len(self.result) < 4:
                self.answer = [self.result[0][0], self.get_name, self.get_city, self.result[0][-1]]
            else:
                self.answer = []

            # for i in self.result:
            #     try:
            #         if i[3].split()[0] == self.get_address.split()[0]:
            #             self.answer = [i[0], self.get_name, i[2], self.get_address, i[4]]
            #             break
            #     except:
            #         if i[3] == self.get_address:
            #             self.answer = [i[0], self.get_name, i[2], self.get_address, i[4]]
            #             break


        except Exception as e:
            print(e)
            return

    def close(self, reason):
        pass
        print(self.answer)
        if self.answer != []:
            connection = sqlite3.connect('db/{}.db'.format('attr'))
            cursor = connection.cursor()
            try:
                cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?, ?)',
                                    (self.answer[0], self.answer[1], self.answer[2], '', '', self.answer[3]))
            except:
                cursor.execute("UPDATE gis SET rubric=?, company=?, city=?, address=?, phone=? WHERE url=?",
                                    (self.answer[0], self.answer[1], self.answer[2], '', '', self.answer[3]))

            connection.commit()
            connection.close()





class Reviews(scrapy.Spider):
    name = 'gis.review'
    allowed_domains = ['2gis.ru', 'public-api.reviews.2gis.com']
    cookies = {
        '_2gis_webapi_user': 'a28ff7d5-16f3-4322-9b1d-7adc3ab1f2c7',
        '_ga': 'GA1.2.79351340.1648281785',
        '_gid': 'GA1.2.931482717.1648281785',
        '_ym_uid': '1648281785478756357',
        '_ym_d': '1648281785',
        '_2gis_webapi_session': '7f93e759-5a26-4743-8ab1-e6bc50a4d1ae',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, category, row):
        super(Reviews, self).__init__(category)
        self.category = category
        self.company = row[0]
        self.company_link = row[1]
        self.reviews = list()

    def start_requests(self):
        yield scrapy.Request(
            url='https://public-api.reviews.2gis.com/2.0/branches/{}/reviews?limit=50&is_advertiser=true&fields=meta.providers,meta.branch_rating,meta.branch_reviews_count,meta.total_count,reviews.hiding_reason,reviews.is_verified&without_my_first_review=false&rated=true&sort_by=date_edited&key=37c04fe6-a560-4549-b459-02309cf643ad&locale=ru_RU'
               .format(self.company_link[
                                           self.company_link.find('firm/') + 5 :
                                           self.company_link.rfind('/')
                       ]),
            cookies=self.cookies, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            scraped = json.loads(response.text)

            self.reviews.extend([
                [i['text'], i['rating'] / 5,
                 i['date_created'][: i['date_created'].find('T')]]
                for i in scraped['reviews']])

            try:
                yield scrapy.Request(response.urljoin(scraped['meta']['next_link']),
                                         cookies=self.cookies,
                                         callback=self.parse)
            except:
                return

        except Exception as e:
            with open('logging/GIS.log', 'a') as err_log:
                err_log.write(self.company_link + '\n')
                err_log.write(traceback.format_exc() + '\n')


    def close(self, reason):
        pass
        connection = sqlite3.connect('db/reviews_{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS gis
                               (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.reviews:
            cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()



class Reviews_Attr(scrapy.Spider):
    name = 'gis.review.attr'
    allowed_domains = ['2gis.ru', 'public-api.reviews.2gis.com']
    cookies = {
        '_2gis_webapi_user': 'a28ff7d5-16f3-4322-9b1d-7adc3ab1f2c7',
        '_ga': 'GA1.2.79351340.1648281785',
        '_gid': 'GA1.2.931482717.1648281785',
        '_ym_uid': '1648281785478756357',
        '_ym_d': '1648281785',
        '_2gis_webapi_session': '7f93e759-5a26-4743-8ab1-e6bc50a4d1ae',
        '_ym_isad': 2,
        'captcha': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6Ijc3LjM1LjYzLjE1MSIsImF1ZCI6InBhcnNlci1kZXRlY3RvciIsImV4cCI6MTY3MDg1NTY4MiwiaXNzIjoiY2FwdGNoYSJ9.hkjzcDTeAYVur3RtcQaGWxDmVOl4hVQckSOwcXIPJlB6HdPKRiXrIgOxHiclnizqWGbSj86l9sXISpSTBB-YuQ'
    }

    def __init__(self, category, row):
        super(Reviews_Attr, self).__init__(category)
        self.category = category
        self.company = row[0]
        self.company_link = row[1]
        self.reviews = list()

    def start_requests(self):
        yield scrapy.Request(
            url='https://public-api.reviews.2gis.com/2.0/geo/{}/reviews?limit=50&fields=meta.providers,meta.geo_rating,meta.geo_reviews_count,meta.total_count,reviews.hiding_reason&sort_by=date_edited&without_my_first_review=false&key=37c04fe6-a560-4549-b459-02309cf643ad&locale=ru_RU'
                .format(self.company_link[
                        self.company_link.find('firm/') + 5:
                        self.company_link.rfind('/')
                        ]),
            cookies=self.cookies, callback=self.parse)


    def parse(self, response, **kwargs):
        scraped = json.loads(response.text)

        review_text = []
        rating = []
        date_created = []
        for item in scraped['reviews']:
            try:
                review_text.append(item['text'])
            except Exception as e:
                print(e)
                review_text.append('')

            try:
                rating.append(item['rating'] / 5)
            except Exception as e:
                print(e)
                rating.append('None')

            try:
                date_created.append(item['date_created'][: item['date_created'].find('T')])
            except Exception as e:
                print(e)
                date_created.append('None')

        self.reviews.extend(list(zip(review_text, rating, date_created)))

        try:
            yield scrapy.Request(response.urljoin(scraped['meta']['next_link']),
                                 cookies=self.cookies,
                                 callback=self.parse)
        except:
            return



    def close(self, reason):
        connection = sqlite3.connect('db/reviews_{}.db'.format(self.category))
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS gis
                                       (review TEXT, rating REAL, date TEXT, company TEXT, url TEXT)''')

        for item in self.reviews:
            cursor.execute('INSERT INTO gis VALUES (?, ?, ?, ?, ?)',
                           (item[0], item[1], item[2], self.company, self.company_link))
        connection.commit()
        connection.close()
