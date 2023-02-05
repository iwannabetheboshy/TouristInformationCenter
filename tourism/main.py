from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from tourism.VL.spiders import VL_Spider
from tourism.TripAdvisor.spiders import TA_Eat_Spider, TA_Hotel_Spider, TA_Fun_Spider, TA_Attr_Spider, TA_Culture_Spider
from tourism.TwoGis.spiders import GIS_Spider
import sqlite3
import re
from collections import Counter


configure_logging()

vl_runner_companies = CrawlerRunner(settings={'DOWNLOAD_DELAY': 8})
vl_runner_comments = CrawlerRunner(settings={'DOWNLOAD_DELAY': 0})
gis_runner = CrawlerRunner(settings={'DOWNLOAD_DELAY': 0})
ta_runner = CrawlerRunner(settings={'DOWNLOAD_DELAY': 4})


with open('settings/rubrics_refactor.json', 'r', encoding='utf-8') as out_flow:  
    categories = json.load(out_flow)

with open('settings/refactoring_rubric.json', 'r', encoding='utf-8') as out_flow:  
    refactoring_rubric = json.load(out_flow)

with open('settings/city_refactor.json', 'r', encoding='utf-8') as out_flow:  
    refactoring_city = json.load(out_flow)
    
with open('settings/all_rubrics.json', 'r', encoding='utf-8') as out_flow:  
    all_rubrics_gis = json.load(out_flow)
    

# -----------------------------
# Parsing companies and filling the database
# -----------------------------
@defer.inlineCallbacks
def crawl_companies():
    for category in categories.keys():
         if category != 'attr':
             for i in categories[category]['gis']:
                 yield gis_runner.crawl(GIS_Spider.Companies, category, i)
         else:
             for i in categories[category]['gis']:
                 yield gis_runner.crawl(GIS_Spider.Companies_Attr, category, i)
                 break

         for i in categories[category]['vl']:
           yield vl_runner_companies.crawl(VL_Spider.Companies, category, i)

         if category != 'attr':
             for i in categories[category]['trip']:
                 yield gis_runner.crawl(TA_Culture_Spider.Companies, category, i.lower())
         else:
             for i in categories[category]['trip']:
                 yield gis_runner.crawl(TA_Fun_Spider.Fun_Companies, category)


         if category == 'eat':
             for key in categories[category]['trip'].keys():
                 for value in categories[category]['trip'][key]:
                     yield gis_runner.crawl(TA_Eat_Spider.Companies, category, key, value)
         elif category == 'hotel':
             for i in categories[category]['trip']:
                 yield gis_runner.crawl(TA_Hotel_Spider.Companies)
             connection = sqlite3.connect('db/{}.db'.format(category))
             cursor = connection.cursor()
             cursor.execute("SELECT url FROM trip")
             for i in cursor.fetchall():
                 yield gis_runner.crawl(TA_Hotel_Spider.Companies_Phone, category, i[0])
             connection.close()

         -----------------------------
        # START Refactoring rubrics from GIS table
        # -----------------------------
        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        cursor.execute("SELECT rubric, url FROM gis")
        refactoring_rubrics_gis = [[i[0].split('; '), i[1]] for i in cursor.fetchall()]
        
        for i in range(len(refactoring_rubrics_gis)):
            match_rub = []
            for j in range(len(refactoring_rubrics_gis[i][0])):
                for item_ref in all_rubrics_gis[category]:
                    if refactoring_rubrics_gis[i][0][j] == item_ref:
                        match_rub.append(item_ref)
            refactoring_rubrics_gis[i][0] = '; '.join(match_rub)
        
        for item in refactoring_rubrics_gis:
            cursor.execute("UPDATE gis SET rubric=? WHERE url=?", (item[0], item[1]))
        connection.commit()
         
        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        cursor.execute("SELECT rubric, url FROM gis")
        refactoring_rubrics_gis = [[i[0].split('; '), i[1]] for i in cursor.fetchall()]
        
        for i in range(len(refactoring_rubrics_gis)):
            for key, value in refactoring_rubric[category]['change'].items():
                for value_item in value:
                    for j in range(len(refactoring_rubrics_gis[i][0])):
                        if value_item == refactoring_rubrics_gis[i][0][j]:
                            refactoring_rubrics_gis[i][0][j] = key
            clear_rub_item = '; '.join(list(set(refactoring_rubrics_gis[i][0])))
            cursor.execute("UPDATE gis SET rubric=? WHERE url=?",
                           (clear_rub_item, refactoring_rubrics_gis[i][1]))
        connection.commit()
        connection.close()
        # -----------------------------
        # END Refactoring rubrics from GIS table
        # -----------------------------

        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        cursor.execute("SELECT rubric FROM trip")

        # -----------------------------
        # BEGIN Refactoring Rubric from VL/TRIP table
        # -----------------------------
        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        for key, value in refactoring_rubric[category]['change'].items():
            for i in value:
                cursor.execute("UPDATE vl SET rubric=? WHERE rubric=?", (key, i))
                cursor.execute("UPDATE trip SET rubric=? WHERE rubric=?", (key, i))
        connection.commit()
        
        cursor.execute("SELECT rubric, url FROM vl")
        for item in [list(i) for i in cursor.fetchall()]:
            is_match_clear = False
            for i in refactoring_rubric[category]['change'].keys():
                if item[0] == i:
                    is_match_clear = True
                    break
            if is_match_clear == False:
                cursor.execute("DELETE FROM vl WHERE url = ?", (item[1],))
        connection.commit()
        
        cursor.execute("SELECT rubric, url FROM trip")
        for item in [list(i) for i in cursor.fetchall()]:
            is_match_clear = False
            for i in refactoring_rubric[category]['change'].keys():
                if item[0] == i:
                    is_match_clear = True
                    break
            if is_match_clear == False:
                cursor.execute("DELETE FROM trip WHERE url = ?", (item[1],))
        connection.commit()
        # -----------------------------
        # END Refactoring rubrics from VL/TRIP table
        # -----------------------------

        # -----------------------------
        # BEGIN Phones Parsing
        # -----------------------------
        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        cursor.execute("SELECT url FROM gis")
        for url in cursor.fetchall():
            yield gis_runner.crawl(GIS_Spider.Companies_Phone, category, url[0])
        
        cursor.execute("SELECT url FROM vl")
        for url in cursor.fetchall():
            yield vl_runner_comments.crawl(VL_Spider.Companies_Phone, category, url[0])
        
        cursor.execute("SELECT url FROM trip")
        for url in cursor.fetchall():
            if category == 'eat':
                yield gis_runner.crawl(TA_Eat_Spider.Companies_Phone, category, url[0])
            elif category == 'hotel':
                yield gis_runner.crawl(TA_Hotel_Spider.Companies_Phone, category, url[0])
            else:
                yield gis_runner.crawl(TA_Culture_Spider.Companies_Phone, category, url[0])
        connection.commit()
        connection.close()
        # -----------------------------
        # END Phones Parsing
        # -----------------------------

        refactoring_city_db(category)
        refactoring_rus_letter_db(category)

        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()
        cursor.execute("SELECT city FROM trip")
        
# -----------------------------
# Refactoring City
# -----------------------------
def refactoring_city_db(category):
    connection = sqlite3.connect('db/{}.db'.format(category))
    cursor = connection.cursor()

    cursor.execute("SELECT city, url FROM vl")
    city_vl = [list(i) for i in cursor.fetchall()]

    for i in range(len(city_vl)):
        for processed_city, NON_processed_city in refactoring_city.items():
            if city_vl[i][0] in NON_processed_city:
                city_vl[i][0] = processed_city
                cursor.execute("UPDATE vl SET city=? WHERE url=?", (processed_city, city_vl[i][1]))
                break
    connection.commit()

    try:
        cursor.execute("SELECT city, url FROM trip")
        city_trip = [list(i) for i in cursor.fetchall()]

        for i in range(len(city_trip)):
            for processed_city, NON_processed_city in refactoring_city.items():
                if city_trip[i][0] in NON_processed_city:
                    city_trip[i][0] = processed_city
                    cursor.execute("UPDATE trip SET city=? WHERE url=?", (processed_city, city_trip[i][1]))
                    break
        connection.commit()
        connection.close()
    except:
        print('The database does not have a table "TRIP"')

# -----------------------------
# Refactoring the letter "ё"
# -----------------------------
def refactoring_rus_letter_db(category):
    connection = sqlite3.connect('db/{}.db'.format(category))
    cursor = connection.cursor()

    cursor.execute("SELECT address, url FROM gis")
    for item in [list(i) for i in cursor.fetchall()]:
        if 'ё' in item[0]:
            item[0] = item[0].replace('ё', 'е')
            cursor.execute("UPDATE gis SET address=? WHERE url=?", (item[0], item[1]))

    cursor.execute("SELECT address, url FROM vl")
    for item in [list(i) for i in cursor.fetchall()]:
        if 'ё' in item[0]:
            item[0] = item[0].replace('ё', 'е')
            cursor.execute("UPDATE vl SET address=? WHERE url=?", (item[0], item[1]))
    try:
        cursor.execute("SELECT address, url FROM trip")
        for item in [list(i) for i in cursor.fetchall()]:
            if 'ё' in item[0]:
                item[0] = item[0].replace('ё', 'е')
                cursor.execute("UPDATE trip SET address=? WHERE url=?", (item[0], item[1]))
        connection.commit()
        connection.close()
    except:
        print('The database does not have a table "TRIP"')



@defer.inlineCallbacks
def refactoring_rubric_db():
    category = 'hotel'

    connection = sqlite3.connect('db/{}.db'.format(category))
    cursor = connection.cursor()

    cursor.execute("SELECT company, phone, city, address, rubric FROM gis")
    data_gis = sorted([[re.sub('[^а-яА-Яa-zA-Z0-9]', '', i[0]).lower(), i[1], i[2], i[3].strip(), i[4], '', i[0]]
                   for i in cursor.fetchall()])

    cursor.execute("SELECT company, phone, city, address, url FROM vl")
    data_vl = sorted([[re.sub('[^а-яА-Яa-zA-Z0-9]', '', i[0]).lower(), i[1], i[2], i[3].strip(), '', i[4], i[0]]
                   for i in cursor.fetchall()])

    cursor.execute("SELECT company, phone, city, address, url FROM trip")
    data_trip = sorted([[re.sub('[^а-яА-Яa-zA-Z0-9]', '', i[0]).lower(), i[1], i[2], i[3].strip(), '', i[4], i[0]]
                         for i in cursor.fetchall()])

    data = data_trip

    result_refacroting_rubric = []
    # -----------------------------
    # Refactoring Rubric
    # Step 1 - search for a full match of company names and their cities
    # -----------------------------
    def step_1(data_not_gis):
        GET_remainder_after_step_1 = []
        for item in data_not_gis:
            isMatch = False
            for item_gis in data_gis:
                if (item[0] == item_gis[0]) and (item[2] == item_gis[2]):
                    result_refacroting_rubric.append([item_gis[4], item_gis[-1], item_gis[2], item_gis[3], item_gis[1], item[-2]])
                    isMatch = True
                    break
            if isMatch == False:
                GET_remainder_after_step_1.append(item)

        print('----------step 1-------------')
        print('Search for a full match of company names and their cities')
        print('result=', len(result_refacroting_rubric))
        print('reminder=', len(GET_remainder_after_step_1))
        print()
        return GET_remainder_after_step_1

    remainder_after_step_1 = step_1(data)

    # -----------------------------
    # Refactoring Rubric
    # Step 2 - search for a full match of cities, address, as well as search for a partial match of company names (in)
    # -----------------------------
    def step_2(GET_remainder_after_step_1):
        GET_remainder_after_step_2 = []
        for item in GET_remainder_after_step_1:
            isMatch = False
            for item_gis in data_gis:
                if (item[3] == item_gis[3]) and (item[2] == item_gis[2]) and ((item[0] in item_gis[0]) or (item_gis[0] in item[0])):
                    result_refacroting_rubric.append([item_gis[4], item_gis[-1], item_gis[2], item_gis[3], item_gis[1], item[-2]])
                    isMatch = True
                    break
            if isMatch == False:
                GET_remainder_after_step_2.append(item)

        print('----------step 2-------------')
        print('Search for a full match of cities, address, as well as search for a partial match of company names')
        print('result=', len(result_refacroting_rubric))
        print('reminder=', len(GET_remainder_after_step_2))
        print()
        return GET_remainder_after_step_2

    remainder_after_step_2 = step_2(remainder_after_step_1)

    # -----------------------------
    # Refactoring Rubric
    # Step 3 - search for a complete match of city, phone numbers and addresses
    # -----------------------------
    def step_3(GET_remainder_after_step_2):
        GET_remainder_after_step_3=[]
        for item_vl in GET_remainder_after_step_2:
            isMatch = False
            for item_gis in data_gis:
                if (item_vl[2] == item_gis[2]) and (item_vl[1] == item_gis[1]) and (item_vl[3] == item_gis[3]):
                    result_refacroting_rubric.append([item_gis[4], item_gis[-1], item_gis[2], item_gis[3], item_gis[1], item_vl[-2]])
                    isMatch = True
                    break
            if isMatch == False:
                GET_remainder_after_step_3.append(item_vl)

        print('----------step 3-------------')
        print('Search for a complete match of company names, their phone numbers and addresses')
        print('result=', len(result_refacroting_rubric))
        print('reminder=', len(GET_remainder_after_step_3))
        print()
        return GET_remainder_after_step_3

    remainder_after_step_3 = step_3(remainder_after_step_2)

    # -----------------------------
    # Refactoring Rubric
    # Step 4 - search for a full match of addresses and partial match of company names
    # -----------------------------
    def step_4(GET_remainder_after_step_3):
        GET_remainder_after_step_4=[]
        for item in GET_remainder_after_step_3:
            isMatch = False
            for item_gis in data_gis:
                if (item[3] == item_gis[3]) and ((item[0] in item_gis[0]) or (item_gis[0] in item[0])):
                    result_refacroting_rubric.append([item_gis[4], item_gis[-1], item_gis[2], item_gis[3], item_gis[1], item[-2]])
                    isMatch = True
                    break
            if isMatch == False:
                GET_remainder_after_step_4.append(item)

        print('----------step 4-------------')
        print('Search for a full match of addresses and partial match of company names')
        print('result=', len(result_refacroting_rubric))
        print('reminder=', len(GET_remainder_after_step_4))
        print()
        return GET_remainder_after_step_4

    remainder_after_step_4 = step_4(remainder_after_step_3)

    # -----------------------------
    # Refactoring Rubric
    # Step 5 - update VL.db and TRIP.db
    # -----------------------------
    for result_item in result_refacroting_rubric:
        cursor.execute("UPDATE vl SET rubric=?, company=?, city=?, address=?, phone=? WHERE url=?",
                               (result_item[0], result_item[1], result_item[2], result_item[3], result_item[4], result_item[5]))
    connection.commit()

    for result_item in result_refacroting_rubric:
        cursor.execute("UPDATE trip SET rubric=?, company=?, city=?, address=?, phone=? WHERE url=?",
                               (result_item[0], result_item[1], result_item[2], result_item[3], result_item[4], result_item[5]))
    connection.commit()

    # -----------------------------
    # Refactoring Rubric
    # Step 6 - search new 2GIS items (Insert and Update)
    # -----------------------------
    print()
    print('----------step 6-------------')
    print('Search new 2GIS items (Insert and Update)...')
    print()

    from_db = 'fromTRIP'
    if category != 'attr':
        for item in remainder_after_step_4:
            yield gis_runner.crawl(GIS_Spider.Other_companies, item[-1], item[2], item[3], item[1], from_db) #name, city, address, phone
    else:
        for item in remainder_after_step_4:
            yield gis_runner.crawl(GIS_Spider.Other_attr_companies, item[-1], item[2], from_db) #name, city, address, phone

    cursor.execute("SELECT company, address, rubric, url FROM gis")
    data_gis = [i for i in cursor.fetchall()]

    cursor.execute("SELECT company, address, rubric, url FROM vl")
    data_vl = [i for i in cursor.fetchall()]

    cursor.execute("SELECT company, address, rubric, url FROM trip")
    data_trip = [i for i in cursor.fetchall()]

    data = data_trip

    def last_step(data, rubric_name):
        cursor.execute("SELECT company, address, rubric, url FROM gis")
        data_gis = sorted([i for i in cursor.fetchall()])
        for item_gis in data_gis:
            for item in data:
                if (item[0] == item_gis[0]) and (item_gis[2] == rubric_name):
                    cursor.execute("UPDATE gis SET rubric=? WHERE url=?", (item[-2], item_gis[-1]))
                    break
        connection.commit()
    refactoring_rus_letter_db(category)
    last_step(data, 'fromTRIP')


@defer.inlineCallbacks
def crawl_reviews():
    for category in categories.keys():
        connection = sqlite3.connect('db/{}.db'.format(category))
        cursor = connection.cursor()

        if category != 'attr':
            cursor.execute("SELECT company, url FROM gis")
            gis_companies = cursor.fetchall()
            for row in gis_companies:
                yield gis_runner.crawl(GIS_Spider.Reviews, category, row)
        else:
            cursor.execute("SELECT company, url FROM gis")
            gis_companies = cursor.fetchall()
            for row in gis_companies:
                yield gis_runner.crawl(GIS_Spider.Reviews_Attr, category, row)

        cursor.execute("SELECT company, url FROM vl")
        vl_companies = cursor.fetchall()
        for row in vl_companies:
            yield vl_runner_comments.crawl(VL_Spider.Reviews, category, row)

        cursor.execute("SELECT company, url FROM trip")
        trip_companies = cursor.fetchall()
        for row in trip_companies:
            yield gis_runner.crawl(TA_Culture_Spider.Reviews, category, row)

        connection.close()

refactoring_rubric_db()
crawl_companies()
crawl_reviews()
reactor.run()