import os
import re
import sys
import csv
import time
import json
import shutil
import urllib
import datetime
import traceback
import requests
import pandas as pd
from random import randint

from datetime import datetime, timedelta
import pytz

# Driver imports
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


listing_data_dir = 'events_facebook.csv'
today = datetime.now().strftime('%d-%b-%y') 
year = datetime.now().strftime('%y')
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%d-%b-%y')

def store_event_data(information):
    listings_data = pd.read_csv(listing_data_dir)
    listings_data = pd.concat([listings_data, pd.DataFrame.from_records([information])], ignore_index=True)
    listings_data.drop_duplicates(subset=['url'], keep='last', inplace=True)
    listings_data.to_csv(listing_data_dir, index=False)
    print(f"Stored listings data: {information}")

def login(driver):
    driver.get('https://www.facebook.com/login')
    time.sleep(randint(2,4))
    driver.find_element(By.ID,'email').send_keys('sdsourav158031@gmail.com')
    time.sleep(randint(2,4))
    driver.find_element(By.ID,'pass').send_keys('')
    time.sleep(randint(2,4))
    input('Press any key to continue...')
    driver.find_element(By.ID,'loginbutton').click()
    time.sleep(randint(10,20))

    input('Press any key to continue...')

    # 

def scroll_down(driver):
    ct = 0
    for i in range(3):
        SCROLL_PAUSE_TIME = 20
        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")
        counter = 0
        while True:
            counter+=1
            print("Counter:",counter)
            # Scroll down to bottom
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            events = driver.find_elements(By.XPATH,'//a[contains(@class,"x1s688f") and @role="presentation"]')
            check = events[-1].text.lower()
            if 'jeep' not in check:
                ct += 1
                if ct > 3:
                    break
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        time.sleep(20)

def clean(x):
    x = x.replace('EDT', '').replace('EST', '').strip()
    if 'at ' in x.lower() and len(x.split(' ')) == 9:
        st = x.split(' – ')[0].strip()
        et = x.split(' – ')[1].strip()

        st_d = st.split(' at ')[0].strip()
        startdate = datetime.strptime(st_d, '%d %b').strftime(f'%d-%b-{year}')
        starttime = st.split(' at ')[1].strip()

        et_d = et.split(' at ')[0].strip()
        enddate = datetime.strptime(et_d, '%d %b').strftime(f'%d-%b-{year}')
        endtime = et.split(' at ')[1].strip()

        return startdate, starttime, enddate, endtime
    elif "today" in x.lower() or "tomorrow" in x.lower():
        if 'from' in x:
            t = x.split('from ')[1].strip()
            if 'tomorrow' in x.lower():
                startdate = tomorrow
                enddate = tomorrow
            elif 'today' in x.lower():
                startdate = today
                enddate = today
            starttime = t.split('-')[0].strip()
            endtime = t.split('-')[1].strip()
            
        else:
            t = x.split('at ')[1].strip()
            startdate = ''
            if 'tomorrow' in x.lower():
                startdate = tomorrow
            elif 'today' in x.lower():
                startdate = today
            enddate = ''
            starttime = t
            endtime = ''

        return startdate, starttime, enddate, endtime
    elif 'at ' in x.lower() and len(x.split(' ')) == 3:
        t = x.split('at ')[1].strip()
        startdate = ''
        enddate = ''
        starttime = t
        endtime = ''

        return startdate, starttime, enddate, endtime


    elif 'at ' in x.lower() or 'from ' in x.lower() and len(x.split(' ')) == 6:
        if 'from' in x:
            d = x.split('from ')[0].strip()
            startdate = datetime.strptime(d, '%A, %d %B %Y').strftime('%d-%b-%y')
            enddate = startdate
            t = x.split('from ')[1].strip()
            starttime = t.split('-')[0].strip()
            endtime = t.split('-')[1].strip()
        else:
            d = x.split('at ')[0].strip()
            startdate = datetime.strptime(d, '%A, %d %B %Y').strftime('%d-%b-%y')
            enddate = ''
            t = x.split('at ')[1].strip()
            starttime = t
            endtime = ''

        return startdate, starttime, enddate, endtime   

def clean_date_sourav_account(x):
    x = x.split('UTC')[0].strip()
    if ' at ' in x.lower() and ' – ' not in x.lower():
        x_datetime = datetime.strptime(x, '%A, %d %B %Y at %H:%M')
        x_datetime_timezone = x_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        x_datetime_timezone_us = x_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        startdate = x_datetime_timezone_us.strftime('%d-%b-%y')
        starttime = x_datetime_timezone_us.strftime('%H:%M')

        enddate = ''
        endtime = ''

        return startdate, starttime, enddate, endtime

    elif ' from ' in x.lower():
        d = x.split(' from ')[0].strip()
        t = x.split(' from ')[1].strip()

        st = t.split('-')[0].strip()
        et = t.split('-')[1].strip()

        s_d_t = f'{d} {st}'
        e_d_t = f'{d} {et}'

        s_datetime = datetime.strptime(s_d_t, '%A, %d %B %Y %H:%M')
        s_datetime_timezone = s_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        s_datetime_timezone_us = s_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        startdate = s_datetime_timezone_us.strftime('%d-%b-%y')

        starttime = s_datetime_timezone_us.strftime('%H:%M')

        e_datetime = datetime.strptime(e_d_t, '%A, %d %B %Y %H:%M')
        e_datetime_timezone = e_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        e_datetime_timezone_us = e_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        enddate = e_datetime_timezone_us.strftime('%d-%b-%y')

        endtime = e_datetime_timezone_us.strftime('%H:%M')

        return startdate, starttime, enddate, endtime

    
    elif (len(x.split('UTC')[0].strip().split(' – ')[0].split(' ')) == 5) and 'AT ' in x.upper() and ' – ' in x:
        st = x.split(' – ')[0].strip()
        et = x.split(' – ')[1].strip()

        st_datetime = datetime.strptime(st, '%d %b %Y AT %H:%M')
        s_datetime_timezone = st_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        s_datetime_timezone_us = s_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        startdate = s_datetime_timezone_us.strftime('%d-%b-%y')
        starttime = s_datetime_timezone_us.strftime('%H:%M')

        et_datetime = datetime.strptime(et, '%d %b %Y AT %H:%M')
        e_datetime_timezone = et_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        e_datetime_timezone_us = e_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        enddate = e_datetime_timezone_us.strftime('%d-%b-%y')
        endtime = e_datetime_timezone_us.strftime('%H:%M')

        return startdate, starttime, enddate, endtime

    elif 'AT ' in x.upper() and ' – ' in x.upper():
        dt_n = datetime.now().strftime('%y')

        st = x.split(' – ')[0].strip() + f' {dt_n}'
        et = x.split(' – ')[1].strip() + f' {dt_n}'

        st_datetime = datetime.strptime(st, '%d %b at %H:%M %y')
        s_datetime_timezone = st_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        s_datetime_timezone_us = s_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        startdate = s_datetime_timezone_us.strftime('%d-%b-%y')
        starttime = s_datetime_timezone_us.strftime('%H:%M')

        et_datetime = datetime.strptime(et, '%d %b at %H:%M %y')
        e_datetime_timezone = et_datetime.astimezone(pytz.timezone('Asia/Dhaka'))
        e_datetime_timezone_us = e_datetime_timezone.astimezone(pytz.timezone('US/Eastern'))

        enddate = e_datetime_timezone_us.strftime('%d-%b-%y')
        endtime = e_datetime_timezone_us.strftime('%H:%M')

        return startdate, starttime, enddate, endtime

def process(driver):
    driver.get('https://www.facebook.com/search/events?q=jeep%20events%20usa')
    time.sleep(randint(2,4))

    scroll_down(driver)


    events = driver.find_elements(By.XPATH,'//a[contains(@class,"x1s688f") and @role="presentation"]')
    events = [event.get_attribute('href') for event in events]



    # with open("events.txt",'w') as txt:
    #     txt.write(str(events))

    # import ast
    # with open('events.txt', 'r') as f:
    #     events = f.read()
    # events = ast.literal_eval(events)
   
    print(f'Total events: {len(events)}')

    for idx, event in enumerate(events):

        print(f'Processing event --- {idx+1}/{len(events)} --- {event}')
        driver.get(f'{event}')
        time.sleep(20)

        event_title = driver.find_element(By.XPATH, '//meta[@property="og:title"]').get_attribute('content')

        if 'j.e.e.p' not in event_title.lower():
            if 'jeep' not in event_title.lower():
                print('Skipping this event')
                continue

        event_information = collect_event_information(driver, event)

        if event_information['events_address'] == None:
            print('Skipping this event')
            continue

        data = event_information['events_address']

        if collect_zip_city_state(data) is not None:
            zipcode, city, state, country = collect_zip_city_state(data)
            d = {'zipcode': zipcode, 'city': city, 'state': state, 'country': country}
            event_information.update(d)
        else:
            print('Skipping this event')
            continue

        store_event_data(event_information)

def clean_string(string):
    return string.replace('\\n','\n').replace('\\ud83d\\udd3a','').replace('\\u2019',"'").replace("\'","'").replace('\\/', '/')\
        .replace('\\"', '"').replace('\\u0040','@')

def collect_zip_city_state(data):
    address= urllib.parse.quote(data)
    URL=f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&sensor=true&key=AIzaSyCl9NUj6BYu-5CjPIQv3YbOeWOSGgmZQxY"
    response=requests.get(URL)
    json_object=json.loads(response.text)
    if len(json_object['results']) == 1:
        zipcode_found=False
        city_found=False
        state_found=False
        country_found=False

        zipcode=""
        for item in json_object['results'][0]['address_components']:
            if 'postal_code' in item['types']:
                zipcode_found=True
                zipcode=item['long_name']
                break
        city=""
        for item in json_object['results'][0]['address_components']:
            if 'locality' in item['types']:
                city_found=True
                city=item['long_name']
                break
        state=""
        for item in json_object['results'][0]['address_components']:
            if 'administrative_area_level_1' in item['types']:
                state_found=True
                state=item['long_name']
                break
        country=""
        for item in json_object['results'][0]['address_components']:
            if 'country' in item['types']:
                country_found=True
                country=item['long_name']
                break
        if zipcode_found and city_found and state_found and country_found:
            return zipcode, city, state, country
        else:
            return None

def collect_event_information(driver, event):
    page_source = driver.page_source

    event_title = driver.find_element(By.XPATH, '//meta[@property="og:title"]').get_attribute('content')

    day_time_sentence = page_source[page_source.find('day_time_sentence') + 20:]
    day_time_sentence = day_time_sentence[:day_time_sentence.find('","event_place":')]
    day_time_sentence = day_time_sentence.replace('\\u2013','–')
    day_time_sentence = day_time_sentence.replace('\u2013','–')

    # startdate, starttime, enddate, endtime = clean_date_sourav_account(day_time_sentence)
    startdate, starttime, enddate, endtime = clean(day_time_sentence)
    try:
        if int(startdate.split('-')[0]) > int(enddate.split('-')[0]):
            enddate = startdate
    except:
        pass
        
    try:
        location = page_source[[m.start() for m in re.finditer(r'address', page_source)][-1]:]
        location = location[:location.find(',"featurable_title"')]
        street = location[location.find("street")+9:location.find('"},"city"')]
        city = location[location.find('"contextual_name":"')+19:location.find('","id')]
        for w in street.split(' '):
            if w in city:
                street = street.replace(w, '')
                street = street.replace(',,', '').strip()
        event_location = street + ', ' + city
        event_location = event_location.replace(',,', ',').strip().lstrip(', ')

        if len(event_location) > 1000:
            print('Location is too long')
            location = page_source[[m.start() for m in re.finditer(r'address', page_source)][-2]:]
            location = location[:location.find(',"featurable_title"')]
            street = location[location.find("street")+9:location.find('"},"city"')]
            city = location[location.find('"contextual_name":"')+19:location.find('","id')]
            for w in street.split(' '):
                if w in city:
                    street = street.replace(w, '')
                    street = street.replace(',,', '').strip()
            event_location = street + ', ' + city
            event_location = event_location.replace(',,', ',').strip()

        if len(event_location) > 1000: 
            print('###### Location is too long ##########')
            location = page_source[page_source.find('event_place":{"__typename":"Page","name":"')+42:]
            location = page_source[page_source.find(',"contextual_name":"')+20:]
            event_location = location[:location.find('",')]
            if len(event_location) > 1000:
                with open('long_location.txt','a') as txt:
                    txt.write(event + '\n,')
                event_location = None
    except Exception as e:
        print('__isPlace":"FreeformPlace"')
        location = page_source[page_source.find('__isPlace":"FreeformPlace","contextual_name":"')+45:]
        location = location[:location.find('","')]
        event_location = location.replace('"', '')

        if len(event_location) > 1000:
            location = page_source[page_source.find('event_place":{"__typename":"Page","name":"')+42:]
            location = page_source[page_source.find(',"contextual_name":"')+20:]
            event_location = location[:location.find('",')]

        if len(event_location) > 1000: 
            print('###### Location is too long ##########')
            with open('long_location.txt','a') as txt:
                txt.write(event + '\n')
            event_location = None
    try:
        if 'nul' in event_location:
            event_location = None
    except:
        pass

    event_description = page_source[page_source.find('event_description":{"text":"')+28:]
    event_description = event_description[:event_description.find(',"delight_ranges"')]
    event_description = clean_string(event_description)
        

    d = {
        'url': event,
        'post_title': event_title,
        'events_address': event_location,
        'post_content': event_description,
        'event_datetime': day_time_sentence,
        'startdate': startdate,
        'starttime': starttime,
        'enddate': enddate,
        'endtime': endtime
    }

    return d

def is_home_page(driver) -> bool:
    driver.get('https://www.facebook.com/')
    time.sleep(5)
    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@aria-label="Create a post"]')))
        print('We are in home page')
        return True
    except Exception as e:
        print("We are in login page.")
        return False

def profile_saving(profile_path, profile_backup_path):
    try:
        shutil.rmtree(profile_path, ignore_errors=True)
        print(f'{profile_path} DELETED!!! #1')                              
    except Exception as e:
        print(f'{profile_path} Permisssion ERROR!!! #1')
        print(f'{e}')
    try:
        shutil.copytree(profile_backup_path, profile_path)
        print(f'Backup folder copied. #1')
    except:
        print('Failed to copy the backup folder. #1')
        try:
            shutil.rmtree(profile_path, ignore_errors=True)
            print(f'{profile_path} DELETED!!! #2')                              
        except Exception as e:
            print(f'{profile_path} Permisssion ERROR!!! #2')
            print(f'{e}')

        try:
            shutil.copytree(profile_backup_path, profile_path)
            print(f'Backup folder copied. #2')
        except Exception as e:
            print('Failed to copy the backup folder. #2')
            print(f'{e}')

def create_csvfile():
    with open('events.csv', 'w', newline='') as csvfile:
        # fieldnames = ['url', 'post_title', 'events_address', 'post_content', 'event_datetime']
        fieldnames = ['url', 'post_title', 'events_address', 'post_content', 'startdate', 'starttime', 'enddate', 'endtime', 'zipcode', 'city', 'state', 'country', 'event_datetime']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

# def store_event_data(event_information):
#     with open('events.csv', 'a', newline='') as csvfile:
#         fieldnames = ['url', 'post_title', 'events_address', 'post_content', 'event_datetime']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writerow(event_information)

def globaltracebackdata():
        exType, exValue, exTraceback = sys.exc_info()

        trace_back = traceback.extract_tb(exTraceback)

        stackTrace = list()

        for trace in trace_back:
            stackTrace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

        msg=""
        msg+='\n-- START DETAILED ERROR MSG --'
        msg+="\nException type : %s " % exType.__name__
        msg+="\nException message : %s" % exValue
        msg+="\nStack trace : %s" % stackTrace
        msg+='\n-- END DETAILED ERROR MSG --'

        print(msg)

        input("PAUSED")

        return msg

if __name__ == '__main__':
    PROXY = "46.4.73.88:2000"
    listing_data_dir = 'events.csv'
    profile_path = os.path.join(os.getcwd(), 'Profile')
    profile_backup_path = os.path.join(os.getcwd(), 'ProfileBK')

    profile_saving(profile_path, profile_backup_path)

    options = uc.ChromeOptions()
    # options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f'--user-data-dir={profile_path}')
    options.add_argument('--no-first-run --no-service-autorun --password-store=basic')

    # options.add_argument(f'--proxy-server={PROXY}')
    # options.add_argument("--incognito")
    # options.binary="C:\\Program Files\\BraveSoftware\\Brave-Browser\\Application\\brave.exe"

    driver = uc.Chrome(options=options)

    if not os.path.exists(listing_data_dir):
        create_csvfile()
    try:
        ret = is_home_page(driver)
        if not ret:
            login(driver)
        else:
            process(driver)
    except Exception as e:
        print(f'Error: {e}')
        globaltracebackdata()
        driver.quit()
    finally:
        # os.system('python cleaning_facebook_sourav.py')
        os.system('python addresses.py')
        sys.exit(0)