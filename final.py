import os
import re
import sys
import csv
import time
import json
import shutil
import logging
import datetime
import traceback
import unicodedata
import pandas as pd
import logging.handlers
from random import randint

from datetime import datetime
import pytz
from selenium import webdriver

# Driver imports
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


listing_data_dir = 'events_facebook.csv'

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
    # driver.get('https://www.facebook.com/search/events?q=jeep%20events%20usa')
    # time.sleep(randint(2,4))

    # scroll_down(driver)


    # events = driver.find_elements(By.XPATH,'//a[contains(@class,"x1s688f") and @role="presentation"]')
    # events = [event.get_attribute('href') for event in events]



    # with open("events.txt",'w') as txt:
    #     txt.write(str(events))

    import ast
    with open('events.txt', 'r') as f:
        events = f.read()
    events = ast.literal_eval(events)
   
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

        store_event_data(event_information)

def collect_event_information(driver, event):
    page_source = driver.page_source

    event_title = driver.find_element(By.XPATH, '//meta[@property="og:title"]').get_attribute('content')

    day_time_sentence = page_source[page_source.find('day_time_sentence') + 20:]
    day_time_sentence = day_time_sentence[:day_time_sentence.find('","event_place":')]
    day_time_sentence = day_time_sentence.replace('\\u2013','–')
    day_time_sentence = day_time_sentence.replace('\u2013','–')
    # day_time_sentence = unicodedata.normalize('NFKD', day_time_sentence).encode('ascii', 'ignore')

    startdate, starttime, enddate, endtime = clean_date_sourav_account(day_time_sentence)
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

    event_description = unicodedata.normalize('NFKD', event_description).encode('ascii', 'ignore')
        

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
        fieldnames = ['url', 'post_title', 'events_address', 'post_content', 'startdate', 'starttime', 'enddate', 'endtime']
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
        sys.exit(0)