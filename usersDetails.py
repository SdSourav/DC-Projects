#VERSION 04.09.2021 10:52PM
# Title

# --- Imports ---

# Webdriver Imports
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.options import Options

# String management imports
import re
# from bs4 import BeautifulSoup as bs

# Default Imports
import os
import json
import sys
import traceback
from os import path
import datetime
from datetime import datetime as dt
from time import sleep
from random import random
from random import randint
import time
import shutil
from csv import writer
import csv
import urllib.parse
import smtplib
import upwork
from upwork.routers import *
import pyodbc
import requests
#encryption imports


from base64 import b64encode
from base64 import b64decode
import binascii

# threading imports
import threading 
import concurrent.futures
# --- Global Function ---
# sys.path.append('C:\\DataProcessing\\Python')
from ErrorEmailsUsers import sendErrorEmail

def connectToDatabaseThread(configDict):

	if configDict['Trusted_Connection'].strip().lower() == "true":
		db_conn = pyodbc.connect('Driver={SQL Server};'
					'Server='+configDict['server']+';'
					'Database='+configDict['db_name']+';'
					'Trusted_Connection=yes;',autocommit=True)
	else:
		db_conn = pyodbc.connect('Driver={SQL Server};'
					'Server='+configDict['server']+';'
					'Database='+configDict['db_name']+';'
					'UID='+configDict['db_username']+';'
					'PWD='+configDict['db_password']+';',autocommit=True)
	return db_conn, db_conn.cursor()

def connectToDatabase(configDict):

	if configDict['Trusted_Connection'].strip().lower() == "true":
		db_conn = pyodbc.connect('Driver={SQL Server};'
					'Server='+configDict['server']+';'
					'Database='+configDict['db_name']+';'
					'Trusted_Connection=yes;',autocommit=True)
	else:
		db_conn = pyodbc.connect('Driver={SQL Server};'
					'Server='+configDict['server']+';'
					'Database='+configDict['db_name']+';'
					'UID='+configDict['db_username']+';'
					'PWD='+configDict['db_password']+';',autocommit=True)
	globals()['db_conn'] = db_conn
	globals()['db_cursor'] = db_conn.cursor()
	return db_conn, db_conn.cursor()

def executeSQLquery(query):
	global db_cursor
	counter=0
	while True:
		try:
			db_cursor.execute(query)
			return db_cursor
		except Exception as e:
			if "Violation of PRIMARY KEY constraint" in str(e):
				console.Log("Duplicate Found, ignoring ..")
				break
			else:
				if "Connection is busy with results for another hstmt" in str(e) or 'was deadlocked on lock resources' in str(e):
					counter+=1
					if counter == 15:
						print("Database connection in use for 15 seconds.., aborting query execution.")
						console.Log("Database in use for 15 seconds.., aborting query execution.")
						return
					print("Database connection in use,sleeping for 1 sec then resubmitting results to database...")
					console.Log("Database connection in use,sleeping for 1 sec then resubmitting results to database...")
					sleep(1)
				else:
					print("Something went wrong during data insertion: "+str(e))
					console.Log("Something went wrong during data insertion: "+str(e))
					console.Log(query)
					sendErrorEmail(console,"SQL query errored in usersDetails guy. ERROR:"+str(e))
					sys.exit(0)
					break

def executeSQLqueryThread(query, db_cursor):
	counter=0
	while True:
		try:
			db_cursor.execute(query)
			return db_cursor
		except Exception as e:
			if "Violation of PRIMARY KEY constraint" in str(e):
				console.Log("Duplicate Found, ignoring ..")
				break
			else:
				if "Connection is busy with results for another hstmt" in str(e) or 'was deadlocked on lock resources' in str(e):
					counter+=1
					if counter == 15:
						print("Database connection in use for 15 seconds.., aborting query execution.")
						console.Log("Database in use for 15 seconds.., aborting query execution.")
						return
					print("Database connection in use,sleeping for 1 sec then resubmitting results to database...")
					console.Log("Database connection in use,sleeping for 1 sec then resubmitting results to database...")
					sleep(1)
				else:
					print("Something went wrong during data insertion: "+str(e))
					console.Log("Something went wrong during data insertion: "+str(e))
					console.Log(query)
					sendErrorEmail(console,"SQL query errored in usersDetails guy. ERROR:"+str(e))
					sys.exit(0)
					break

def GLOBALEXIT():
	if globals()['configDict']['autoRun'] == "false":
		input("press ENTER to exit")
	# Close all windows
	sys.exit()

def GLOBALTRACEBACKDATA(console):

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

	console.Log('-- START DETAILED ERROR MSG --')
	console.Log("Exception type : %s " % exType.__name__)
	console.Log("Exception message : %s" % exValue)
	console.Log("Stack trace : %s" % stackTrace)
	console.Log('-- END DETAILED ERROR MSG --')

	sendErrorEmail(console,msg)
	return

# --- Functions ---
def ReadConfig(show=False):
	'''
	Return a dictionary of config settings:
	Keys: [username, password, outputFilename, lastRunStartDate, reprocessInterval, profileType]
	'''

	# Check if config file exists
	configFilename = 'config.txt'
	configFileString = ''
	if(path.exists(configFilename)):
		file = open(configFilename, 'r', encoding="utf-8")
		configFileString = file.read()
		file.close()
		print('Reading Config File')
	else:
		print('Config File not found')
		return False

	# Create return dict structure
	configDict = {
	}

	# Format file text
	cleanString = configFileString.strip()
	newLineList = cleanString.split('\n')
	for line in newLineList:
		breakAtColon = (line.strip()).split(':', 1)
		if(len(breakAtColon) != 2):
			print('Error reading config file')
			return False
		else:
			try:
				configDict[breakAtColon[0]] = (breakAtColon[1]).strip()
			except KeyError:
				print('Error reading config file: KeyError')
				return False

	print('Config file read successfully')

	if(show):
		print(configDict)

	return configDict

def ResetDirectory():

	# Reset the CWD of the program
	try:

		cwd = os.getcwd()

		appPath = path.abspath(path.dirname(sys.argv[0]))

		if(cwd != appPath):
			print('Changing filepath to: ' + str(appPath))
			os.chdir(appPath)

	except Exception as e:
		print('Failed to re-assign file directory. Continuing (Failing) in 5...')
		sleep(5)

	return

# This needs to be called pretty early
class Console():

	def __init__(self, configDict):
		self.configDict = configDict
		self.logging = True
		self.directory = None
		self.errorLogDirectory = None
		self.filepath = None
		self.errorLogFilepath = None

		if(not self.configDict['logging'].lower() == 'true'):
			self.logging = False
			print('Logging has been disabled')


		ignoreList = ['', ' ', '-', 'default', 'none']
		if(not configDict['logDir'] in ignoreList):
			self.directory = configDict['logDir']
			print('Custom Logging filepath found')
		if(not configDict['errorLogDir'] in ignoreList):
			self.errorLogDirectory = configDict['errorLogDir']
			print('Custom Error Logging filepath found')

		if(self.directory == None):
			self.filepath = self.LogFileStartup('logs')
		else:
			self.filepath = self.LogFileStartup(self.directory)

		if(self.errorLogDirectory == None):
			self.errorLogFilepath = self.ErrorLogFileStartup('errorLogs')
		else:
			self.errorLogFilepath = self.ErrorLogFileStartup(self.errorLogDirectory)

		return

	def LogFileStartup(self, folderPath):
		'''
		Each day has a new log file.
		'''

		logFilename = ''

		# Check if log file exists. If not, create it
		if(folderPath == 'logs' and not path.exists('logs')):
			# Make the log folder
			os.mkdir('logs')

		# Create todays filename
		dateString = datetime.date.today().strftime('%d%B%Y')
		logFilename = 'Log_' + dateString + '.txt'

		filepath = folderPath + '/' + logFilename
		# Check if file exists
		if(not path.exists(filepath)):
			print('Creating new log file')
			file = open(filepath, 'w', encoding="utf-8")
			file.write('=========================\nLog File Created: ' + dateString + '\n=========================')
			file.close()

		timeString = datetime.datetime.now().strftime("[%H:%M:%S]")
		# Message to identify run starting
		file = open(filepath, 'a', encoding="utf-8")
		file.write('\n--------------- New Run: ' + dateString + ' @ ' + timeString + ' ---------------')
		file.close()

		return filepath

	def ErrorLogFileStartup(self, folderPath):

		logFilename = ''

		# Check if log file exists. If not, create it
		if(folderPath == 'errorLogs' and not path.exists('errorLogs')):
			# Make the log folder
			os.mkdir('errorLogs')

		# Create todays filename
		dateString = datetime.date.today().strftime('%d%B%Y')
		logFilename = 'errorLogs_' + dateString + '.txt'

		filepath = folderPath + '/' + logFilename
		# Check if file exists
		if(not path.exists(filepath)):
			print('Creating new errorLog file')
			file = open(filepath, 'w', encoding="utf-8")
			file.write('=========================\nError Log File Created: ' + dateString + '\n=========================')
			file.close()

		timeString = datetime.datetime.now().strftime("[%H:%M:%S]")
		# Message to identify run starting
		file = open(filepath, 'a', encoding="utf-8")
		file.write('\n--------------- New Run: ' + dateString + ' @ ' + timeString + ' ---------------')
		file.close()

		return filepath

	def Log(self, msg):

		if(self.logging):
			lineStart = '\n' + datetime.datetime.now().strftime("[%H:%M:%S]")
			fullMessage = lineStart + str(msg)
			file = open(self.filepath, 'a', encoding="utf-8")
			file.write(fullMessage)
			file.close()

		return True

	def ErrorLog(self, msg):

		lineStart = '\n' + datetime.datetime.now().strftime("[%H:%M:%S]")
		fullMessage = lineStart + str(msg)
		file = open(self.errorLogFilepath, 'a', encoding="utf-8")
		file.write(fullMessage)
		file.close()

		return True

	def LogCompletionMessage(self):

		dateString = datetime.date.today().strftime('%d%B%Y')
		timeString = datetime.datetime.now().strftime("[%H:%M:%S]")
		self.Log('\n--------------- Run Completed: ' + dateString + ' @ ' + timeString + ' ---------------')

		return True


def update_DB_state(state):

	if state=='Started':
		query=f"""
			UPDATE dbo.adm_job_control 
			SET
			[Status]='{state}',
			[StartDate]=getdate(),
			[EndDate]=NULL
			where JobName='usersDetails'
		"""
	else:
		query=f"""
			UPDATE dbo.adm_job_control 
			SET
			[Status]='{state}',
			[EndDate]=getdate()
			where JobName='usersDetails'
		"""
	executeSQLquery(query)




class UsersDetailsScraper():

	def __init__(self, configDict,console):
		self.console=console
		self.configDict=configDict
		self.company_id="4yfupeazb4fivchggmqd0g"
		token={'access_token': 'oauth2v2_d71a77649f155a7c4ff2d1662793d411',
			 'expires_at': 1654647128.7692978,
			 'expires_in': 86400,
			 'refresh_token': 'oauth2v2_c7c1d9d01fa381db4c4c312b8149f3df',
			 'token_type': 'Bearer'}
		self.config = upwork.Config({'client_id': 'f4a05b02cbcd74c0f66b318e4f2477cf', 'client_secret': '32348ac79caf413a', 'token': token})
		self.client=upwork.Client(self.config)
	
	def logInfo(self,info):
		print(info)
		self.console.Log(info)

	def getUsersProfilesKeysUnofficial(self):
		self.logInfo("Extracting profilesURLs from DB..")
		query=f"SELECT EmployeeId,UpworkProfileURL, UserID FROM dbo.tblEmployee;"
		res=executeSQLquery(query)
		data=list(res)
		idKeyList=list()
		for i in data:
			
			if i[1] != None:
				key=i[1].split("?s=")[0]
				key=key.split("/")[-1]#.split("?")[0]
				if len(key.strip()) == 0:
					key=i[1].split("/")[-2]
				key = key.split("?")[0]
			else:
				key="NULL"
			# print([i[0],key, i[2]])
			idKeyList.append([i[0],key, i[2]])
		return idKeyList

	def getUsersProfilesKeys(self):
		self.logInfo("Extracting profilesURLs from DB..")
		query=f"SELECT EmployeeId,UpworkProfileURL FROM dbo.tblEmployee;"
		res=executeSQLquery(query)
		data=list(res)
		idKeyList=list()
		for i in data:
			
			if i[1] != None:
				key=i[1].split("?s=")[0]
				key=key.split("/")[-1]#.split("?")[0]
				if len(key.strip()) == 0:
					key=i[1].split("/")[-2]
				key = key.split("?")[0]
			else:
				key="NULL"
			idKeyList.append([i[0],key])
		return idKeyList

	def extractEngagementApiDate(self,engagements):
		if str(type(engagements)) == "<class 'dict'>":
			engagements=[engagements]

		engagements_without_BM_data=list()
		engagements_BM_data=list()

		for engagement in engagements:
			title=engagement['job__title'].lower()
			if  (("yittbox" in title) and ("business" in title) and ("manager" in title)):
				engagements_BM_data.append(engagement)
			else:
				engagements_without_BM_data.append(engagement)
		BMOrigRate="NULL"
		BMCurRate = "NULL"
		OrigRate = "NULL"
		CurRate ="NULL"
		LastRaiseDateBM = ""
		LastRaiseDate = ""

		if len(engagements_BM_data) != 0:
			oldest_BM_contract = engagements_BM_data[0]
			new_BM_contract = engagements_BM_data[-1]
			if oldest_BM_contract["engagement_job_type"] == "hourly": 
				BMOrigRate = self.floatPrepareForDB(oldest_BM_contract["hourly_charge_rate"])

			if new_BM_contract["engagement_job_type"] == "hourly": 
				BMCurRate = self.floatPrepareForDB(new_BM_contract["hourly_charge_rate"])

			if BMOrigRate != BMCurRate and (BMCurRate != "NULL"):
				LastRaiseDateBM = dt.fromtimestamp(int(new_BM_contract["created_time"])/1000)
			else:
				LastRaiseDateBM = ""

		if len(engagements_without_BM_data) != 0:
			oldest_contract = engagements_without_BM_data[0]
			new_contract = engagements_without_BM_data[-1]
			if oldest_contract["engagement_job_type"] == "hourly": 
				OrigRate = self.floatPrepareForDB(oldest_contract["hourly_charge_rate"])
			else:
				OrigRate = "NULL"
			if new_contract["engagement_job_type"] == "hourly": 
				CurRate = self.floatPrepareForDB(new_contract["hourly_charge_rate"])
			else:
				CurRate = "NULL"

			if OrigRate != CurRate and (CurRate != "NULL"):
				LastRaiseDate = dt.fromtimestamp(int(new_contract["created_time"])/1000)
			else:
				LastRaiseDate = ""

		if LastRaiseDate != "" and LastRaiseDateBM != "":
			if LastRaiseDate < LastRaiseDateBM:
				LastRaiseDate=self.stringPrepareForDB(LastRaiseDateBM.strftime("%Y-%m-%d %H:%M:%S"))
			else:
				LastRaiseDate=self.stringPrepareForDB(LastRaiseDate.strftime("%Y-%m-%d %H:%M:%S"))

		elif LastRaiseDate != "":
			LastRaiseDate=self.stringPrepareForDB(LastRaiseDate.strftime("%Y-%m-%d %H:%M:%S"))

		elif LastRaiseDateBM != "":
			LastRaiseDate=self.stringPrepareForDB(LastRaiseDateBM.strftime("%Y-%m-%d %H:%M:%S"))

		else:
			LastRaiseDate=self.stringPrepareForDB("")


		HireDate = self.stringPrepareForDB(dt.fromtimestamp(int(engagements[0]["created_time"])/1000).strftime("%Y-%m-%d %H:%M:%S"))
		
		LastContract = engagements[-1]



		return OrigRate, CurRate, LastRaiseDate, HireDate ,LastContract , BMOrigRate, BMCurRate
	def stringPrepareForDB(self,string):
		string=string.replace("'","''")
		if string.strip() == "":
			return "NULL"
		else:
			return "'"+string+"'"
	def floatPrepareForDB(self,string):
		try:
			data=float(string)
			return data
		except:
			return "NULL"
	def extractUserDetailsUnofficial(self, userCipherText, EmployeeId, creds):
		token=creds[0]
		vnd_eo_span_id=creds[1]
		vnd_eo_parent_span_id=creds[2]
		vnd_eo_trace_id=creds[3]
		cookies= {
	        'enabled_ff': 'CI11132Air2Dot75,CI9570Air2Dot5,!CI10270Air2Dot5QTAllocations,!CI10857Air3Dot0,!air2Dot76,!air2Dot76Qt,!OTBnr,!SSINav,OTBnrOn',
	        'lang': 'en',
	        'lang': 'en',
	        'cookie_prefix': '',
	        'cookie_domain': '.upwork.com',
	        '_vwo_uuid_v2': 'D61822BEB4F1A018AD0B75E29C7C78B92|e9675e4c7691f227418b063681cac5b6',
	        '_pxvid': '91514b8a-b625-11ec-b339-67516a4c6473',
	        'pxcts': '920e60a9-b625-11ec-9930-6f6555695456',
	        '_vis_opt_test_cookie': '1',
	        '_vis_opt_s': '1%7C',
	        '_vwo_uuid': 'D61822BEB4F1A018AD0B75E29C7C78B92',
	        '_vwo_ds': '3%241649303301%3A10.8202377%3A%3A',
	        '_vis_opt_exp_24_combi': '2',
	        'device_view': 'full',
	        '_gcl_au': '1.1.2088992491.1649303304',
	        'OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Apr+07+2022+04%3A48%3A24+GMT%2B0100+(Central+European+Standard+Time)&version=6.28.0&isIABGlobal=false&hosts=&consentId=060b4e69-9353-4039-a37c-c873d6148be3&interactionCount=1&landingPath=https%3A%2F%2Fwww.upwork.com%2F&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1',
	        'AWSALB': 'K/LYpIV+uvPuiIS6A9Ca+8qaZE70nhGBMxAhfclgyq40PAt94CGmHu77hs6GzA+wEz2cyCW1czkcMW4JoJroknXeUXNAaoZHM4sFB27u5jyipw2K5WISUoznTer0',
	        'AWSALBCORS': 'K/LYpIV+uvPuiIS6A9Ca+8qaZE70nhGBMxAhfclgyq40PAt94CGmHu77hs6GzA+wEz2cyCW1czkcMW4JoJroknXeUXNAaoZHM4sFB27u5jyipw2K5WISUoznTer0',
	        'spt': '3a537220-aa86-4c40-852d-33383e0365fa',
	        '_gid': 'GA1.2.1356305297.1649303306',
	        'G_ENABLED_IDPS': 'google',
	        '__pdst': 'df2b72b4b98a42788c828aa8cb79dc1d',
	        '_rdt_uuid': '1649303306877.120c2c32-becf-4485-8b26-a336a63ff215',
	        'IR_gbd': 'upwork.com',
	        '_fbp': 'fb.1.1649303307007.770244220',
	        'recognized': 'yittboxhm',
	        'console_user': 'yittboxhm',
	        'user_uid': '1108567846488072192',
	        'DA_yittboxhm': '5a859d176df1b277acfeb2220dbd0173fa134599082e0d19c06d50cbb1f939e6',
	        'channel': 'other',
	        'SZ': '104220b6397dc09b0711353d9d47ca2fe1d062bf51abe4cedbf83bb6791ac93b',
	        'odesk_signup.referer.raw': 'https%3A%2F%2Fwww.upwork.com%2Ffreelancers%2Fsettings%2FcontactInfo',
	        'company_last_accessed': '6112778',
	        'current_organization_uid': '1108561291845459968',
	        'JobSearchUI_tile_size_1108567846488072192': 'medium',
	        'IR_13634': '1649384934779%7C0%7C1649384934779%7C%7C',
	        '_dpm_id.5831': 'ffaa6f64-408a-4250-9367-fb6837009ea5.1649303307.3.1649384935.1649369569.a8350d51-1512-4c40-9f3e-0441fb5c79ab',
	        '_uetvid': '94c4b620b62511ec98935134f47858bb',
	        '_ga': 'GA1.2.922223323.1649303305',
	        '_clck': '1ek5b70|1|f0h|0',
	        '_clsk': '1503thm|1649465285483|1|0|l.clarity.ms/collect',
	        '_ga_KSM221PNDX': 'GS1.1.1649467152.4.0.1649467153.0',
	        '__cfruid': 'b6e6d4946d30dd4ef7849f42bc5da268cfef268d-1649470455',
	        '__cf_bm': 'sgtoUET.Tmu9xVxF679hi7IiuoV7Dfa.ZQm3sX4XE0k-1649538175-0-AbgIHORa9FZ8KkQ3Q5qv9q8JoIlGe+/W2/b5RoggkswqsXozJkqIc7oR6/KTpN7zSauwTCSaXOMVAjOzu5x1aEo=',
	        '_pxhd': 'vA7LOggKEyLfrOfjP0jP2oArqwCwrd/VIxrOlZwzNeEt5qjMOn9IV/Euv-qFRUPByG/G2HX9IoyL59Vae3wzIw==:kwJTKEw93FweFuN9PaY8ctFFT1bJVhxE5/qPSK2u/T-OO7DVUekseQkFyy4AQu7MxRwh7pvxazPM3oZ---esk/cH4szuePM20F6IuRw1bIU=',
	        'master_access_token': f'9505c4a3.{token}',
	        'oauth2_global_js_token': token,
	        'XSRF-TOKEN': '5dfa16d2db928e998c83dc93757677f1',
	        '_gat_UA-62227314-1': '1',
	        '_sp_ses.2a16': '*',
	        '_hp2_ses_props.2858077939': '%7B%22ts%22%3A1649538179907%2C%22d%22%3A%22www.upwork.com%22%2C%22h%22%3A%22%2Fnx%2Fplans%2Fmembership%2Findex%22%2C%22q%22%3A%22%3FisSuccess%3D1%22%7D',
	        '_hp2_props.2858077939': '%7B%22container_id%22%3A%22GTM-WNVF2RB%22%2C%22user_context%22%3A%22agency%22%2C%22user_logged_in%22%3Atrue%7D',
	        '_hp2_id.2858077939': '%7B%22userId%22%3A%222166490857744133%22%2C%22pageviewId%22%3A%226908285715225399%22%2C%22sessionId%22%3A%222880864680784323%22%2C%22identity%22%3A%221108567846488072192%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D',
	        '_px3': 'c173754f5b2f72297d3e64d69a2c05751bcbb89af95949c5e2a2c8b67e4e7cc5:Df1LuZwstrRVmdR9iiu2yWnZfRvyx/N5HqwuDf8KzguxVMWtS7IKMOrlSsmEpF9JRGmBvcMrY0rG1cP+TLzf9w==:1000:MOnwgcuAT7ARe+EKIiyxslHElaS6xFgpUS/UmTOkmkTNKHpd2OAM2ZkGt+5qdj8o2+VNKqQVE+R/sBHUgg9cFUh6hJT2m/FmXwvt8S11j5MJsWf6cbARx5VCjp1QwxkHKKMeo9VaIU9eWDFVNEoVfrHrcMYIP94/l5gAQNN3j+9n8cjjWHgikmknY3+VPdmaIw5wQlQSieu10/WEWFW81g==',
	        '_sp_id.2a16': '977da421-2b0d-4f50-9ae0-79fddacd4915.1649303304.8.1649538190.1649467726.2373c429-78af-4ab8-91c8-ec716967136c',
	    }
		headers = {
	        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0',
	        'Accept': 'application/json, text/plain, */*',
	        'Accept-Language': 'en-US,en;q=0.5',
	        'Referer': f'https://www.upwork.com/freelancers/{userCipherText}',
	        'x-odesk-user-agent': 'oDesk LM',
	        'x-requested-with': 'XMLHttpRequest',
	        'vnd-eo-trace-id': vnd_eo_trace_id,
	        'vnd-eo-span-id': vnd_eo_span_id,
	        'vnd-eo-parent-span-id': vnd_eo_parent_span_id,
	        'Authorization': f'Bearer {token}',
	        'Connection': 'keep-alive',
	        'Sec-Fetch-Dest': 'empty',
	        'Sec-Fetch-Mode': 'cors',
	        'Sec-Fetch-Site': 'same-origin',
	    }
		attempts=0
		while attempts <3:
			try:
			    response = requests.get(f'https://www.upwork.com/freelancers/api/v1/freelancer/profile/{userCipherText}/details', headers=headers, cookies=cookies)
			    break
			except Exception as e:
				print('Failed to get the response')
				print(e)
				attempts+=1
		if attempts == 3:
		    return False



		data = json.loads(response.text)

		
		try:
			uid = data['profile']['identity']['uid']
			query=f"""
				UPDATE dbo.tblEmployee
				SET
					UserID= {uid},
					LastUpdatedBy = 'usersDetails.py',
					LastUpdatedDate = getdate()
				WHERE
					EmployeeId = {EmployeeId}
				"""
			executeSQLquery(query)
			self.logInfo("Updated tblEmployee with extracted data!")
		except Exception as e:
			uid = None
			print(data)

		return uid
		

		


	def extractDataForUser(self,profile_key,EmployeeId, creds):
		db_conn, db_cursor = connectToDatabaseThread(globals()['configDict'])

		# uid = extractUserDetailsUnofficial(profile_key, creds)

		

		try:
			attempts = 0
			while True:
				try:
					userApiResult=upwork.routers.freelancers.profile.Api(self.client).get_specific(profile_key)
					profileInfo=userApiResult['profile']
					engagementApiResult=upwork.routers.hr.engagements.Api(self.client).get_list({'profile_key':profile_key,'buyer_team__reference':'5066971'})
					break
				except Exception as e:
					attempts+=1
					if "line 1 column 1 (char 0)" in str(e):
						self.logInfo("Upwork API connection aborted. ERROR:"+str(e))
						return
					if attempts ==5:
						self.logInfo("Upwork API connection aborted. ERROR:"+str(e))
						if "profile" in str(e):
							return
						sendErrorEmail(self.console,"Upwork API connection aborted, Script didn't recieve any data back from the API. ERROR:"+str(e))
						return
					sleep(2.5)


			try:
				if userApiResult['error']['status'] == 403:
					self.logInfo("ERROR: " +  userApiResult['error']['message'])
					return
			except:
				pass
			try:
				profileInfo=userApiResult['profile']
			except Exception as e :
				sendErrorEmail(self.console,"Error userDetails Guy. Error:" +str(e)+" EmployeeId:"+str(EmployeeId)+"\n API RESULT:"+str(userApiResult) )
				return
			EmployeeFirstName = self.stringPrepareForDB(profileInfo["dev_first_name"])
			EmployeeLastName = self.stringPrepareForDB(profileInfo["dev_last_name"])
			UpworkEmployeeFullName = self.stringPrepareForDB(profileInfo["dev_short_name"])
			profile_status=self.stringPrepareForDB(profileInfo["dev_ui_profile_access"])
			try:
				engagements=engagementApiResult["engagements"]["engagement"]
			except KeyError:
				return
			
			OrigRate, CurRate, LastRaiseDate, HireDate, LastContract, BMOrigRate, BMCurRate = self.extractEngagementApiDate(engagements)
			
			TimeZone = self.stringPrepareForDB(profileInfo["dev_timezone"].split(" ")[0])
			Country = self.stringPrepareForDB(profileInfo["dev_country"])
			Talents = self.stringPrepareForDB(profileInfo["dev_profile_title"])
			City = self.stringPrepareForDB(profileInfo["dev_city"])
			City = City.replace(","," ").replace(":"," ").replace("  "," ").replace("  "," ").title()
			# Check last contract status:
			try:
				if LastContract["is_paused"].strip() == "1":
					IsPaused = 1
				else:
					IsPaused = 0
			except KeyError:
				IsPaused= 0

			UpworkUserId = self.stringPrepareForDB(LastContract["provider__id"])
			UpworkEmployeeProfileName = self.stringPrepareForDB(profileInfo["dev_short_name"])
			Contract_Id = self.stringPrepareForDB(LastContract["reference"])
			Talent_Badge = self.stringPrepareForDB(profileInfo["talent_badge"])
			Dev_Bill_Rate = self.floatPrepareForDB(profileInfo["dev_bill_rate"])
			Last_Worked_Day = self.stringPrepareForDB(profileInfo["dev_last_worked"])
			Picture_URL = self.stringPrepareForDB(profileInfo["dev_portrait"])
			Skills = ""

			if str(type(profileInfo["skills"]["skill"])) == "<class 'dict'>":
				skillsList=[profileInfo["skills"]["skill"]]
			else:
				skillsList=profileInfo["skills"]["skill"]
			
			for skill in skillsList:
				Skills += skill["skl_name"] + " | "
			
			if len(Skills) != 0:
				Skills = Skills[:-2]
			Skills = self.stringPrepareForDB(Skills)

			Total_Hours_Worked = self.floatPrepareForDB(profileInfo["dev_total_hours"])
			Feedbacks_Score = self.floatPrepareForDB(profileInfo["dev_adj_score"])
			"""
			print("EmployeeFirstName:",EmployeeFirstName)
			print("EmployeeLastName:",EmployeeLastName)
			print("UpworkEmployeeFullName:",UpworkEmployeeFullName)
			print("OrigRate:",OrigRate)
			print("CurRate:",CurRate)
			print("LastRaiseDate:",LastRaiseDate)
			print("HireDate:",HireDate)
			print("TimeZone:",TimeZone)
			print("Country:",Country)
			print("Talents:",Talents)
			print("City:",City)
			print("IsPaused:",IsPaused)
			print("UpworkUserId:",UpworkUserId)
			print("UpworkEmployeeProfileName:",UpworkEmployeeProfileName)
			print("Contract_Id:",Contract_Id)
			print("Talent_Badge:",Talent_Badge)
			print("Dev_Bill_Rate:",Dev_Bill_Rate)
			print("Last_Worked_Day:",Last_Worked_Day)
			print("Picture_URL:",Picture_URL)
			print("Skills:",Skills)
			print("Total_Hours_Worked:",Total_Hours_Worked)
			print("Feedbacks_Score:",Feedbacks_Score)
			print("#################################################")
			"""
			"""
			ALTER TABLE dbo.tblEmployee ADD Talent_Badge VARCHAR(25) NULL, Dev_Bill_Rate MONEY NULL, Last_Worked_Day VARCHAR(20) NULL, Picture_URL VARCHAR(150) NULL, Skills VARCHAR(700) NULL, Total_Hours_Worked MONEY NULL, Feedbacks_Score MONEY NULL;
			EmployeeLastName= {EmployeeLastName}
			IsPaused= {IsPaused},
			UpworkEmployeeFullName= {UpworkEmployeeFullName}
			"""
			if BMCurRate == "NULL" and BMOrigRate == "NULL":
				query=f"""
				UPDATE dbo.tblEmployee
				SET
					EmployeeFirstName= {EmployeeFirstName},
					OrigRate= {OrigRate},
					CurRate= {CurRate},
					LastRaiseDate= {LastRaiseDate},
					HireDate= {HireDate},
					TimeZone= {TimeZone},
					Country= {Country},
					Talents= {Talents},
					City= {City},
					
					UpworkUserId= {UpworkUserId},
					UpworkEmployeeProfileName= {UpworkEmployeeProfileName},
					Contract_Id= {Contract_Id},
					Talent_Badge= {Talent_Badge},
					Dev_Bill_Rate= {Dev_Bill_Rate},
					Last_Worked_Day= {Last_Worked_Day},
					Picture_URL= {Picture_URL},
					Skills= {Skills},
					Total_Hours_Worked= {Total_Hours_Worked},
					Feedbacks_Score= {Feedbacks_Score},
					Profile_Status={profile_status},
					LastUpdatedBy = 'usersDetails.py',
					LastUpdatedDate = getdate()
				WHERE
					EmployeeId = {EmployeeId}
				"""
			else:
				query=f"""
				UPDATE dbo.tblEmployee
				SET
					EmployeeFirstName= {EmployeeFirstName},
					OrigRate= {OrigRate},
					CurRate= {CurRate},
					LastRaiseDate= {LastRaiseDate},
					HireDate= {HireDate},
					TimeZone= {TimeZone},
					Country= {Country},
					Talents= {Talents},
					City= {City},
					UpworkUserId= {UpworkUserId},
					UpworkEmployeeProfileName= {UpworkEmployeeProfileName},
					Contract_Id= {Contract_Id},
					Talent_Badge= {Talent_Badge},
					Dev_Bill_Rate= {Dev_Bill_Rate},
					Last_Worked_Day= {Last_Worked_Day},
					Picture_URL= {Picture_URL},
					Skills= {Skills},
					Total_Hours_Worked= {Total_Hours_Worked},
					BMOrigRate={BMOrigRate},
					BMCurRate = {BMCurRate},
					Feedbacks_Score= {Feedbacks_Score},
					Profile_Status={profile_status},
					LastUpdatedBy = 'usersDetails.py',
					LastUpdatedDate = getdate()
				WHERE
					EmployeeId = {EmployeeId}
				"""
			executeSQLqueryThread(query, db_cursor)
			self.logInfo("Updated tblEmployee with extracted data!")
		except Exception as e:
			GLOBALTRACEBACKDATA(self.console)
			self.logInfo("ERROR IN EmployeeId: "+str(EmployeeId))

	def extractCurrentTeamMembersUW(self):
		attempts=0
		while True:
			freelancers_profile_key=set()
			self.logInfo("Getting the total number of contracts to extract..")
			try:
				engagements=upwork.routers.hr.engagements.Api(self.client).get_list({'buyer_team__reference':'5066971'})
				total_count= int(engagements['engagements']['lister']['total_count'])
				if total_count % 10 == 0:
					number_of_requests=int(total_count/10)
				else:
					number_of_requests=int(total_count/10) + 1
				self.logInfo("FOUND "+str(total_count)+" contracts ==> We need to make "+str(number_of_requests)+" requests")
				break

			except Exception as e:
				attempts+=1
				if attempts == 9:
					sendErrorEmail(self.console,"Upwork API connection aborted, Script didn't recieve any data back from the API. ERROR:"+str(e))
					self.logInfo("Upwork API connection aborted.")
					sys.exit(0)
				sleep(3)

		attempts=0
		count=0
		while True:
			try:
				while count < number_of_requests:
					self.logInfo("REQUEST #"+str(count+1)+"/"+str(number_of_requests))
					engagements=upwork.routers.hr.engagements.Api(self.client).get_list({'buyer_team__reference':'5066971','page':str(count*10)+':10'})
					engagements=engagements['engagements']['engagement']
					if str(type(engagements)) == "<class 'list'>":
						for engagement in engagements:
							freelancers_profile_key.add(engagement['dev_recno_ciphertext'])
					else:
						freelancers_profile_key.add(engagements['dev_recno_ciphertext'])
					print("Currently: "+str(len(freelancers_profile_key))+" distinct profiles found.")
					count+=1
				return list(freelancers_profile_key)
			except Exception as e:
				attempts+=1
				if attempts == 9:
					sendErrorEmail(self.console,"Upwork API connection aborted, Script didn't recieve any data back from the API. ERROR:"+str(e))
					self.logInfo("Upwork API connection aborted.")
					sys.exit(0)
				sleep(3)
	def checkAndAddNewTeamMembers(self,freelancers_profile_key,idKeyList):
		self.logInfo("Checking if any of the extracted profiles is not present in the DB data..")
		for profile_key in freelancers_profile_key:
			new=True
			for entry in idKeyList:
				if entry[1] == profile_key:
					new=False
					break
			if new:

				profileURL="https://www.upwork.com/freelancers/"+profile_key
				self.logInfo("FOUND new profile: "+profileURL)
				query=f"""
					INSERT INTO dbo.tblEmployee (UpworkProfileURL,CreatedBy,CreatedDate,ActiveFlg,ActiveFlgAgency,IsPaused,BusinessManagerFlg)
					VALUES('{profileURL}','usersDetails.py',getdate(),1,1,0,0);
				"""
				executeSQLquery(query)
				self.logInfo("Added to DB!")

	def Start(self):
		freelancers_profile_key=self.extractCurrentTeamMembersUW()
		idKeyList=self.getUsersProfilesKeys()

		self.checkAndAddNewTeamMembers(freelancers_profile_key,idKeyList)
		idKeyList=self.getUsersProfilesKeys()
		
		#idKeyList = [[52,"~01e8e8a9c1c52ebc0f"]]
		self.logInfo("Entries To updated: " + str(len(idKeyList)))
		# threading 
		# with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
		# 	executor.submit()

		query=f"SELECT [auth2_token], [vnd_eo_span_id], [vnd_eo_parent_span_id], [vnd_eo_trace_id], [Cookies] FROM [dbo].[UpworkAPICreds] WHERE creds_for='{self.configDict['creds_for']}'"
		results=executeSQLquery(query)
		res=list(results)
		token=res[0][0]
		vnd_eo_span_id=res[0][1]
		vnd_eo_parent_span_id=res[0][2]
		vnd_eo_trace_id=res[0][3]
		cookies=res[0][4]
		creds=[token, vnd_eo_span_id, vnd_eo_parent_span_id, vnd_eo_trace_id,cookies]

		keysUnofficial = self.getUsersProfilesKeysUnofficial()

		for idx, entry in enumerate(keysUnofficial):
			self.logInfo(f"Processing Unofficial: {str(idx)}/{str(len(keysUnofficial))}")
			if entry[1][0] == "~": # NULL or custom URL
				if entry[2] != None:
					continue
				else:
					self.extractUserDetailsUnofficial(entry[1],entry[0],creds)
			else:
				self.logInfo("Ignored the following record because profile key invalid: "+str(entry))

		def extractDataAsThreads(idKeyList_thread, thread):
			counter = 1
			for entry in idKeyList_thread:
				self.logInfo(f"Processing: {str(counter)}/{str(len(idKeyList_thread))} of thread {thread}")
				if entry[1][0] == "~": # NULL or custom URL
					self.extractDataForUser(entry[1],entry[0],creds)
				else:
					self.logInfo("Ignored the following record because profile key invalid: "+str(entry))
				counter+=1

		start = 0
		end = 50 + len(idKeyList) % 50
		threads = []

		for thread in range(int(len(idKeyList) / 50)):
			idKeyList_thread = idKeyList[start:end]
			self.logInfo(f"Started thread {thread}.....")

			t = threading.Thread(target=extractDataAsThreads, args=[idKeyList_thread, thread])
			t.start()
			threads.append(t)

			start = end
			end += 50

		for thread in threads:
			thread.join()

def Main(configDict, console):
	
	ResetDirectory()

	
	scraper=UsersDetailsScraper(configDict,console)
	scraper.Start()

	print("PROGRAM FINISHED!")
	return True

def test():
	
	roomid="room_169167c2501e7d2fdb4cf417a9bbae4b"
	configDict = ReadConfig()
	
	config=upwork.Config({"consumer_key":configDict['key'],
							"consumer_secret":configDict['secret'],
							"access_token":configDict['access_token'],
							"access_token_secret":configDict['access_token_secret'],})
	client=upwork.Client(config)
	company_id="4yfupeazb4fivchggmqd0g"
	#activities.team.Api(client).get_list("4yfupeazb4fivchggmqd0g","4yfupeazb4fivchggmqd0g")
	activity_id="activity_id_for_testing"#https://www.upwork.com/freelancers/~01459c54f517050245
	print( upwork.routers.freelancers.profile.Api(client).get_specific("~01459c54f517050245") )
	#print(upwork.routers.hr.engagements.Api(client).get_list({'profile_key':'~01af218f15e8f43bd8'}))
	#activities.team.Api(client).archive_activities(company_id,company_id,activity_id)
	#print(activities.team.Api(client).get_specific_list(company_id,company_id,activity_id))
	#print(activities.team.Api(client).update_activities(company_id,company_id,activity_id,{'description':"TEST_description","engagements":"24355084"}))
	#print(activities.team.Api(client).add_activity(company_id,company_id,{'code':'TEST_Test','description':'TEST_Test'}))
	#print(activities.team.Api(client).unarchive_activities(company_id,company_id,activity_id))


try:
	start = time.time()
	globals()['configDict'] = ReadConfig()
	connectToDatabase(globals()['configDict'])
	update_DB_state('Started')
	console = Console(globals()['configDict'])
	
	Main(globals()['configDict'], console)
	update_DB_state('Completed')
	end = time.time()
	print(f'Program finished in {end-start} seconds')
except Exception as e:
	print(e)
	console.ErrorLog(e)
	GLOBALTRACEBACKDATA(console)
	print('Closing application and related windows')
	update_DB_state('Failed')
	GLOBALEXIT()

# End.