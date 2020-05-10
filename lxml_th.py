# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2

#downloading website and generating tree
page = requests.get("https://hnz.thueringen.de/hw2.0/thueringen.html")
tree = html.fromstring(page.content)

#getting names, water level, flow, time , warning and IDs from the table of the website
mpNames = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[2]//a/text()')
mpWlevel = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[8]/text()')
mpFlow = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[11]/text()')
mpTime = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[7]/text()')
mpWarning = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[9]//span/@class')
mpNumber = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[2]//a/@href')

data = []

mpTimeFixed = []
mpNumberFixed = []
mpWarningFixed = []

#fixing the ID by removing the dot
for x in mpNumber:
  mpNumberFixed.append(x.split('.')[0])

#fixing time format (YYYY-MM-DD HH:mm)
for x in mpTime:
  datetimeSplit = x.split(' ')
  dateSplit = datetimeSplit[0].split('.')
  date = dateSplit[2]+'-'+dateSplit[1]+'-'+dateSplit[0]+' '+datetimeSplit[1]
  mpTimeFixed.append(date)

#the warning level is stored in the class of the HTML-Element of the cell in the table. here we are checking which warning is in the 
#class name
for x in mpWarning:
  if ("stufe-0" in x):
    mpWarningFixed.append(0)
  if ("stufe-1" in x):
    mpWarningFixed.append(1)
  if ("stufe-2" in x):
    mpWarningFixed.append(2)
  if ("stufe-3" in x):
    mpWarningFixed.append(3)
  if ("stufe-4" in x):
    mpWarningFixed.append(4)
  if ("stufe-6" in x):
    mpWarningFixed.append(-9)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#iterating through measuring points and storing them in the database if an entry with the same name and time is
#not already in the database
for c in range(len(mpNames)):
  wlevel = flow = None
  cur.execute("SELECT id FROM th_data WHERE name = %s AND time = %s",(mpNames[c],mpTimeFixed[c]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  try:
    wlevel = float(mpWlevel[c])
  except:
    print('no wlevel')
  
  #removing spaces and correcting decimal seperator character
  try:
    flow = float(mpFlow[c].replace(' ','').replace(',','.'))
  except:
    print('no flow')
  
  cur.execute("INSERT INTO th_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(mpNames[c],wlevel,flow,mpTimeFixed[c],mpNumberFixed[c],mpWarningFixed[c]))

  conn.commit()
  