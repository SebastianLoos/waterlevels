# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 08:51:56 2019

@author: Hayuki
"""

from lxml import html
import requests
import datetime
import psycopg2

#downloading website and generating tree
page = requests.get("https://www.hnd.bayern.de/pegel/meldestufen//tabellen")
tree = html.fromstring(page.content)


data = []
#getting the name, water level, flow, time and warning level
mpNames = tree.xpath('//tr[@class="row" or @class="row2"][count(td)>3]//td[position() = 1]//a/text()')
mpWlevel = tree.xpath('//tr[@class="row" or @class="row2"][count(td)>3]//td[position() = 4]/text()')
mpFlow = tree.xpath('//tr[@class="row" or @class="row2"][count(td)>3]//td[position() = 6]/text()')
mpTime = tree.xpath('//tr[@class="row" or @class="row2"][count(td)>3]//td[position() = 3]/text()')
mpWarning = tree.xpath('//tr[@class="row" or @class="row2"][count(td)>3]//td[position() = 7]/text()')
mpWlevelFixed = []
mpFlowFixed = []
mpTimeFixed = []
mpWarningFixed = []

now = datetime.datetime.now()

#fixing the water level value
for x in mpWlevel:
  a = x.replace(',','.')
  try:
    a = float(a)
  except:
    a = None
  mpWlevelFixed.append(a)

#fixing the flow value
for x in mpFlow:
  a = x.replace(',','.')
  try:
    a = float(a)
  except:
    a = None
  mpFlowFixed.append(a)
  
#fixing the time format to YYYY-MM-DD HH:mm
for x in mpTime:
  y = x.split(u'\xa0')
  a = str(now.year)+'-'+y[0].split('.')[1]+'-'+y[0].split('.')[0]+' '+y[2]
  mpTimeFixed.append(a)

#fixing the warning
for x in mpWarning:
  try:
    y = int(x)
  except:
    y = None
  mpWarningFixed.append(y)

#merging data for each measuring point into one array
for x in range(len(mpWlevel)):
  element = [mpNames[x],mpWlevelFixed[x],mpFlowFixed[x],mpTimeFixed[x],mpWarningFixed[x]]
  data.append(element)

print(data)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#iterating through the data, storing it in the database if there is no entry in the database with the same name and time already
for x in data:
  #getting the id for the measuring point from the database
  cur.execute("SELECT number FROM by_dpoints WHERE name = %s",(x[0],))
  result = ('0',)
  try:
    resultList = cur.fetchone()
    result = resultList[0]
  except:
    result = ('-1',)
  
  cur.execute("SELECT id FROM by_data WHERE name = %s AND time = %s",(x[0],x[3]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  print(x[0],x[1])
  print(result)
  print(x[3])
  cur.execute("INSERT INTO by_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(x[0],x[1],x[2],x[3],result,x[4]))

conn.commit()