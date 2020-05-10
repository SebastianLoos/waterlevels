# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2

#downloading website and generating tree
page = requests.get("https://pegelportal-mv.de/pegel-mv/pegel_list.html")
tree = html.fromstring(page.content)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting names, IDs, waterlevel, warning and time from the table of the website
mpNames = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV" and td[position() = 4]//text()!=""]//td[position() = 1]/text()')
mpNumber = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV" and td[position() = 4]//text()!=""]//td[position() = 8]//a/@href')
mpLevel = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV" and td[position() = 4]//text()!=""]//td[position() = 4]/text()')
mpWarning = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV" and td[position() = 4]//text()!=""]//td[position() = 6]//img/@title')
mpTime = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV" and td[position() = 4]//text()!=""]//td[position() = 3]/text()')

mpNumberFixed = []
for x in mpNumber:
  mpNumberFixed.append(x.split('_')[1].split('.')[0])

mpWarningFixed = []
for x in mpWarning:
  mpWarningFixed.append(x.split(' ')[1])

#iterating through missing measuring points, crawling data and storing it in the database if an entry with the 
#same name and time is not already in the database
data = []
for c in range(len(mpNames)):
  
  #fixing encoding of the strings
  name = mpNames[c].__str__().encode("ISO-8859-1").decode("utf-8")
  print(name)
  print(mpNames[c])
  wlevel = None
  flow = None
  try:
    print(mpLevel[c])
    wlevel = float(mpLevel[c])
  except:
    continue
  print(mpWarning[c])
  try:
    warning = int(mpWarningFixed[c])
  except:
    warning = 9
  
  time = None
  
  #fixing time format to YYYY-MM-DD HH:mm
  try:
    timeSrc = mpTime[c]
    print(timeSrc)
    timeSplit = timeSrc.split(' ')
    timeDate = timeSplit[0].split('.')
    time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1];
    print(time)
  except:
    continue
  
  cur.execute("SELECT id FROM mv_data WHERE name = %s AND time = %s",(mpNames[c],time))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO mv_data (name,wlevel,time,number,warning) VALUES(%s, %s, %s, %s, %s)",(name,wlevel,time,mpNumberFixed[c],warning))

  conn.commit()