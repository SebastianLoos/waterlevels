# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2

#downloading website and generating tree
page = requests.get("https://wasserportal.berlin.de/messwerte.php?anzeige=tabelle&thema=ws")
tree = html.fromstring(page.content)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting names, IDs, waterlevels, warnings and time from the table of the website
mpNames = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 2]//text()')
mpNumber = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 1]//a//text() | //table[@id="pegeltab"]//tbody//tr//td[position() = 1]//text()')
mpLevel = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 6]')
mpWarning = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 9]//text()')
mpTime = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 5]')


#iterating through the data and storing the data in the database if there is no entry with the same name and time already in the database
data = []
for c in range(len(mpNames)):
  
  print(mpNames[c])
  wlevel = None
  flow = None
  
  #fixing the water level value
  try:
    print(mpLevel[c].xpath('text()')[0])
    wlevel = float(mpLevel[c].xpath('text()')[0])
  except:
    continue
  
  print(mpWarning[c])
  time = None
  
  #fixing the time format to YYYY-MM-DD HH:mm
  try:
    timeSrc = mpTime[c].xpath('text()')[0]
    print(timeSrc)
    timeSplit = timeSrc.split(' ')
    timeDate = timeSplit[0].split('.')
    time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1];
    print(time)
  except:
    continue
  
  cur.execute("SELECT id FROM be_data WHERE name = %s AND time = %s",(mpNames[c],time))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO be_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(mpNames[c],wlevel,flow,time,mpNumber[c],mpWarning[c]))

  conn.commit()