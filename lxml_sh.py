# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2

#downloading website and generating tree
page = requests.get("https://www.umweltdaten.landsh.de/public/hsi/pegelliste.html")
tree = html.fromstring(page.content)

#getting names, water level, time, warning and IDs from the table of the website
mpNames = tree.xpath('//table[@class="pegeltabelle tablesorter"]//tbody//tr[td[position()=7]//a[contains(@href,"pegel.jsp")]]//td[position()=2]//a/text()')
mpWlevel = tree.xpath('//table[@class="pegeltabelle tablesorter"]//tbody//tr[td[position()=7]//a[contains(@href,"pegel.jsp")]]//td[position()=5]/text()')
mpTime = tree.xpath('//table[@class="pegeltabelle tablesorter"]//tbody//tr[td[position()=7]//a[contains(@href,"pegel.jsp")]]//td[position()=9]/text()')
mpWarning = tree.xpath('//table[@class="pegeltabelle tablesorter"]//tbody//tr[td[position()=7]//a[contains(@href,"pegel.jsp")]]//td[position()=1]//span/text()')
mpNumber = tree.xpath('//table[@class="pegeltabelle tablesorter"]//tbody//tr[td[position()=7]//a[contains(@href,"pegel.jsp")]]//td[position()=7]//a/@href')

data = []

mpTimeFixed = []
mpNumberFixed = []

#fixing the ID
for x in mpNumber:
  mpNumberFixed.append(x.split('=')[2])

#fixing the time format (YYYY-MM-DD HH:mm)
for x in mpTime:
  datetimeSplit = x.split('\xa0')
  dateSplit = datetimeSplit[0].split('.')
  date = dateSplit[2]+'-'+dateSplit[1]+'-'+dateSplit[0]+' '+datetimeSplit[1]
  mpTimeFixed.append(date)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

    
#iterating through the data and storing it in the database if an entry with the same name and time is
#not already in the database
for c in range(len(mpNames)):
  
  cur.execute("SELECT id FROM sh_data WHERE name = %s AND time = %s",(mpNames[c],mpTimeFixed[c]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO sh_data (name,wlevel,time,number,warning) VALUES(%s, %s, %s, %s, %s)",(mpNames[c],mpWlevel[c],mpTimeFixed[c],mpNumberFixed[c],mpWarning[c]))

  conn.commit()
  