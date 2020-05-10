# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2


data = []

#downloading website and generating tree
page = requests.get("http://luadb.it.nrw.de/LUA/hygon/pegel.php?karte=nrw")
tree = html.fromstring(page.content)

#the data is stored in the data-itnrw-coords attribute of the map HTML-Element
#splitting the data at each comma
data = str(tree.xpath('//div[@id="itnrwMap_0"]/@data-itnrw-coords')[0]).split('\n,')

dataExtracted = []
dataFixed = []
#removing the square brackets and splitting each data row at each semicolon
for x in data:
  dataExtracted.append(x.replace('[','').replace(']','').split(';'))
  
#only the measuring points operated by NRW should be gathered. filtering is being done by checking if the URL starts with 'h'.
#these URLs refer to an external site and are not operated by NRW and are being skiped
for x in dataExtracted:
  if (x[3][9]=='h'):
    continue
  dataFixed.append(x)
  
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#iterating through measuring points, extracting data and storing it in the database if an entry with the same name and time is
#not already in the database
for x in dataFixed:
  name = x[2].split('<br>')[0][1:]
  wlevel = float(x[2].split('<br>')[3].split(' ')[1])
  
  #fixing time format to YYYY-MM-DD HH:mm
  timeSplit = x[2].split('<br>')[2].split(' ')
  timeDate = timeSplit[0].split('.')
  time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1]
  
  warning = x[5]
  
  cur.execute("SELECT id FROM nw_data WHERE name = %s AND time = %s",(name,time))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO nw_data (name,wlevel,time,warning) VALUES(%s, %s, %s, %s)",(name,wlevel,time,warning))

  conn.commit()