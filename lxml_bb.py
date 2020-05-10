# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2
import xml.etree.ElementTree as ET

#downloading XML-file and generating tree
page = requests.get("https://isk.geobasis-bb.de/Pegelkarte.xml")
root = ET.fromstring(page.content)

print(root)

data = []
#the measuring points are listed with the item tag. Running the commands to extract the data for each point
for item in root.findall('item'):
  #reset the variables
  name = ''
  number = None
  wlevel = None
  flow = None
  time = None
  warning = None
  
  #
  name = item.find('title').text.split('/')[0]
  number = item.find('link').text.split('=')[1]
  
  try:
    wlevel = float(item.findall('./content/Wasserstand')[0].text.split(' ')[0])
  except:
    continue

  try:
    timeSplit = item.findall('./content/Messzeitpunkt')[0].text.split(' ')
    timeDate = timeSplit[0].split('.')
    time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1]
    
  except:
    continue
  
  warning = item.find('iconclr').text
  element = [name,wlevel,flow,time,number,warning]
  data.append(element)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#storing the data in the database if there is no entry with the same name and time already in the database
for c in data:
  print(c)
  cur.execute("SELECT id FROM bb_data WHERE name = %s AND time = %s",(c[0],c[3]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO bb_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4],c[5]))

  conn.commit()