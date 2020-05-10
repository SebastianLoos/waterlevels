# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2
import json

#downloading JSON-String from REST API
page = requests.get("http://www.wasserstaende.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true&includeCharacteristicValues=true")

#generating JSON object from string
rawdata = json.loads(page.content)

data = []
#iterate through the measuring points and extracting the data
for c in rawdata:
  print(c)

  name = c["longname"]
    
  number = c["number"]
  time = None
  wlevel = None
  warning = None
  try:
    wlevel = c["timeseries"][0]["currentMeasurement"]["value"]
    time = c["timeseries"][0]["currentMeasurement"]["timestamp"]
    warning = c["timeseries"][0]["currentMeasurement"]["stateMnwMhw"]
    timeSplit = time.split('T')
    timeTime = timeSplit[1].split('+')
    timeFixed = timeSplit[0]+' '+timeTime[0]
  except:
    continue
  
  
  element = [name,wlevel,timeFixed,warning,number]
  data.append(element)
  print(element)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#iterating through missing measuring points and storing them in the database if an entry with the same name and time is
#not already in the database
for c in data:
  print(c)
  
  cur.execute("SELECT id FROM po_data WHERE name = %s AND time = %s",(c[0],c[2]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO po_data (name,wlevel,time,warning,number) VALUES(%s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4]))

  conn.commit()