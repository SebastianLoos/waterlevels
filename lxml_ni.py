# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2
import json

#downloading JSON-String from API
page = requests.get("https://bis.azure-api.net/PegelonlinePublic/REST/stammdaten/stationen/All?key=9dc05f4e3b4a43a9988d747825b39f43")

#generate JSON object from string
rawdata = json.loads(page.content)

data = []
#iterate through the measuring points (getStammDatenResult-Array) and extracting the data
for c in rawdata["getStammdatenResult"]:
  if (c["Parameter"][0]["Datenspuren"][0]["ParameterName"]!="Wasserstand"):
    continue
  name = c["Name"]
  number = c["STA_ID"]
  try:
    wlevel = c["Parameter"][0]["Datenspuren"][0]["AktuellerMesswert"]
  except:
    continue
  flow = None
  try:
    timeSplit = c["Parameter"][0]["Datenspuren"][0]["AktuellerMesswert_Zeitpunkt"].split(' ')
    timeDate = timeSplit[0].split('.')
    time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1]
  except:
    continue
  warning = c["Parameter"][0]["Datenspuren"][0]["Farbe"]
  
  element = [name,wlevel,flow,time,warning,number]
  data.append(element)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()
  

#iterating through missing measuring points, crawling data and storing them in the database if an entry with the same name and time is
#not already in the database
for c in data:
  cur.execute("SELECT id FROM ni_data WHERE name = %s AND time = %s",(c[0],c[3]))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  cur.execute("INSERT INTO ni_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[5],c[4]))

  conn.commit()