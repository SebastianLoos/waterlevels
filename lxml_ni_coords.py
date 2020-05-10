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

  url = ''
  url = c["BetreiberURL"]
  
  name = c["Name"]
  body = c["GewaesserName"]
  x = c["WGS84Hochwert"]
  y = c["WGS84Rechtswert"]
  z = c["Hoehe"]
  
  number = c["STA_ID"]
  drainage = c["EinzugsgebietQKm_Text"].replace(' ','').replace('km²','').replace(',','.')
  warn1 = warn2 = warn3 = None
  try:  
    warn1 = c["Parameter"][0]["Datenspuren"][0]["Meldestufen"][0]["Wert"]
    warn2 = c["Parameter"][0]["Datenspuren"][0]["Meldestufen"][1]["Wert"]
    warn3 = c["Parameter"][0]["Datenspuren"][0]["Meldestufen"][2]["Wert"]
  except:
    print('no warning')
  
  if (c["Parameter"][0]["Datenspuren"][0]["ParameterName"]!="Wasserstand"):
    continue
  
  element = [name,x,y,z,url,number,body,drainage,warn1,warn2,warn3]
  data.append(element)
  print(element)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting measuring points already stored in the database
cur.execute("SELECT name FROM ni_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingData = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
print(mpNamesSQLList)
  
#getting indices of missing measuring points
for x in data:
  if (x[0] not in mpNamesSQLList):
    print(x[0])
    missingData.append(x)

#iterating through missing measuring points and storing them in the database
for c in missingData:
  
  cur.execute("INSERT INTO ni_dpoints (name,x,y,z,url,number,body,drainage,warn1,warn2,warn3) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4],c[5],c[6],c[7],c[8],c[9],c[10]))

  conn.commit()