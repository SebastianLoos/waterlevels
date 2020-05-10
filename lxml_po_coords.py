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

#iterate through the measuring points and extracting the data
data = []
for c in rawdata:
  print(c)

  url = ''
  url = 'https://www.pegelonline.wsv.de/gast/stammdaten?pegelnr='+c["number"]
  z = None
  
  name = c["longname"]
  body = c["water"]["longname"]
  try:
    x = c["longitude"]
    y = c["latitude"]
  except:
    print('no coordinates. skip')
    continue
  
  try:
    z = c["timeseries"][0]["gaugeZero"]["value"]
  except:
    print('no height')
  
  kilometer = None
  try:
    kilometers = c["km"]
  except:
    print('no kilometers')
    
  number = c["number"]
  MW = MHW = MNW = None
  try:  
    values = c["timeseries"][0]["characteristicValues"]
    for a in values:
      try:
        if (a["shortname"]=='MW'):
          MW = a["value"]
      except:
        print('no MW')
      try:
        if (a["shortname"]=='MHW'):
          MHW = a["value"]
      except:
        print('no MHW')
      try:
        if (a["shortname"]=='MNW'):
          MNW = a["value"]
      except:
        print('no MNW')
  except:
    print('no warning')

  
  element = [name,x,y,z,url,number,body,kilometers,MW,MHW,MNW]
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
  
  print(c)
  
  cur.execute("INSERT INTO po_dpoints (name,x,y,z,url,number,body,kilometers,mw,mhw,mnw) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4],c[5],c[6],c[7],c[8],c[9],c[10]))

  conn.commit()