# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2
from pyproj import Proj
import pyproj

#downloading javascript
page = requests.get("https://www.hvz.baden-wuerttemberg.de/js/hvz-data-peg-db.js")

#fix encoding
pagestring = page.content.decode("utf-8")
print(pagestring)

#split the code into single lines
lines = pagestring.split('\r\n')
print(lines)

data = []
#if the length of a line is longer than 100, it can be considered as a line containing water level information
#it's then split by commas and added to the data array
for x in lines:
  if (len(x)>100):
    data.append(x[1:-2].split(','))
    
dataFixed = []
#fixing the datastrings by removing apostrophes
for x in data:
  rowFixed = []
  for y in x:
    rowFixed.append(y.replace("'",""))
  dataFixed.append(rowFixed)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT name FROM bw_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingData = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
print(mpNamesSQLList)
  
#getting indices of missing measuring points
for x in dataFixed:
  if (x[1] not in mpNamesSQLList):
    print(x[1])
    missingData.append(x)
    
#iterating through missing measuring points, crawling data and storing it in the database.
#data crawled includes the coordinates, id, drainage area, kilometers from source, URL, height, characterstic values of 
#the measuring point and warning levels
for c in missingData:
  
  x = y = None
  z = None
  number = None
  drainage = kilometers = None
  url = ''
  
  print(c[1])
  
  url = 'https://www.hvz.baden-wuerttemberg.de/pegel.html?id='+c[0];
  print(url)
  ost = c[22][1:]
  nord = c[23]
  
  #converting coordinates
  proj_source = Proj(init="epsg:32632")
  proj_dest = Proj(init="epsg:4326")
  x,y = pyproj.transform(proj_source,proj_dest,ost,nord)
  
  try:
    z = float(c[26])
    drainage = float(c[25])
    kilometers = float(c[28])
    number = int(c[0])
  except:
    print('float conversion error')
    
  body = c[2]
    
  lo = me = w2 = w10 = w20 = w50 = w100 = None
  
  try:
    lo = float(c[43])
  except:
    print('-')
  try:
    me = float(c[40])
  except:
    print('-')
  try:
    w2 = float(c[30])
    w10 = float(c[31])
    w20 = float(c[32])
    w50 = float(c[33])
    w100 = float(c[34])
  except:
    print('-')
  
  cur.execute("INSERT INTO bw_dpoints (name,x,y,z,url,number,body,drainage,kilometers,low,norm,high2,high10,high20,high50,high100) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[1],x,y,z,url,number,body,drainage,kilometers,lo,me,w2,w10,w20,w50,w100))

  conn.commit()