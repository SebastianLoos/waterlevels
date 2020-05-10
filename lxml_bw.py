# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2

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

#iterating through the strings, extracting the data and storing it in the database
for c in dataFixed:
  wlevel = None
  try:
    wlevel = float(c[4])
  except:
    continue
  
  flow = None
  try:
    flow = float(c[9])
  except:
    print('no flow')
    
  print(c)
  
  #fixing time format to YYYY-MM-DD HH:mm
  timeSplit = c[7].split(' ')
  timeDate = timeSplit[0].split('.')
  time = timeDate[2]+'-'+timeDate[1]+'-'+timeDate[0]+' '+timeSplit[1]
  print(time)
  
  cur.execute("SELECT id FROM bw_data WHERE name = %s AND time = %s",(c[1],time))
  indata = cur.fetchall()
  print(indata)
  if (len(indata)!=0):
    continue
  
  cur.execute("INSERT INTO bw_data (name,wlevel,flow,time,number,warning) VALUES(%s, %s, %s, %s, %s, %s)",(c[1],wlevel,flow,time,c[0],c[14]))

  conn.commit()