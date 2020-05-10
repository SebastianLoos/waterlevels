# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

from lxml import html
import requests
import psycopg2
from pyproj import Proj
import pyproj
from tika import parser
import time
import json

#downloading website and generating tree
page = requests.get("https://www.hlnug.de/static/pegel/wiskiweb2/data/stationdata.json?v=20200118132821")
rawdata = json.loads(page.text)
features = rawdata["features"]

data = []
    
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT name FROM he_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingData = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
    
##iterating through missing measuring points, crawling data and storing it in the database
for c in features:
  name = body = url =  ''
  m1 = m2 = m3 = drainage = kilometers = None
  x = y = z = Number = None
  
  name = c["attributes"]["station_name"]
  if name in mpNamesSQLList:
    continue
  print(name)
  number = int(c["attributes"]["station_no"])
  print(number)
  try:
    x = float(c["attributes"]["station_longitude"])
    y = float(c["attributes"]["station_latitude"])
  except:
    ost = float(c["attributes"]["station_carteasting"])
    nord = float(c["attributes"]["station_cartnorthing"])
    proj_source = Proj(init="epsg:31467")
    proj_dest = Proj(init="epsg:4326")
    x,y = pyproj.transform(proj_source,proj_dest,ost,nord)
  
  try:
    body = c["attributes"]["river_name"]
  except:
    print('no body')
  
  url = "http://www.hlnug.de/static/pegel/wiskiweb2/stations/"+str(number)+"/station.html"
  
  subpage = requests.get("http://www.hlnug.de/static/pegel/wiskiweb2/stations/"+str(number)+"/station.html")
  subtree = html.fromstring(subpage.content)
  try:
    z = float(subtree.xpath("//table[@class='tblMetadata2']//tr[11]/td[2]/text()")[0])
  except:
    try:
      z = float(subtree.xpath("//table[@class='tblMetadata2']//tr[11]/td[2]/text()")[0])
    except:
      print("height error")
      
  try:
    m1 = float(subtree.xpath("//tr[@id='MS1']//td[2]//script/text()")[0].split('"')[1])
  except:
    print('no m1')
  
  try:
    m2 = float(subtree.xpath("//tr[@id='MS2']//td[2]//script/text()")[0].split('"')[1])
  except:
    try:
      m2 = float(subtree.xpath("//tr[@id='HW10']//td[2]//script/text()")[0].split('"')[1])
    except:
      print('no m2')
  
  try:
    m3 = float(subtree.xpath("//tr[@id='MS3']//td[2]//script/text()")[0].split('"')[1])
  except:
    try:
      m2 = float(subtree.xpath("//tr[@id='HW100']//td[2]//script/text()")[0].split('"')[1])
    except:
      print('no m2')
  try:
    drainage = float(subtree.xpath("//table[@class='tblMetadata2']//tr[9]/td[2]/text()")[0].replace(',','.').split(' ')[0])
  except:
    print("no drainage")
  try:
    kilometers = float(subtree.xpath("//table[@class='tblMetadata2']//tr[10]/td[2]/text()")[0].replace(',','.').split(' ')[0])
  except:
    print("no kilometers")
    
  cur.execute("INSERT INTO he_dpoints (name,x,y,z,url,number,body,drainage,kilometers,m1,m2,m3) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(name,x,y,z,url,number,body,drainage,kilometers,m1,m2,m3))

  conn.commit()