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
import csv


data = []

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT name FROM nw_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])

#parsing csv file and extracting data and storing it in the database
with open('nrw_pegel_stationen.txt') as csvfile:
  datareader = csv.reader(csvfile, delimiter=';', quotechar='|')
  #the first line is a header line, to fix the numbering, it starts at -1
  number = -1
  for c in datareader:
    name = body = ''
    mnw = mhw = mw = inf1 = inf2 = inf3 = ost = nord = drainage = kilometers = None
    x = y = z = None
    number=number+1
    try:
      name = c[0]
      body = c[1]
      ost = float(c[9])
      nord = float(c[10])
      print(name)
    except:
      print('error')
      
    if (name=="Name") or (name in mpNamesSQLList):
      continue
    
    #converting coordinates
    try:
      proj_source = Proj(init="epsg:25832")
      proj_dest = Proj(init="epsg:4326")
      x,y = pyproj.transform(proj_source,proj_dest,ost,nord)
      print(x)
      print(y)
    except:
      print('transform error')
      
    try:
      mnw = float(c[2])
      mhw = float(c[3])
      mw = float(c[4])
    except:
      print('stammdaten error')
      
    try:
      inf1 = float(c[5])
      inf2 = float(c[6])
      inf3 = float(c[7])
    except:
      print('warnung error')
      
    url = "http://luadb.it.nrw.de/LUA/hygon/pegel.php?stationsinfo=ja&stationsname="+name
    page = requests.get(url)
    tree = html.fromstring(page.content)
    
    try:
      z = float(tree.xpath('//table[1]//tr[3]//td[2]/text()')[0].replace('\xa0','').replace(',','.'))
      print(z)
    except:
      print('no height')
    
    try:
      kilometers = float(tree.xpath('//table[1]//tr[4]//td[2]/text()')[0].replace('\xa0','').replace(',','.'))
    except:
      print('no kilometers')
    
    try:
      drainage = float(tree.xpath('//table[1]//tr[5]//td[2]/text()')[0].replace('\xa0','').replace(',','.'))
    except:
      print('no drainage')
    
    cur.execute("INSERT INTO nw_dpoints (name,x,y,z,url,number,body,drainage,kilometers,mnw,mw,mhw,inf1,inf2,inf3) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(name,x,y,z,url,number,body,drainage,kilometers,mnw,mw,mhw,inf1,inf2,inf3))

    conn.commit()