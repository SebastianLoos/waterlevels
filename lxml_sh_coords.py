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

#downloading website and generating tree
page = requests.get("https://www.umweltdaten.landsh.de/public/hsi/pegelliste.html")
tree = html.fromstring(page.content)

#getting URLs from the table of the website
mpURLs = tree.xpath('//td[@class="tdcell--verlaufw"]//a/@href')

mpURLsFixed = []

#skip all points from pegel-online to avoid data overlap
for x in mpURLs:
  if "pegelonline" in x:
    continue
  mpURLsFixed.append("https://www.umweltdaten.landsh.de"+x)

data = []
    
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT url FROM sh_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingData = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
  
##getting indices of missing measuring points
for x in mpURLsFixed:
  if (x not in mpNamesSQLList):
    print(x)
    missingData.append(x)
    
#iterating through missing measuring points, crawling data and storing it in the database
for c in missingData:
  table = '2'
  name = ''
  body = ''
  drainage = kilometers = None
  x = y = z = None
  number = ''
  
  subpage = requests.get(c)
  subtree = html.fromstring(subpage.content)
  
  #the location of the table containing the data depends on the amount of tables contained on the page, the default is the second
  #table (see parameters above), but if the amount of tables is only 1, this (the first) table is the correct one
  if (len(subtree.xpath('//table[@class="info"]'))==1):
    table = '1'
    
  name = subtree.xpath('//table[@class="info"]['+table+']//tr[1]//td[2]//strong/text()')[0].split('\xa0 - \xa0')[0].replace('\xa0','')
  print(name)
  
  try:
    number = subtree.xpath('//table[@class="info"]['+table+']//tr[1]//td[2]//strong/text()')[0].split('\xa0 - \xa0')[1]
    body = subtree.xpath('//table[@class="info"]['+table+']//tr[2]//td[2]//strong/text()')[0].replace('\xa0','')
    print(number)
  except:
    print('no number')
    continue
  
  try:
    mnw = float(subtree.xpath('//table[@class="info"][1]//tr[td[1]/text()="W"]//td[5]/text()')[0])
    mw = float(subtree.xpath('//table[@class="info"][1]//tr[td[1]/text()="W"]//td[6]/text()')[0])
    mhw = float(subtree.xpath('//table[@class="info"][1]//tr[td[1]/text()="W"]//td[7]/text()')[0])
    print(mnw)
    print(mw)
    print(mhw)
  except:
    print('no warning level')
  
  try:
    coordinates = subtree.xpath('//table[@class="info"]['+table+']//tr[7]//td[2]/text()')[0].replace('\xa0','').replace(' ','')
    ost = float(coordinates.split('/')[0])
    nord = float(coordinates.split('/')[1])
    
    #converting coordinates
    proj_source = Proj(init="epsg:4647")
    proj_dest = Proj(init="epsg:4326")
    y,x = pyproj.transform(proj_source,proj_dest,ost,nord)
    
    print(x)
    print(y)
    
    z=float(subtree.xpath('//table[@class="info"]['+table+']//tr[10]//td[2]/text()')[0].replace('\xa0','').split(' ')[0].replace(',','.').replace('m',''))
  except:
    print('coordinates not found')
    continue

  try:
    drainage=float(subtree.xpath('//table[@class="info"]['+table+']//tr[9]//td[2]/text()')[0].replace('\xa0','').replace(',','.'))
  except:
    print('no drainage')
  
  try:
    kilometers=float(subtree.xpath('//table[@class="info"]['+table+']//tr[8]//td[2]/text()')[0].replace('\xa0','').replace(',','.'))
  except:
    print('no km')
  
  element = [name,x,y,z,c,number,body,drainage,kilometers,mnw,mw,mhw]
  data.append(element)

#storing all data in the database
for c in data:
  cur.execute("INSERT INTO sh_dpoints (name,x,y,z,url,number,body,drainage,kilometers,mnw,mw,mhw) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4],c[5],c[6],c[7],c[8],c[9],c[10],c[11]))

  conn.commit()

  cur.execute("")