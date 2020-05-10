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
page = requests.get("https://hnz.thueringen.de/hw2.0/thueringen.html")
tree = html.fromstring(page.content)

#getting names and URLs from the table of the website
mpURLs = tree.xpath('//table[@class="contentTablePage"]//tbody//tr//td[2]//a/@href')

mpURLsFixed = []

for x in mpURLs:
  if "pegelonline" in x:
    continue
  mpURLsFixed.append("https://hnz.thueringen.de/hw2.0/"+x)

data = []
    
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT url FROM th_dpoints")
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
  name = ''
  body = ''
  drainage = kilometers = None
  x = y = z = None
  number = ''
  
  subpage = requests.get(c)
  subtree = html.fromstring(subpage.content)
    
  name = subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[2]//td[2]/text()')[0]
  body = subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[3]//td[2]/text()')[0]
  ost = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[6]//td[2]/text()')[0])
  nord = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[7]//td[2]/text()')[0])
  
  #converting the coordinates
  proj_source = Proj(init="epsg:25832")
  proj_dest = Proj(init="epsg:4326")
  x,y = pyproj.transform(proj_source,proj_dest,ost,nord)
  
  number = int(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[1]//td[2]/text()')[0].split('.')[0])
  z = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[8]//td[2]/text()')[0].split('\xa0')[0].replace(',','.'))
  print(name)
  
  try:
    drainage = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[4]//td[2]/text()')[0].replace(',','.'))
  except:
    print('no drainage')
  
  try:
    kilometers = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[5]//td[2]/text()')[0].replace(',','.'))
  except:
    print('no kilometers')
  
  try:
    mb = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[8]//td[4]/text()')[0])
    a1 = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[9]//td[4]/text()')[0])
    a2 = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[10]//td[2]/text()')[0])
    a3 = float(subtree.xpath('//table[@id="tblStammdaten"]//tbody//tr[11]//td[2]/text()')[0])
  except:
    print('warning error')

  cur.execute("INSERT INTO th_dpoints (name,x,y,z,url,number,body,drainage,kilometers,mb,a1,a2,a3) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(name,x,y,z,c,number,body,drainage,kilometers,mb,a1,a2,a3))

  conn.commit()