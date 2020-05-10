# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:20:46 2019

@author: Hayuki
"""

import requests
import psycopg2
import xml.etree.ElementTree as ET
from lxml import html
from pyproj import Proj
import pyproj

#downloading XML and generating tree
page = requests.get("https://isk.geobasis-bb.de/Pegelkarte.xml")
root = ET.fromstring(page.content)

print(root)

data = []

#the measuring points are listed with the item tag. Running the commands to extract the data for each point
#name of the measuring point, id, name of the body of water and coordinates can be extracted from the xml
for item in root.findall('item'):
  url = ''
  url = item.find('link').text
  
  name = item.find('title').text.split('/')[0]
  body = item.find('title').text.split('/')[1]
  namespaces = {'dc' : 'http://purl.org/dc/elements/1.1/', 'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 'georss': 'http://www.georss.org/georss', 'gml': 'http://www.opengis.net/gml'}
  ost = float(item.findall('./georss:where/gml:Point/gml:pos',namespaces)[0].text.split(' ')[0].replace(',','.'))
  nord = float(item.findall('./georss:where/gml:Point/gml:pos',namespaces)[0].text.split(' ')[1].replace(',','.'))
  #converting the coordinates
  proj_source = Proj(init="epsg:25833")
  proj_dest = Proj(init="epsg:4326")
  x,y = pyproj.transform(proj_source,proj_dest,nord,ost)
  
  number = item.find('link').text.split('=')[1]
  
  element = [name,x,y,url,number,body]
  data.append(element)
  print(name)
  print(x)
  print(y)
  
#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting measuring points already stored in the database
cur.execute("SELECT name FROM bb_dpoints")
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

#iterating through missing measuring points, crawling the rest of the data not contained in the xml from the website.
#missing data is the size of drainage area, the amount of kilometers the measuring point is located from the source of 
#the river (if the body of water is a river), the height, the characteristic values of the measuring point and the warning levels
for c in missingData:
  drainage = kilometers = z = nw = mnw = mw = mhw = hw = warn1 = warn2 = warn3 = warn4 = None
  
  #downloading website of the measuring point
  subpage = requests.get(c[3])
  subtree = html.fromstring(subpage.content)
  
  #extracting data
  try:
    drainage = float(subtree.xpath('//table[@title="Pegelstammdaten"]//tr[position()=2]//td[position()=4]/text()')[0].replace('\r\n','').replace(' ','').replace(',','.').replace(u'\xa0km',''))    
  except:
    print('drainage error')
    
  try:
    z = float(subtree.xpath('//table[@title="Pegelstammdaten"]//tr[position()=1]//td[position()=6]/text()')[0].replace('\r\n','').replace(' ','').replace(',','.').replace('müNHN',''))
  except:
    print('height error')
    
  try:
    kilometers = float(subtree.xpath('//table[@title="Pegelstammdaten"]//tr[position()=2]//td[position()=6]/text()')[0].replace('\r\n','').replace(' ','').replace(',','.').replace('Fluss-km','').replace('kmohMdg',''))
  except:
    print('kilometers error')
    
  try:
    nw = float(subtree.xpath('//table[@title="Hauptwerte"]//tr[position()=4]//td[position()=3]/text()')[0])
    mnw = float(subtree.xpath('//table[@title="Hauptwerte"]//tr[position()=5]//td[position()=3]/text()')[0])
    mw = float(subtree.xpath('//table[@title="Hauptwerte"]//tr[position()=6]//td[position()=3]/text()')[0])
    mhw = float(subtree.xpath('//table[@title="Hauptwerte"]//tr[position()=7]//td[position()=3]/text()')[0])
    hw = float(subtree.xpath('//table[@title="Hauptwerte"]//tr[position()=8]//td[position()=3]/text()')[0])
  except:
    print('hauptwerte error')
    
  try:
    warn1 = float(subtree.xpath('//table[@title="Alarmstufen"]//tr[position()=2]//td[position()=2]/text()')[0].replace('\r\n','').replace(' ',''))
    warn2 = float(subtree.xpath('//table[@title="Alarmstufen"]//tr[position()=3]//td[position()=2]/text()')[0].replace('\r\n','').replace(' ',''))
    warn3 = float(subtree.xpath('//table[@title="Alarmstufen"]//tr[position()=4]//td[position()=2]/text()')[0].replace('\r\n','').replace(' ',''))
    warn4 = float(subtree.xpath('//table[@title="Alarmstufen"]//tr[position()=5]//td[position()=2]/text()')[0].replace('\r\n','').replace(' ',''))
  except:
    print('warnstufen error')
    
  print(c[0])
  print(z)
  print(kilometers)
  
  #insert into database
  cur.execute("INSERT INTO bb_dpoints (name,x,y,z,url,number,body,drainage,kilometers,nw,mnw,mw,mhw,hw,warn1,warn2,warn3,warn4) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],z,c[3],c[4],c[5],drainage,kilometers,nw,mnw,mw,mhw,hw,warn1,warn2,warn3,warn4))

  conn.commit()