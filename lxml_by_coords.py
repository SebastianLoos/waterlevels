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
page = requests.get("https://www.hnd.bayern.de/pegel/meldestufen//tabellen")
tree = html.fromstring(page.content)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting names and URLs and body of water from the table of the website
mpNames = tree.xpath('//tr[@class="row" or @class="row2"]//td[position() = 1]//a/text()')
mpURLs = tree.xpath('//tr[@class="row" or @class="row2"]//td[position() = 1]//a/@href')
mpBody = tree.xpath('//tr[@class="row" or @class="row2"]//td[position() = 2]//text()')

print(mpBody)


#getting measuring points already stored in the database
cur.execute("SELECT name FROM by_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingNames = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
print(mpNamesSQLList)
  
#getting indices of missing measuring points
for x in range(len(mpNames)):
  if (mpNames[x] not in mpNamesSQLList):
    print(mpNames[x])
    missingNames.append(x)
    
#iterating through missing measuring points, crawling data and storing it in the database
data = []
for c in missingNames:
  x = y = None
  z = None
  number = None
  drainage = kilometers = None
  url = ''
  
  print(mpNames[c])
  y = mpURLs[c].replace('/abfluss','')
  y+= '/stammdaten'
  url = y;
  print(y)
  subpage = requests.get(y)
  subtree = html.fromstring(subpage.content)
  
  hoch = subtree.xpath('//tr[td = "Hochwert (Gauss-Krüger):"]//td[position()=2]//text()')
  rechts = subtree.xpath('//tr[td[contains(text(),"Rechtswert")]]//td[position()=2]//text()')
  rechtsGK = rechts[0][:-2]
  hochGK = hoch[0][:-2]
  
  #conversion of coordiantes
  proj_31467 = Proj(init="epsg:31468")
  proj_4326 = Proj(init="epsg:4326")
  x,y = pyproj.transform(proj_31467,proj_4326,rechtsGK,hochGK)
  
  try:
    height = subtree.xpath('//tr[td = "Pegelnullpunktshöhe:"]//td[position()=2]//text()')
    heightSplit = height[0].split(' m')
    z = heightSplit[0].replace('.','').replace(',','.')
    if (z=='--'):
      z = None
  except:
    print('height error')
    
  number = int(subtree.xpath('//tr[td = "Messstellen-Nr.:"]//td[position()=2]//text()')[0])
  try:
    drainage = float(subtree.xpath('//tr[td = "Einzugsgebiet:"]//td[position()=2]//text()')[0][:-4].replace('.','').replace(',','.'))
    kilometers = float(subtree.xpath('//tr[td = "Flußkilometer:"]//td[position()=2]//text()')[0][:-3].replace('.','').replace(',','.'))
  except:
    print('data error')
    
  w0 = w1 = w2 = w3 = w4 = None
  
  try:
    w0 = float(subtree.xpath('//tr[td = "Meldebeginn:"]//td[position()=2]//text()')[0][:-3].replace(',','.'))
    w1 = float(subtree.xpath('//tr[td = "Meldestufe 1:"]//td[position()=2]//text()')[0][:-3].replace(',','.'))
    w2 = float(subtree.xpath('//tr[td = "Meldestufe 2:"]//td[position()=2]//text()')[0][:-3].replace(',','.'))
    w3 = float(subtree.xpath('//tr[td = "Meldestufe 3:"]//td[position()=2]//text()')[0][:-3].replace(',','.'))
    w4 = float(subtree.xpath('//tr[td = "Meldestufe 4:"]//td[position()=2]//text()')[0][:-3].replace(',','.'))
  except:
    print('Meldestufen error')
  
  print(number)
  print(x)
  print(y)
  print(z)
  print(drainage)
  print(kilometers)
  
  cur.execute("INSERT INTO by_dpoints (name,x,y,z,url,number,drainage,kilometers,warn0,warn1,warn2,warn3,warn4,body) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(mpNames[c],x,y,z,url,number,drainage,kilometers,w0,w1,w2,w3,w4,mpBody[c]))

  conn.commit()