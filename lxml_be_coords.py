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
page = requests.get("https://wasserportal.berlin.de/start.php?anzeige=tabelle")
tree = html.fromstring(page.content)

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()

#getting URLs, name  and type of the measuring point, name of the body of water, id, kilometers from the source and coordinates from
#the website
mpURLs = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 2]//a/@href')
mpNames = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 2]//text()')
mpType = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 4]//text()')
mpBody = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 5]//text()')
mpNumber = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 1]//text()')
mpKilometers = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 6]/text()')
mpRechts = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 8]//text()')
mpHoch = tree.xpath('//table[@id="pegeltab"]//tbody//tr//td[position() = 9]//text()')

#print(mpNames)

##getting measuring points already stored in the database
cur.execute("SELECT name FROM be_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingNames = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
#print(mpNamesSQLList)
  
#getting indices of missing measuring points
for x in range(len(mpNames)):
  if (mpNames[x] not in mpNamesSQLList):
    #print(mpNames[x])
    missingNames.append(x)
    
#iterating through missing measuring points, crawling missing data and storing it in the database
#missing data includes the height and characteristic values of the measuring point
data = []
for c in missingNames:
  x = y = 0
  z = None
  number = 0
  drainage = kilometers = None
  url = ''
  NW = MNW = MW = MHW = HW = None
  
  #only continue crawling if the measuring point is a water level measuring point
  if 'Wasserstand' not in mpType[c]:
    continue
  
  print(mpNames[c])
  
  #if the measuring point is operated by Brandenburg, skip to the next to avoid overlap of data. URLs to the Brandenburg-Website
  #can be identified by the term "pegeldaten" in the URL
  if 'pegeldaten' in mpURLs[c]:
    continue
  
  #if the measuring point is operated by Pegelonline, use the following code to extract data
  if 'pegelonline' in mpURLs[c]:
    urlFixed = mpURLs[c]
    urlFixed2 = 'https://www.pegelonline.wsv.de/gast/stammdaten?pegelnr='+mpNumber[c]
    
    subpage = requests.get(urlFixed2)
    subtree = html.fromstring(subpage.content)
    
    x = mpRechts[c]
    y = mpHoch[c]
    
    try:
      height = subtree.xpath('//table[@summary="Allgemeine Stammdaten"]//tr[position() = 8]//td[position() = 2]/text()')[0].replace(',','.')
      z = height;
    except:
      print('height error')
    
    try:
      kilometers = subtree.xpath('//table[@summary="Allgemeine Stammdaten"]//tr[position() = 5]//td[position() = 2]/text()')[0].replace(' ','').replace('\r\n','').replace('km','').replace(',','.')
    except:
      print('kilometers error')
      
    print(height)
    print(kilometers)
    
    MNW = subtree.xpath('//table[@summary="Kennzeichnende Wasserstände"]//tr[position() = 2]//td[position() = 2]/text()')[0].replace(' ','').replace('\r\n','').replace('cm','')
    MW = subtree.xpath('//table[@summary="Kennzeichnende Wasserstände"]//tr[position() = 3]//td[position() = 2]/text()')[0].replace(' ','').replace('\r\n','').replace('cm','')
    MHW = subtree.xpath('//table[@summary="Kennzeichnende Wasserstände"]//tr[position() = 4]//td[position() = 2]/text()')[0].replace(' ','').replace('\r\n','').replace('cm','')
    
  #if the measuring point is operated by Berlinm use the following code
  #URLs to the Berlin-Website can be identified by the term "station.php" in the URL
  if 'station.php' in mpURLs[c]:
    urlFixed = 'https://wasserportal.berlin.de/'+mpURLs[c]
    urlFixed2 = urlFixed.replace('anzeige=i','anzeige=k')
    print(urlFixed)
    
    subpage = requests.get(urlFixed)
    subtree = html.fromstring(subpage.content)
  
    #converting coordinates
    proj_source = Proj(init="epsg:32633")
    proj_dest = Proj(init="epsg:4326")
    x,y = pyproj.transform(proj_source,proj_dest,mpRechts[c],mpHoch[c])
    
    print(x)
    print(y)
    
    try:
      height = subtree.xpath('//table[@summary="Pegel Berlin"]//tbody//tr[position() = 7]//td[position() = 2]/text()')
      print(height[0])
      z = height[0]
    except:
      print('height error')
    
    try:
      kilometers = subtree.xpath('//table[@summary="Pegel Berlin"]//tbody//tr[position() = 6]//td[position() = 2]/text()')[0]
    except:
      print('kilometers error');
    
    subpage2 = requests.get(urlFixed2)
    subtree2 = html.fromstring(subpage2.content)
    
    try:
      NW = subtree2.xpath('//table[@summary="Hauptwerte Wasserstand Berlin"]//tbody//tr[position() = 1]//td[position() = 2]/text()')[0]
      MNW = subtree2.xpath('//table[@summary="Hauptwerte Wasserstand Berlin"]//tbody//tr[position() = 2]//td[position() = 2]/text()')[0]
      MW = subtree2.xpath('//table[@summary="Hauptwerte Wasserstand Berlin"]//tbody//tr[position() = 3]//td[position() = 2]/text()')[0]
      MHW = subtree2.xpath('//table[@summary="Hauptwerte Wasserstand Berlin"]//tbody//tr[position() = 4]//td[position() = 2]/text()')[0]
      HW = subtree2.xpath('//table[@summary="Hauptwerte Wasserstand Berlin"]//tbody//tr[position() = 5]//td[position() = 2]/text()')[0]
    except:
      print('kennwerte error')
    
  print(NW)
  print(MNW)
  print(MW)
  print(MHW)
  print(HW)

  
  cur.execute("INSERT INTO be_dpoints (name,x,y,z,url,number,body,drainage,kilometers,nw,mnw,mw,mhw,hw) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(mpNames[c],x,y,z,urlFixed,mpNumber[c],mpBody[c],drainage,kilometers,NW,MNW,MW,MHW,HW))

  conn.commit()