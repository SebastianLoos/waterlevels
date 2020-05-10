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

#downloading website and generating tree
page = requests.get("https://pegelportal-mv.de/pegel-mv/pegel_list.html")

tree = html.fromstring(page.content)


#getting PDF Links and URLs from the table of the website
mpURLs = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV"]//td[position() = 8]//a/@href')
mpURLs2 = tree.xpath('//table[@id="pegeltab"]//tbody//tr[td[position() = 9]//text()="Land MV"]//td[position() = 7]//a[@class="verlaufw"]/@href')

mpURLsFixed = []
mpURLs2Fixed = []

for x in mpURLs:
  mpURLsFixed.append("https://pegelportal-mv.de/pegel-mv/"+x) 
  
for x in mpURLs2:
  mpURLs2Fixed.append("https://pegelportal-mv.de/pegel-mv/"+x)

data = []

#establishing conntection to the sql server
conn = psycopg2.connect("host=localhost port=5432 dbname=pegelstaende user=postgres password=project")
cur = conn.cursor()


#getting measuring points already stored in the database
cur.execute("SELECT url FROM mv_dpoints")
mpNamesSQL = []
mpNamesSQL = cur.fetchall()
mpNamesSQLList = []
missingData = []

for x in mpNamesSQL:
  mpNamesSQLList.append(x[0])
print(mpNamesSQLList)
  
#getting indices of missing measuring points
for x in mpURLsFixed:
  if (x not in mpNamesSQLList):
    print(x)
    missingData.append(x)
    
#iterating through missing measuring points, downloading and parsing the PDFs for data
#data in the PDFs: name, body of water, id, height,coordinates
for c in range(len(mpURLsFixed)):
  name = ''
  body = ''
  drainage = None
  x = y = z = None
  number = ''
  
  #downloading PDF
  print(mpURLsFixed[c])
  pdf = requests.get(mpURLsFixed[c])
  
  #saving PDF to file
  with open('temp.pdf', 'wb') as f:
    f.write(pdf.content)
  
  #parsing the PDF file
  raw = parser.from_file('temp.pdf')
  
  #try to split the content of the PDF into individual lines, if the PDF failed to parse this will throw an exception and the
  #script will continue with the next PDF
  try:
    lines = raw["content"].splitlines()
  except:
    continue
  
  for x in lines:
    if (x!=''):
      linedata = x.split(' ')
      if (linedata[0]=="Steckbrief"):
        for y in range(len(linedata)):
          if (y>2):
            name+=linedata[y]
            name+=" "
      if (linedata[0]=="Pegelkennzahl"):
        number = linedata[1].split('.')[0]
      if (linedata[0]=="Gewässer"):
        for y in range(len(linedata)):
          if (y!=0):
            body+=linedata[y]
            body+=" "
      if (linedata[0]=="Pegelnull"):
        z = linedata[3]
      if (linedata[0]=="Rechtswert"):
        ost = linedata[2]
      if (linedata[0]=="Hochwert"):
        nord = linedata[2]
      if (linedata[0]=="EZG-Fläche"):
        try:
          drainage = linedata[3]
        except:
          print('no drainage')
  print(name)
  name=name[:-1]
  
  
  body=body[:-1]
  
  #converting coordinates
  proj_source = Proj(init="epsg:5650")
  proj_dest = Proj(init="epsg:4326")
  z=z.replace(',','.')
  x,y = pyproj.transform(proj_source,proj_dest,ost,nord)
  
  #adding the data to an array
  element = [name,x,y,z,mpURLsFixed[c],number,body,drainage]
  data.append(element)
  
#gathering additional data (characteristic values) from the website
for c in range(len(mpURLs2Fixed)):
  subpage = requests.get(mpURLs2Fixed[c])
  subtree = html.fromstring(subpage.content)
  
  hhw = mhw = mw = mnw = nw = None
  
  hhw = subtree.xpath('//span[@class="hhwline"]/text()')[0].split(': ')[1]
  mhw = subtree.xpath('//span[@class="hwline"]/text()')[0].split(': ')[1]
  mw = subtree.xpath('//span[@class="mwline"]/text()')[0].split(': ')[1]
  mnw = subtree.xpath('//span[@class="mnwline"]/text()')[0].split(': ')[1]
  nw = subtree.xpath('//span[@class="nwline"]/text()')[0].split(': ')[1]
  
  element = [nw,mnw,mw,mhw,hhw]
  
  subname = subtree.xpath('//h1/text()')[0].split('-')[2].split('.')[0][1:]
  
  #finding the data from the PDF for the current measuring point and adding the additional data to the array
  for i in data:
    if (i[5]==subname):
      print(i[0]+" found")
      for j in element:
        i.append(j)
        
#iterating thorugh the array and storing it in thedatabase if complete data is complete (array has been extended)
for c in data:
  if (len(c)!=13):
    print(c[0]+" no")
    continue;
  cur.execute("INSERT INTO mv_dpoints (name,x,y,z,url,number,body,drainage,nw,mnw,mw,mhw,hhw) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(c[0],c[1],c[2],c[3],c[4],c[5],c[6],c[7],c[8],c[9],c[10],c[11],c[12]))

  conn.commit()