from pymongo import MongoClient as mc
from bs4 import *
import urllib2

con = mc("localhost",27017)
ipset = con["dzdp"]["ip"]
url = 'http://www.xicidaili.com/nn/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
req = urllib2.Request(url, headers=headers)
res = urllib2.urlopen(req, timeout=20)
soup = BeautifulSoup(res.read(), 'lxml')
ips = soup.find_all('tr')

for i in range(1, len(ips)):
    ip_info = ips[i]
    if ip_info.find(class_="bar_inner slow") is not None or ip_info.find(class_="bar_inner medium") is not None:
        continue
    tds = ip_info.find_all('td')

    res = tds[1].text+':'+tds[2].text
    if ipset.find_one({"ip": res}) is None:
        ipset.insert({"ip": res,"weight":1})