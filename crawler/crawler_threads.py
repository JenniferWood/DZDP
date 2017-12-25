import parser
import random
import threading
import time
import urllib2
from Queue import Queue

from bs4 import *
from pymongo import MongoClient as mc

from urldata import UrlData

userAgentList = ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38',
            #'Mozilla/5.0 (Macintosh;Intel Mac OS X 10.6;rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
            #'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8;U;en) Presto/2.8.131 Version/11.11',
            ]

cookieList = ['_hc.v="9bf0c497-6bcd-442c-8831-11af2b3775ac.1462454285"; __mta=247773112.1492345219184.1492486916777.1495705642466.3; _lxsdk_cuid=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; _lxsdk=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; dper=9e48486875eae06ee78acead0909f66257cb95a64498ef3113ec261cb8c40844; ua=%E6%B5%85%E5%A4%8FJean; ctu=11adc716774af4c6141bdc2788044bfc8c6a40ff0eced6ea23be6ee89eaf9a42; __utma=1.1396579858.1463541809.1510497248.1510503354.8; __utmz=1.1510497248.7.5.utmcsr=dianping.com|utmccn=(referral)|utmcmd=referral|utmcct=/beijing/food; s_ViewType=10; aburl=1; cy=2; cye=beijing; _lx_utm=utm_source%3Ddianping.com%26utm_medium%3Dreferral%26utm_content%3D%252Fbeijing%252Ffood; ll=7fd06e815b796be3df069dec7836c3df; _lxsdk_s=15fdc6e8a45-f92-d56-6d4%7C%7C14',
            '__utma=1.1222946919.1462597685.1462597685.1462597685.1; _hc.v="596cccc3-0737-42f5-8a30-c5521b5a6e8a.1456192435"; _lxsdk=15fb5376285c8-0dbcd74b8b19bd-3e636f4c-fa000-15fb5376285b3; _lxsdk_cuid=15fb5376285c8-0dbcd74b8b19bd-3e636f4c-fa000-15fb5376285b3; _lxsdk_s=15fdc713407-18f-17d-5a8%7C%7C26; _tr.u=hffRueLti9XjTuY2; aburl=1; ctu=11adc716774af4c6141bdc2788044bfc182f9556fc3052fa9ca3faaea419b5ae; cy=2; cye=beijing; dper=9e48486875eae06ee78acead0909f662d619e60b7a4323140197ebbbcbafa4d1; ll=7fd06e815b796be3df069dec7836c3df; s_ViewType=10; ua=%E6%B5%85%E5%A4%8FJean;']

headersList = [{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36','Cookie':'_hc.v="9bf0c497-6bcd-442c-8831-11af2b3775ac.1462454285"; __mta=247773112.1492345219184.1492486916777.1495705642466.3; _lxsdk_cuid=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; _lxsdk=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; dper=9e48486875eae06ee78acead0909f66257cb95a64498ef3113ec261cb8c40844; ua=%E6%B5%85%E5%A4%8FJean; ctu=11adc716774af4c6141bdc2788044bfc8c6a40ff0eced6ea23be6ee89eaf9a42; __utma=1.1396579858.1463541809.1510497248.1510503354.8; __utmz=1.1510497248.7.5.utmcsr=dianping.com|utmccn=(referral)|utmcmd=referral|utmcct=/beijing/food; s_ViewType=10; aburl=1; cy=2; cye=beijing; _lx_utm=utm_source%3Ddianping.com%26utm_medium%3Dreferral%26utm_content%3D%252Fbeijing%252Ffood; ll=7fd06e815b796be3df069dec7836c3df; _lxsdk_s=15fdc6e8a45-f92-d56-6d4%7C%7C14'},
            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38','Cookie':'__utma=1.1222946919.1462597685.1462597685.1462597685.1; _hc.v="596cccc3-0737-42f5-8a30-c5521b5a6e8a.1456192435"; _lxsdk=15fb5376285c8-0dbcd74b8b19bd-3e636f4c-fa000-15fb5376285b3; _lxsdk_cuid=15fb5376285c8-0dbcd74b8b19bd-3e636f4c-fa000-15fb5376285b3; _lxsdk_s=15fdc713407-18f-17d-5a8%7C%7C26; _tr.u=hffRueLti9XjTuY2; aburl=1; ctu=11adc716774af4c6141bdc2788044bfc182f9556fc3052fa9ca3faaea419b5ae; cy=2; cye=beijing; dper=9e48486875eae06ee78acead0909f662d619e60b7a4323140197ebbbcbafa4d1; ll=7fd06e815b796be3df069dec7836c3df; s_ViewType=10; ua=%E6%B5%85%E5%A4%8FJean;'}]

regexes = {"shop":"shop[/][0-9]+$","review_m":"member[/][0-9]+[/]reviews","review_s":"shop[/][0-9]+[/]review_all","wishlist":"wishlist","others":".+"}

succWaitTime = 10
failWaitTime = 5
loopWaitTime = 5
retryWaitTime = 1
NOT_FINISHED = True
QUANTITY_EACH_TIME = 10

_insert = 0
_update = 1


class CrawlerClass:
    def __init__(self,dbname):
        self.con = mc("localhost",27017)
        self.db = self.con[dbname]
        self.ipList = ["127.0.0.1"]
        self.ipWeights = [10]

        shopQueue = Queue()
        memberReviewQueue = Queue()
        shopReviewQueue = Queue()
        wishlistQueue = Queue()
        otherQueue = Queue()

        self.queues = {
            "shop" : shopQueue,
            "review_m" : memberReviewQueue,
            "review_s" : shopReviewQueue,
            "wishlist" : wishlistQueue,
            "others" : otherQueue
        }

    def get_entry(self,collectionName,**kv):
        collection = self.db[collectionName]
        return collection.find(kv)

    def get_one_entry(self,collectionName,**kv):
        collection = self.db[collectionName]
        return collection.find_one(kv)

    def remove_entry(self,collectionName,kv):
        self.db[collectionName].remove(kv)

    def exists(self,collectionName,field,value):
        collection = self.db[collectionName]

        res = collection.find_one({field:value})

        if res == None:
            return False
        else:
            return True

    def insert_with_update(self,collectionName,dic,key):
        collection = self.db[collectionName]
        if not self.exists(collectionName,key,dic[key]):
            collection.insert(dic)
            return _insert
        else:
            collection.update({key:dic[key]},{'$set':dic},upsert=True)
            return _update

    def crawl_page(self,page):
        Max_Num = 5
        #if ref != '':
            #headers['Referer']=ref

        for i in range(Max_Num):
            ipno = -1
            req = urllib2.Request(url=page.url,headers=random.choice(headersList))

            #if page.ref != '':
            #    req.add_header('Referer',page.ref)

            ipno,proxy_ip = self.pick_ip_randomly()

            try:
                if ipno == 0:
                    c=urllib2.urlopen(req,timeout=10).read()
                else:
                    http_type = proxy_ip[0:proxy_ip.index(':')]
                    proxies = {http_type: proxy_ip}
                    proxy_s=urllib2.ProxyHandler(proxies)
                    opener=urllib2.build_opener(proxy_s)
                    c = opener.open(req,timeout=15).read()

                #headers['Referer'] = url
                soup = BeautifulSoup(c,'lxml')
                urlparser = parser.getparser(page, soup)
                crawledData = urlparser.parse()
                links = urlparser.get_links()

                # Add weights for the ip we used
                self.ipWeights[ipno] = self.ipWeights[ipno]+3
                if self.ipWeights[ipno] > 100: self.ipWeights[ipno] = 100
                #print "[Crawled][%s] %s #%d %s, weight: %d"%(page.collection,page.url, i+1, proxy_ip, self.ipWeights[ipno])
                #self.db["ip"].update({"ip":proxy_ip},{"$set":{"weight":self.ipWeights[ipno]}})

                return crawledData,links
            except:
                self.ipWeights[ipno] -= 5
                if self.ipWeights[ipno] < 1: self.ipWeights[ipno] = 1
                #self.db["ip"].update({"ip":proxy_ip},{"$set":{"weight":self.ipWeights[ipno]}})

                if i < Max_Num -1:
                    time.sleep(retryWaitTime)
                    continue

                raise

    def crawl(self,_type):
        crawl_queue = self.queues.get(_type)

        loop = 0
        succ_count = 0
        try_count = 0
        while NOT_FINISHED:
            self.add_to_queue(_type, QUANTITY_EACH_TIME)
            loop += 1

            while not crawl_queue.empty():
                if not NOT_FINISHED: break
                print "[%s] %d, loop %d, try_count %d, succ_count %d" % (_type,crawl_queue.qsize(),loop,try_count,succ_count)

                page = crawl_queue.get()
                if self.exists("urllist","url",page.url): continue

                '''
                if _type == "review_s":
                    if not self.exists("shop","id",page.id):
                        self.db["unfinished"].insert({"url":page.url,"ref":page.ref})
                        continue
                if _type == "review_m" or _type == "wishlist":
                    if not self.exists("member","id",page.id):
                        self.db["unfinished"].insert({"url":page.url,"ref":page.ref})
                        continue
                '''

                try_count += 1

                try:
                    crawled_data,links = self.crawl_page(page)
                except Exception,ex:
                    print "[Exception][%s] %s: %s" % (_type,page.url,ex)
                    self.db["unfinished"].insert({"url":page.url,"ref":page.ref})
                    time.sleep(random.randint(0, failWaitTime))
                    continue

                #insert
                if page.collection == "wishlist":
                    self.db["wishlist"].update({"member-id":crawled_data["member-id"]},{"$pushAll":{"wishlist":crawled_data["wishlist"]}},upsert = True)
                else:
                    for data in crawled_data:
                        res = self.insert_with_update(page.collection,data,"id")
                        if page.collection == "review" and res == _insert:
                            self.db["review"].update(data,{"$set":{"got":False}},upsert =True)

                #update queue
                for link in links:
                    if self.exists("urllist","url",link.url) or self.exists("unfinished","url",link.url): continue
                    self.db["unfinished"].insert({"url":link.url})

                self.db["urllist"].insert({"url":page.url,"ref":page.ref})
                succ_count += 1
                time.sleep(random.randint(0,succWaitTime))

            time.sleep(loopWaitTime)

        self.queue_to_db(_type)
        print "-----------------------------\n[Finish Thread] %s\n-----------------------------\n" % _type

    def pick_ip_randomly(self):
        x = random.randint(0,sum(self.ipWeights)-1)
        cumulProb = 0
        i = 0
        for ip, ipPr in zip(self.ipList,self.ipWeights):
            cumulProb += ipPr
            if x < cumulProb: break
            i += 1
        return i,str(ip)

    def update_ip_list(self):
        ip_url = 'http://www.xicidaili.com/nn/'
        req = urllib2.Request(url=ip_url, headers=random.choice(headersList))
        res = urllib2.urlopen(req, timeout=20)
        soup = BeautifulSoup(res.read(), 'lxml')
        ips = soup.find_all('tr')

        for i in range(1, len(ips)):
            ip_info = ips[i]
            if ip_info.find(class_="bar_inner slow") is not None or ip_info.find(class_="bar_inner medium") is not None:
                continue
            tds = ip_info.find_all('td')
            res = "%s://%s:%s" % (tds[5].text.lower(), tds[1].text, tds[2].text)
            self.ipList.append(res)
            self.ipWeights.append(1)

    def add_to_queue(self,qType,num=1):
        for i in range(num):
            res = self.get_one_entry("unfinished",url={"$regex":regexes[qType]})
            if res is None: continue
            self.remove_entry("unfinished",res)

            if self.exists("urllist","url",res["url"]):continue

            inData = UrlData(res["url"])

            if "ref" in res:
                inData.ref = res["ref"]
            self.queues.get(qType).put(inData)
        print qType,self.queues.get(qType).qsize()

    def queue_to_db(self, _type):
        Q = self.queues[_type]
        while not Q.empty():
            p = Q.get()
            if not self.exists("unfinished", "url", p.url):
                self.db["unfinished"].insert({"url": p.url, "ref": p.ref})
        print "Clear %s Done" % _type

    def finish(self):
        for q in self.queues:
            self.queue_to_db(q)

    def crawl_by_type(self,_type):
        crawlThread = threading.Thread(target=self.crawl,args=(_type,))
        #crawlThread.setDaemon(True)
        crawlThread.start()

    def setup(self):
        for qType in regexes:
            self.crawl_by_type(qType)