import parser
import random
import time
import urllib2
from bs4 import *
from pymongo import MongoClient
from urldata import UrlData
from concurrent import futures

HEADER_LIST = [
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62'
                      '.0.3202.94 Safari/537.36',
        'Cookie': '_hc.v="\"9bf0c497-6bcd-442c-8831-11af2b3775ac.1462454285\""; __mta=247773112.1492345219184.14924869'
                  '16777.1495705642466.3; _lxsdk_cuid=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; _lxsdk'
                  '=15f2eb60fedc8-0cf4c585118869-31657c03-fa000-15f2eb60fedc8; ctu=11adc716774af4c6141bdc2788044bfc8c6'
                  'a40ff0eced6ea23be6ee89eaf9a42; s_ViewType=10; __utma=1.1396579858.1463541809.1511494700.1511496495.'
                  '14; __utmz=1.1510497248.7.5.utmcsr=dianping.com|utmccn=(referral)|utmcmd=referral|utmcct=/beijing'
                  '/food; aburl=1; cy=2; cye=beijing; dper=9e48486875eae06ee78acead0909f6623cbe8a1b1dee503f34b2120d694'
                  'a54ae; ua=%E6%B5%85%E5%A4%8FJean; _lx_utm=utm_source%3Ddianping.com%26utm_medium%3Dreferral%26utm_c'
                  'ontent%3D%252Fbeijing%252Ffood; ll=7fd06e815b796be3df069dec7836c3df; _lxsdk_s=1607bf59217-af6-033-4'
                  'e2%7C%7C14',
    },
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/'
                      '11.0.2 Safari/604.4.7',
        'Cookie': '_lxsdk_s=1607bf5fe58-9b9-e3f-c23%7C%7C27; _lxsdk=1607bf5fe54c8-0b5f02a6b57a74-1c451b26-fa000-1607bf'
                  '5fe54c8; _lxsdk_cuid=1607bf5fe54c8-0b5f02a6b57a74-1c451b26-fa000-1607bf5fe54c8; cy=15072; cye=Otaru;'
                  ' __utma=1.1222946919.1462597685.1462597685.1511495987.2; __utmz=1.1511495987.2.1.utmcsr=(direct)|ut'
                  'mccn=(direct)|utmcmd=(none); ua=%E6%B5%85%E5%A4%8FJean; _hc.v="\"596cccc3-0737-42f5-8a30-c5521b5a6e'
                  '8a.1456192435\""'
    }
]

COLL_KEY = {
    "member": (1, "id"),
    "review": (1, "id"),
    "shop": (1, "id"),
    "unfinished": (1, "url"),
    "urllist": (1, "url"),
    "wishlist": (2, "member-id", "shop-id")
}

RETRY_MAX_TIMES = 5
RETRY_WAIT_TIME = 1
THREAD_WAIT_UPPER = 60
THREAD_WAIT_LOWER = 5
MAX_WORKER_NUM = 5
MAX_CRAWLING_NUM = 1000


class CrawlerClass:
    def __init__(self, db_name):
        self._con = MongoClient("localhost", 27017)
        self._db = self._con[db_name]
        self._ip_list = ["127.0.0.1"]
        self._ip_weights = [5]

    def get_entry(self, collection_name, **kv):
        collection = self._db[collection_name]
        return collection.find(kv)

    def get_entry_iter(self, collection_name, is_limited, **kv):
        res = self._db[collection_name].find(kv)
        count = 0
        for item in res:
            if is_limited and count >= MAX_CRAWLING_NUM:
                break
            # self._db[collection_name].remove(item)
            yield item["url"]

            count += 1

    def get_one_entry(self, collection_name, **kv):
        collection = self._db[collection_name]
        return collection.find_one(kv)

    def exists(self, collection_name, field, value):
        collection = self._db[collection_name]
        res = collection.find_one({field: value})

        return res is not None

    def insert_with_update(self, collection_name, json):
        collection = self._db[collection_name]
        query = {}
        for i in range(COLL_KEY[collection_name][0]):
            key = COLL_KEY[collection_name][i + 1]
            query[key] = json[key]
        collection.update(query, {'$set': json}, upsert=True)

    def crawl_page(self, page):
        # if ref != '':
        #   headers['Referer']=ref

        for i in range(RETRY_MAX_TIMES):
            req = urllib2.Request(url=page.url, headers=random.choice(HEADER_LIST))
            req.add_header('Host', 'www.dianping.com')

            # if page.ref != '':
            #   req.add_header('Referer',page.ref)

            ip_no, proxy_ip = self.pick_ip_randomly()
            # print "%d %d %s %s" % (i, ip_no, proxy_ip, page.url)

            try:
                if ip_no == 0:
                    c = urllib2.urlopen(req, timeout=10).read()
                else:
                    http_type = proxy_ip[0:proxy_ip.index(':')]
                    proxies = {http_type: proxy_ip}
                    proxy_s = urllib2.ProxyHandler(proxies)
                    opener = urllib2.build_opener(proxy_s)
                    c = opener.open(req, timeout=15).read()

                soup = BeautifulSoup(c, 'lxml')
                url_parser = parser.getparser(page, soup)
                crawled_data = url_parser.parse()
                links = url_parser.get_links()

                # Add weights for the ip we used
                self._ip_weights[ip_no] = self._ip_weights[ip_no] + 3
                if self._ip_weights[ip_no] > 100:
                    self._ip_weights[ip_no] = 100
                print "[Crawled][%s] %s #%d %s, weight: %d" %\
                      (page.collection, page.url, i+1, proxy_ip, self._ip_weights[ip_no])
                return crawled_data, links
            except Exception:
                self._ip_weights[ip_no] -= 5
                if self._ip_weights[ip_no] < 1:
                    self._ip_weights[ip_no] = 1

                if i < RETRY_MAX_TIMES - 1:
                    time.sleep(RETRY_WAIT_TIME)
                    continue

                raise

    def crawl(self, url):
        page = UrlData(url)
        if self.exists("urllist", "url", url):
            print "[Already Crawled] %s" % url
            self._db["unfinished"].remove({"url": url})
            return

        try:
            crawled_data, links = self.crawl_page(page)

            # Insert
            for data in crawled_data:
                self.insert_with_update(page.collection, data)
                if page.collection == "review":
                    db_content = self.get_one_entry("review", **data)
                    if "got" not in db_content:
                        self._db["review"].update(data, {"$set": {"got": False}}, upsert=True)

            # Next Links
            for link in links:
                if self.exists("urllist", "url", link.url) or self.exists("unfinished", "url", link.url):
                    continue
                self._db["unfinished"].insert({"url": link.url})
            self.done_crawl(page)

        except Exception, ex:
            self._db["unfinished"].remove({"url": url})
            self._db["unfinished"].insert({"url": url, "ref": page.ref})

            print "[Exception][%s] %s: %s" % (page.collection, url, ex)

        finally:
            time.sleep(random.randint(THREAD_WAIT_LOWER, THREAD_WAIT_UPPER))

    def pick_ip_randomly(self):
        x = random.randint(0, sum(self._ip_weights)-1)
        accumulation_prob, ip, i = 0, 0, 0
        for ip, ipPr in zip(self._ip_list, self._ip_weights):
            accumulation_prob += ipPr
            if x < accumulation_prob:
                break
            i += 1
        return i, str(ip)

    def update_ip_list(self):
        ip_url = 'http://www.xicidaili.com/nn/'
        req = urllib2.Request(url=ip_url, headers=random.choice(HEADER_LIST))
        res = urllib2.urlopen(req, timeout=20)
        soup = BeautifulSoup(res.read(), 'lxml')
        ips = soup.find_all('tr')

        for i in range(1, len(ips)):
            ip_info = ips[i]
            tds = ip_info.find_all('td')
            res = "%s://%s:%s" % (tds[5].text.lower(), tds[1].text, tds[2].text)
            self._ip_list.append(res)
            if ip_info.find(class_="bar_inner slow") is not None or ip_info.find(class_="bar_inner medium") is not None:
                self._ip_weights.append(1)
            else:
                self._ip_weights.append(4)

        print "Get proxy ip Done! total %d." % len(self._ip_list)

    def done_crawl(self, page):
        self._db["urllist"].insert({"url": page.url, "ref": page.ref})
        self._db["unfinished"].remove({"url": page.url})

    def setup(self, is_limited=False):
        start_time = time.time()
        with futures.ThreadPoolExecutor(MAX_WORKER_NUM) as pool:
            pool.map(self.crawl, self.get_entry_iter("unfinished", is_limited))
        print "Process finished, time consuming %f seconds." % (time.time()-start_time)


if __name__ == "__main__":
    obj = CrawlerClass("dzdp")
    obj.update_ip_list()
    obj.setup(True)
