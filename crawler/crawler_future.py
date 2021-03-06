import parser
import random
import time
import urllib2
import threading
import socket
from bs4 import *
from urldata import UrlData
from concurrent import futures
from db import mongo

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
    },
    {'User-Agent': 'Mozilla/5.0 (Macintosh;Intel Mac OS X 10.6;rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
    {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8;U;en) Presto/2.8.131 Version/11.11'}
]

COLL_KEY = {
    "member": (1, "id"),
    "review": (1, "id"),
    "shop": (1, "id"),
    "unfinished": (1, "url"),
    "urllist": (1, "url"),
    "wishlist": (2, "member-id", "shop-id")
}

COLL_REVIEW = "review"
COLL_UNFINISHED = "unfinished"
COLL_URL_LIST = "urllist"


class CrawlerClass:

    crawl_num = 0
    success_num = 0

    def __init__(self, db_name, conf_file=None):
        self._dao = mongo.MyMongoDb(db_name)
        self._ip_list = ["127.0.0.1"]
        self._ip_weights = [5]
        self.skip = {}

        # default
        self.retry_max_times = 5
        self.retry_wait_time = 1
        self.thread_wait_upper = 300
        self.thread_wait_lower = 60
        self.max_worker_num = 5

        if conf_file:
            self.__dict__.update(self.load_param(conf_file))

    @staticmethod
    def load_param(conf_file):
        print "Loading param..."
        p = {}
        with open(conf_file, 'r') as f_open:
            for line in f_open:
                line = line.strip()
                k_v = line.split(' ')
                p[k_v[0]] = int(k_v[1])
                print line
        return p

    def whether_to_skip(self, page_collection):
        if page_collection not in self.skip:
            return False

        _ = random.randint(0, 100)
        return _ <= self.skip[page_collection]

    def change_ip_weight(self, ip_no, add=True):
        if add:
            self._ip_weights[ip_no] = min(100, self._ip_weights[ip_no] + 3)
        else:
            self._ip_weights[ip_no] = max(1, self._ip_weights[ip_no] - 5)

    def crawl_page(self, page):
        # if ref != '':
        #   headers['Referer']=ref

        for i in range(self.retry_max_times):
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

                self.change_ip_weight(ip_no)
                return crawled_data, links
            except Exception:
                self.change_ip_weight(ip_no, False)

                if i < self.retry_max_times - 1:
                    time.sleep(self.retry_wait_time)
                    continue

                raise

    def crawl(self, url, ignore_exists):
        if not url.startswith('http://'):
            self._dao.update("unfinished", {"url": url}, {"url": "http://"+url}, False)
            url = "http://"+url

        CrawlerClass.crawl_num += 1
        if CrawlerClass.crawl_num % 10 == 0:
            print "==============crawl_num: %d success_num: %d==============" % \
                  (CrawlerClass.crawl_num, CrawlerClass.success_num)

        page = UrlData(url)

        if not ignore_exists and self._dao.exists(COLL_URL_LIST, url=url):
            print "[Already Crawled] %s" % url
            self._dao.remove(COLL_UNFINISHED, url=url)
            CrawlerClass.success_num += 1
            return

        if self.whether_to_skip(page.collection):
            self._dao.move_to_last(COLL_UNFINISHED, url=url)
            # print "Crawl [%s] %s later..." % (page.collection, url)
            return

        try:
            crawled_data, links = self.crawl_page(page)

            # Insert
            with open('./newly_review_ids', 'a') as fopen:
                for data in crawled_data:
                    self._dao.insert_with_update(page.collection, data)
                    if page.collection == "review":
                        fopen.write("%s\n" % data["id"])
                    if page.collection in ["wishlist", "review"]:
                        for coll in ["member", "shop"]:
                            self._dao.update(coll, {"id": data["%s-id" % coll]}, {"item2vec": False}, upsert=False)

            # Next Links
            for link in links:
                if self._dao.exists(COLL_URL_LIST, url=link.url) or self._dao.exists(COLL_UNFINISHED, url=link.url):
                    continue
                self._dao.insert(COLL_UNFINISHED, url=link.url)

            self.done_crawl(page)
            CrawlerClass.success_num += 1
            print "[%s][Crawled][%s] %s" % (threading.currentThread().getName(), page.collection, page.url)

        except (urllib2.URLError, urllib2.HTTPError, socket.error):
            self._dao.move_to_last(COLL_UNFINISHED, url=url)
            # print "[%s][Exception][%s] %s: %s" % (threading.currentThread().getName(), page.collection, url, ex)
        except (ValueError, AttributeError, Exception), ex:
            self._dao.move_to_last(COLL_UNFINISHED, url=url)
            print "[%s] %s: %s" % (threading.currentThread().getName(), url, ex)
        finally:
            time_slot = random.randint(self.thread_wait_lower, self.thread_wait_upper)
            print "[%s][%d]" % (threading.currentThread().getName(), time_slot)
            time.sleep(time_slot)

    def pick_ip_randomly(self):
        x = random.randint(0, sum(self._ip_weights)-1)
        accumulation_prob, ip, i = 0, 0, 0
        for ip, ipPr in zip(self._ip_list, self._ip_weights):
            accumulation_prob += ipPr
            if x < accumulation_prob:
                break
            i += 1
        return i, str(ip)

    def update_proxy_list(self, pages=1):
        for p in range(pages):
            ip_url = 'http://www.xicidaili.com/nn/%d' % (p+1)
            req = urllib2.Request(url=ip_url, headers=random.choice(HEADER_LIST))
            res = urllib2.urlopen(req, timeout=20)
            soup = BeautifulSoup(res.read(), 'lxml')
            ips = soup.find_all('tr')

            for i in range(1, len(ips)):
                ip_info = ips[i]
                tds = ip_info.find_all('td')
                res = "%s://%s:%s" % (tds[5].text.lower(), tds[1].text, tds[2].text)
                self._ip_list.append(res)
                if ip_info.find(class_="bar_inner slow") or ip_info.find(class_="bar_inner medium"):
                    self._ip_weights.append(1)
                else:
                    self._ip_weights.append(5)

        print "Get proxy ip Done! Totally %d." % len(self._ip_list)

    def set_skip_collections(self, **cp):
        for collection, prob in cp.iteritems():
            if type(prob) is not int or prob < 0 or prob > 100:
                raise ValueError("Skip probability illegal: [{0} {1}]".format(collection, prob))
            self.skip[collection] = prob
            print "Set skip prob: %s %d%%" % (collection, prob)

    def done_crawl(self, page):
        self._dao.insert(COLL_URL_LIST, url=page.url, ref=page.ref)
        self._dao.remove(COLL_UNFINISHED, url=page.url)

    def main(self, url_list, ignore_exists=False):
        if isinstance(url_list, list):
            print "We got %d urls to crawl." % len(url_list)

        start_time = time.time()
        with futures.ThreadPoolExecutor(self.max_worker_num) as pool:
            for url in url_list:
                pool.submit(self.crawl, url, ignore_exists)

            # pool.map(self.crawl, url_list)
        print "Process finished, time consuming %f seconds." % (time.time()-start_time)

    def setup(self, max_crawling_num=None):
        self.main(self._dao.get_iter(COLL_UNFINISHED, max_crawling_num))
