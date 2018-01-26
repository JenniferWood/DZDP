import re

COLLECTION_MAP = {
    u"shop_": "shop",
    u"member_": "member",
    u"review_": "review",
    u"list_": "list",
    u"shop_review_all": "review",
    u"member_reviews": "review",
    u"member_wishlists": "wishlist"
    }


class UrlData:
    def __init__(self, url, ref='', **kv):
        self.url = url.strip('/')
        self.url = self.url.split('#')[0]
        self.type = ''
        self.collection = ''
        self.id = ''
        self.ref = ref
        self.suffix = None

        if len(kv) > 0:
            self.__dict__.update(kv)
        else:
            type_info = UrlData.classify_url(url)
            if type_info is not None:
                self.type = type_info["urltype"]
                self.id = type_info["id_"]

                suff = type_info["suffix"]
                suffixes = suff.split('?')
                supp = suffixes[0]
                self.collection = COLLECTION_MAP.get(self.type+"_"+supp, "")

                if len(suffixes) > 1:
                    self.suffix = suffixes[1]

                if self.type == "member" and (len(suffixes) < 2 or suffixes[1] == ''):
                    if self.collection == "review":
                        self.url += "?reviewCityId=2&reviewShopType=10"
                    elif self.collection == "wishlist":
                        self.url += "?favorTag=s10_c2_t-1"

    @staticmethod
    def classify_url(url):
        pattern = re.compile(r'http://www.dianping.com/(?P<urltype>\w+)/(?P<id_>\d+)/?(?P<suffix>[^/]*)$')
        res = re.search(pattern, url)
        if res is None:
            return None
        return res.groupdict()
