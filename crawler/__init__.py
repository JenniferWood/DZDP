from crawler_future import CrawlerClass

MAX_CRAWLING_NUM = 500
CONF_FILE = '../conf/param_crawler.conf'


def setup(db_name, is_limited=False):
    obj = CrawlerClass(db_name, CONF_FILE)
    obj.update_proxy_list()
    obj.set_skip_collections(review=20, member=90)
    if is_limited:
        obj.setup(MAX_CRAWLING_NUM)
    else:
        obj.setup()


if __name__ == "__main__":
    setup("dzdp")
