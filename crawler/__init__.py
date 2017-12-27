from crawler_future import CrawlerClass

MAX_CRAWLING_NUM = 400


def setup(db_name, is_limited=False):
    obj = CrawlerClass(db_name)
    obj.update_ip_list()
    obj.set_skip_collections(review=50, member=90, shop=50, wishlist=10)
    obj.setup(MAX_CRAWLING_NUM, is_limited)


if __name__ == "__main__":
    setup("dzdp", True)
