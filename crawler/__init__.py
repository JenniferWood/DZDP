from crawler_future import CrawlerClass

MAX_CRAWLING_NUM = 500
CONF_FILE = 'param.conf'


def load_param(conf_file):
    print "Loading param..."
    p = {}
    with open(conf_file, 'r') as f_open:
        for line in f_open:
            k_v = line.split(' ')
            p[k_v[0]] = int(k_v[1])
            print line
    return p


def setup(db_name, is_limited=False):
    obj = CrawlerClass(db_name)
    obj.__dict__.update(load_param(CONF_FILE))
    obj.update_ip_list()
    obj.set_skip_collections(review=60, member=90, shop=50, wishlist=10)
    obj.setup(MAX_CRAWLING_NUM, is_limited)


if __name__ == "__main__":
    setup("dzdp")
