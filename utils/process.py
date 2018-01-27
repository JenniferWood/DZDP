from pymongo import errors
from db import MyMongoDb

DAO = MyMongoDb("dzdp")


def process_one():
    count, last_count = 0, 0
    while True:
        try:
            for item in DAO.get_all("wishlist_f"):
                count += 1
                if count % 10 == 0:
                    print count
                member_id = item["member-id"]
                wish_list = item["wishlist"]
                for shop in wish_list:
                    json = {"member-id": member_id, "shop-id": shop}
                    if DAO.exists_by_key("wishlist", json):
                        continue
                    DAO.insert("wishlist", **json)
                DAO.remove("wishlist_f", **item)
        except errors.CursorNotFound, ex:
            print ex
        finally:
            print count
            if count == last_count:
                break
            last_count = count


def complete_review_create_time():
    from crawler import CrawlerClass
    import re

    page_fm = "http://www.dianping.com/review/%s"
    url_list = [page_fm % item["id"] for item in DAO.get_all("review", **{"create-time": re.compile("^1.*")})]

    obj = CrawlerClass("dzdp", "../conf/param_crawler.conf")
    obj.update_ip_list()
    obj.main(url_list)


if __name__ == "__main__":
    complete_review_create_time()