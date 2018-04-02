# -*- coding: utf-8 -*-

from pymongo import errors
from db import MyMongoDb
from crawler import parser
import math

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
    url_list = [page_fm % item["id"] for item in DAO.get_all("review", **{"create-time": {"$exists": False}})]
    url_list += [page_fm % item["id"] for item in DAO.get_all("review", **{"create-time": re.compile("^1.*")})]

    obj = CrawlerClass("dzdp", "../conf/param_process.conf")
    obj.update_proxy_list(pages=5)
    obj.main(url_list, True)


def modify_time_format():
    import re
    cursor = DAO.get_all("review", **{"create-time": re.compile("^2.*")})
    print "[%d]" % cursor.count()
    for review in cursor:
        update = {}
        create_time = review["create-time"]
        try:
            if u"更新于" in create_time:
                create_time = create_time.split(u"更新于")[0].strip()
                if create_time.count("-") == 3:
                    create_time = create_time[create_time.index("-")+1:]
                else:
                    raise Exception

            update["create-time"] = parser.ParserFactory.supplement_time_format(create_time)
            if "update-time" in review:
                update["update-time"] = parser.ParserFactory.supplement_time_format(review["update-time"])

            DAO.update("review", review, update)
        except Exception, e:
            print review["id"], review["create-time"]
            print e


def modify_contri_format():
    for member in DAO.get_all("member", **{"contri-value":{"$type":2}}):
        update = {}
        try:
            update["contri-value"] = int(member["contri-value"])
            DAO.update("member", member, update)
        except Exception, e:
            print member["id"], member["contri-value"]
            print e


def complement_review_star():
    for review in DAO.get_all("review", **{"star":{"$exists": False}}):
        if "score" in review and review["score"]:
            valid_scores = list(filter(lambda x: x>0, review["score"]))
            if valid_scores:
                update = {"star": float(sum(valid_scores))/ len(valid_scores)}
                DAO.update("review", review, update)
                print review["id"], update["star"]


if __name__ == "__main__":
    complete_review_create_time()

    '''
    rt_list = []
    with open('/Users/Jean/Desktop/PT.csv', 'r') as pt:
        for line in pt:
            pts = map(float, line.strip().split(','))
            rts = [math.log(pts[t]/pts[t-1]) for t in range(1, len(pts))]
            rt_list.append(rts)

    with open('/Users/Jean/Desktop/kg_test_data.csv', 'w') as data_file:
        with open('/Users/Jean/Desktop/AT.csv', 'r') as at:
            i = 0
            for line in at:
                ats = map(float, line.strip().split(','))
                new_line_list = ["%f,%f" % (rt_list[i][j], ats[j]) for j in range(len(ats))]
                data_file.write("%s\n" % (" ".join(new_line_list)))
                i += 1
    '''