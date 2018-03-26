from db import MyMongoDb
import numpy as np
import pandas as pd

DAO = MyMongoDb("dzdp")
NEW_COLUMNS = ["avr-flavor", "avr-env", "avr-service", "avr-star", "avr-pay"]


def extract_category_feature(collection_name, feature):
    category_statistic = [s for s in DAO.count_column_num(collection_name, feature) if s["_id"]]
    category_statistic.sort(key=lambda x: x["num"], reverse=True)
    category_statistic.insert(0, {"_id": None})

    i = 0
    for category_num in category_statistic:
        category_num["name"] = category_num["_id"]
        category_num["_id"] = i
        i += 1

    DAO.insert_many(feature, category_statistic)


def update_avr(shop_id):
    update = {}
    shop_review_cur = DAO.get_all("review", **{"shop-id": shop_id})
    update["review-num"] = shop_review_cur.count()
    if update["review-num"] == 0:
        print "No review."
        return

    score_matrix = []
    for r in shop_review_cur:
        if "score" not in r or not r["score"]:
            score = [0.0, 0.0, 0.0]
        else:
            score = r["score"]
        score.append(r["star"] if "star" in r else 0.0)
        score.append(r["pay"] if "pay" in r else 0.0)
        score_matrix.append(score)
    score_data = pd.DataFrame(score_matrix)
    score_data[score_data == 0.0] = np.nan
    mean = score_data.mean()

    for i in range(0, len(NEW_COLUMNS)):
        update[NEW_COLUMNS[i]] = mean[i]

    DAO.update("shop", {"id": shop_id}, update, upsert=False, multi=False)

    print "shop id %s" % shop_id
    print update


def update_shop_scale(shop_name):
    update = {
        "num": len(DAO.get_all("shop", name=shop_name).distinct("full-name"))
    }

    DAO.update("branch", {"name": shop_name}, update)

    print "shop name %s" % shop_name
    print update, "\n"


def do_work(limit=None):
    i = 0
    for shop in DAO.get_and_limit("shop", limit=limit):
        shop_id = shop["id"]
        shop_name = shop["name"]

        print "[%d]" % i
        i += 1
        if "avr-pay" not in shop:
            update_avr(shop_id)
        if not DAO.exists("branch", name=shop_name):
            update_shop_scale(shop_name)


if __name__ == "__main__":
    updated_shops = set()
    with open("./newly_review_ids", "r") as fopen:
        i = 1
        for line in fopen:
            review_id = line.strip()
            shop_id = DAO.get_one("review", id=review_id)["shop-id"]
            print i
            i += 1
            if shop_id not in updated_shops:
                update_avr(shop_id)
                updated_shops.add(shop_id)