import math
import time
import random
import datetime
from train import DataLoader
from db import mongo
from gensim.models import word2vec

MODEL_DIR_SRC = '../models/embedding'
MODEL_DIR_TARGET = '../models/dnn/embedding'
DAO = mongo.MyMongoDb("dzdp")
CATEGORICAL_FEATURE_DIMS = [2, 36, 18, 2, 3]

DISTINCT_SHOP = DAO.get_all("review").distinct("shop-id")

model_member = word2vec.Word2Vec.load("%s/model_member" % MODEL_DIR_TARGET)
model_shop = word2vec.Word2Vec.load("%s/model_shop" % MODEL_DIR_TARGET)


def transfer_timestamp(origin_time):
    time_type = type(origin_time)
    if time_type is str or time_type is unicode:
        time_format = "%Y-%m-%d"
        if len(origin_time) > 10:
            time_format = "%Y-%m-%d %H:%M"

        time_tuple = time.strptime(origin_time, time_format)
    elif time_type is datetime.datetime:
        time_tuple = origin_time.timetuple()
    else:
        raise ValueError("Wrong format for time.")

    timestamp = time.mktime(time_tuple)
    return timestamp


def min_max_scale(value, min_val, max_val):
    assert min_val < max_val
    if not value or math.isnan(value):
        return random.random()
    if value >= max_val:
        return 1.0
    if value <= min_val:
        return 0.0

    return float(value - min_val) / (max_val - min_val)


def binary_search(stamp_list, q):
    if not stamp_list:
        return []

    s, e = 0, len(stamp_list)-1
    while s <= e:
        mid = s + (e-s)/2
        if stamp_list[mid][0] <= q:
            s = mid + 1
        else:
            e = mid - 1

    return list(set(map(lambda x: x[1], stamp_list[0:s])))


class DataGenerator:
    def __init__(self):
        self.shop_conti_features = ["avr-flavor", "avr-env", "avr-service", "avr-star", "avr-pay", "review-num"]
        self.threshold = {
            "avr-pay": (0, 2000),
            "lat": (39.0, 42.0),
            "lon": (115.0, 118.0)
        }

        now = time.time()
        self._min_max_list = []
        self._min_max_list.append((
            now - transfer_timestamp(DAO.get_max_or_min("review", "create-time", True, **{"create-time":{"$type":9}})["create-time"]),
            now - transfer_timestamp(DAO.get_max_or_min("review", "create-time", False, **{"create-time":{"$type":9}})["create-time"])))
        self._min_max_list.append((
            DAO.get_max_or_min("branch", "num", False)["num"],
            DAO.get_max_or_min("branch", "num", True)["num"]))
        self._min_max_list.append((
            max(self.threshold["lat"][0], DAO.get_max_or_min("shop", "coordinate.0", False)["coordinate"][0]),
            min(self.threshold["lat"][1], DAO.get_max_or_min("shop", "coordinate.0", True)["coordinate"][0])))
        self._min_max_list.append((
            max(self.threshold["lon"][0], DAO.get_max_or_min("shop", "coordinate.1", False)["coordinate"][1]),
            min(self.threshold["lon"][1], DAO.get_max_or_min("shop", "coordinate.1", True)["coordinate"][1])))

        for shop_key in self.shop_conti_features:
            _min = DAO.get_max_or_min("shop", shop_key, False, **{shop_key: {"$ne": float('nan')}})[shop_key]
            _max = DAO.get_max_or_min("shop", shop_key, True, **{shop_key: {"$ne": float('nan')}})[shop_key]
            if shop_key in self.threshold:
                _min = max(_min, self.threshold[shop_key][0])
                _max = min(_max, self.threshold[shop_key][1])
            self._min_max_list.append((_min, _max))

        self._min_max_list.append((
            DAO.get_max_or_min("member", "contri-value", False)["contri-value"],
            DAO.get_max_or_min("member", "contri-value", True)["contri-value"]))
        self._min_max_list.append((
            transfer_timestamp(DAO.get_max_or_min("member", "register-date", False)["register-date"]),
            transfer_timestamp(DAO.get_max_or_min("member", "register-date", True)["register-date"])))

    def _deal_continuous_data(self, continuous):
        for i in range(len(continuous)):
            continuous[i] = min_max_scale(continuous[i], *self._min_max_list[i])

    def _train_reader(self, is_infer):
        def reader():
            with DAO.get_all("review", **{"create-time":{"$type":9}}) as cursor:
                member_met_shops = {}
                for item in cursor:
                    try:
                        member_id, shop_id = item["member-id"].encode('utf-8'), item["shop-id"].encode('utf-8')

                        if member_id not in model_member or shop_id not in model_shop\
                                or "star" not in item:
                            continue

                        score = item["star"]

                        shop_info = DAO.get_one("shop", id=shop_id)
                        user_info = DAO.get_one("member", id=member_id)

                        if not shop_info or not user_info:
                            continue

                        # continuous features
                        create_timestamp = transfer_timestamp(item["create-time"])
                        continuous = [create_timestamp, DAO.get_one("branch", name=shop_info["name"])["num"],
                                      shop_info["coordinate"][0], shop_info["coordinate"][1]]
                        for feature in self.shop_conti_features:
                            continuous.append(shop_info[feature])

                        continuous.append(user_info["contri-value"])
                        continuous.append(transfer_timestamp(user_info["register-date"]))
                        self._deal_continuous_data(continuous)

                        # categorical features
                        review_updated = int("update-time" in item)  # dim 2
                        shop_category = DAO.get_one("category", name=shop_info["category"])["_id"]  # dim CATEGORY_NUM
                        shop_district = DAO.get_one("district", name=shop_info["district"])["_id"]  # dim DISTRICT_NUM
                        user_vip = int(user_info["is-vip"])  # dim 2
                        user_gender = user_info["gender"]  # dim 3

                        categorical = [review_updated,
                                       shop_category,
                                       shop_district,
                                       user_vip,
                                       user_gender]

                        # sparse input
                        if member_id not in member_met_shops:
                            reviewed_shops = []
                            for review in DAO.get_all("review", **{"member-id": member_id, "create-time": {"$type": 9}}):
                                reviewed_shops.append((transfer_timestamp(review["create-time"]),
                                                       DISTINCT_SHOP.index(review["shop-id"])))
                            # reviewed_shops.sort(key=lambda x: x[0])

                            # wished_shops = []
                            for wish in DAO.get_all("wishlist", **{"member-id": member_id}):
                                if wish["shop-id"] not in DISTINCT_SHOP:
                                    continue
                                reviewed_shops.append((transfer_timestamp(wish["time"]) if "time" in wish else 0.0,
                                                     DISTINCT_SHOP.index(wish["shop-id"])))
                            reviewed_shops.sort(key=lambda x: x[0])

                            member_met_shops[member_id] = reviewed_shops
                            # {
                               # "reviewed": reviewed_shops,
                                # "wished": wished_shops
                            # }

                        sparse_vector = binary_search(member_met_shops[member_id], create_timestamp)
                        # wished_before = binary_search(member_met_shops[member_id]["wished"], create_timestamp)
                        for i in range(5):
                            sparse_vector.append(len(DISTINCT_SHOP)+sum(CATEGORICAL_FEATURE_DIMS[:i])+categorical[i])

                        '''
                        features = [shop_info["name"], user_info["name"],  # name
                                    model_shop[shop_id], model_member[member_id], continuous,
                                    sparse_vector] + categorical
                        '''

                        shop_emb = ' '.join(map(str, model_shop[shop_id]))
                        user_emb = ' '.join(map(str, model_member[member_id]))
                        conti_str = ' '.join(map(str, continuous))
                        sparse_vec_str = ' '.join(map(str, sparse_vector))
                        categorical_str = ','.join(map(str,categorical))
                        feature_str = "%s,%s,%s,%s,%s,%s,%s,%f" % \
                                      (shop_info["name"].encode('utf-8'), user_info["name"].encode('utf-8'),shop_emb, user_emb, conti_str, sparse_vec_str, categorical_str, score)

                        # if not is_infer:
                            # features += [[score]]
                        yield feature_str

                    except Exception, e:
                        print e.message
                        continue

        return reader

    def train(self):
        return self._train_reader(False)

    def infer(self):
        return self._train_reader(True)


obj = DataLoader()
reader = obj.train()

with open('train_data.csv','w') as f:
    i, c = 1, 1
    for d in reader():
        if i % 1000 == 0:
            print c
            c += 1
        f.write("%s\n" % d)
        i += 1
