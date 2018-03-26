import paddle.v2 as paddle
import os
import time
import random
import shutil
import numpy as np
import datetime
import math
from db import mongo
from gensim.models import word2vec

DAO = mongo.MyMongoDb("dzdp")
EMB_FEATURE_DIM = 256
RELU_NUM = [512, 256, 128]
MODEL_DIR_SRC = '../models/embedding'
MODEL_DIR_TARGET = '../models/dnn/embedding'
PARAM_TAR = '../models/dnn/parameters.tar'

with_gpu = os.getenv('WITH_GPU', '0') != '0'
paddle.init(use_gpu=with_gpu)

DISTINCT_SHOP = DAO.get_all("review").distinct("shop-id")
CATEGORY_NUM = 36
DISTRICT_NUM = 18
CATEGORICAL_FEATURE_DIMS = [2, 36, 18, 2, 3]
FM_SIZE = len(DISTINCT_SHOP) + sum(CATEGORICAL_FEATURE_DIMS)
EMB_SIZE = 64


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


def min_max_scale(value, min_val, max_val):
    assert min_val < max_val
    if not value or math.isnan(value):
        return random.random()
    if value >= max_val:
        return 1.0
    if value <= min_val:
        return 0.0

    return float(value - min_val) / (max_val - min_val)


class DataLoader:
    def __init__(self):
        self.shop_conti_features = ["avr-flavor", "avr-env", "avr-service", "avr-star", "avr-pay", "review-num"]
        self.threshold = {
            "avr-pay": (0, 2000),
            "lat": (39.0, 42.0),
            "lon": (115.0, 118.0)
        }

        self._min_max_list = []
        self._min_max_list.append((
            transfer_timestamp(DAO.get_max_or_min("review", "create-time", False, **{"create-time":{"$type":9}})["create-time"]),
            transfer_timestamp(DAO.get_max_or_min("review", "create-time", True, **{"create-time":{"$type":9}})["create-time"])))
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

                        features = [shop_info["name"], user_info["name"],  # name
                                    model_shop[shop_id], model_member[member_id], continuous,
                                    sparse_vector] + categorical

                        if not is_infer:
                            features += [[score]]
                        yield features

                    except Exception, e:
                        print e.message
                        continue

        return reader

    def train(self):
        return self._train_reader(False)

    def infer(self):
        return self._train_reader(True)


def copy_needed_model_files():
    with open('../models/NEW_MODEL', 'w+') as flag_file:
        flag = flag_file.read().strip().split()
        if len(flag) < 2 or flag[0] != "embedding" or flag[1] != "1":
            flag_file.write("embedding 0")
            print "Already updated model files."
            return

    print "Copying item2vec model files..."
    if os.path.exists(MODEL_DIR_TARGET):
        shutil.rmtree(MODEL_DIR_TARGET)
    shutil.copytree(MODEL_DIR_SRC, MODEL_DIR_TARGET)


def dnn_part():
    uid = paddle.layer.data(
        name='user_id',
        type=paddle.data_type.dense_vector(EMB_FEATURE_DIM))

    sid = paddle.layer.data(
        name='shop_id',
        type=paddle.data_type.dense_vector(EMB_FEATURE_DIM))

    continuous = paddle.layer.data(
        name='continuous',
        type=paddle.data_type.dense_vector(12)
    )

    updated = paddle.layer.data(
        name='review_updated',
        type=paddle.data_type.integer_value(2)
    )
    up_emb = paddle.layer.embedding(
        input=updated,
        size=EMB_SIZE
    )

    category = paddle.layer.data(
        name='shop_category',
        type=paddle.data_type.integer_value(CATEGORY_NUM)
    )
    cat_emb = paddle.layer.embedding(
        input=category,
        size=EMB_SIZE
    )

    district = paddle.layer.data(
        name='shop_district',
        type=paddle.data_type.integer_value(DISTRICT_NUM)
    )
    dis_emb = paddle.layer.embedding(
        input=district,
        size=EMB_SIZE
    )

    vip = paddle.layer.data(
        name='user_vip',
        type=paddle.data_type.integer_value(2)
    )
    vip_emb = paddle.layer.embedding(
        input=vip,
        size=EMB_SIZE
    )

    gender = paddle.layer.data(
        name='user_gender',
        type=paddle.data_type.integer_value(3)
    )
    gen_emb = paddle.layer.embedding(
        input=gender,
        size=EMB_SIZE
    )

    hidden0 = paddle.layer.fc(
        input=[uid, sid, continuous, up_emb, cat_emb, dis_emb, vip_emb, gen_emb],
        size=RELU_NUM[0],
        act=paddle.activation.Relu())

    hidden1 = paddle.layer.fc(
        input=hidden0,
        size=RELU_NUM[1],
        act=paddle.activation.Relu())

    hidden2 = paddle.layer.fc(
        input=hidden1,
        size=RELU_NUM[2],
        act=paddle.activation.Relu())

    return hidden2


def fm_layer(input, factor_size):
    first_order = paddle.layer.fc(
        input=input, size=1, act=paddle.activation.Linear())

    second_order = paddle.layer.factorization_machine(
        input=input,
        factor_size=factor_size,
        act=paddle.activation.Linear())

    out = paddle.layer.addto(
        input=[first_order, second_order],
        act=paddle.activation.Linear(),
        bias_attr=False)

    return out


def recommender():
    dnn = dnn_part()

    reviewed_sparse = paddle.layer.data(
        name='sparse_input',
        type=paddle.data_type.sparse_binary_vector(FM_SIZE)
    )

    fm = fm_layer(
        input=reviewed_sparse,
        factor_size=FM_SIZE)

    predict = paddle.layer.fc(
        input=[dnn, fm],
        size=1,
        act=paddle.activation.Sigmoid()
    )

    return predict


def event_handler(event):
    if isinstance(event, paddle.event.EndIteration):
        print "\nPass %d, Batch %d, Cost %.2f" % (
            event.pass_id, event.batch_id, event.cost)


def train():
    global parameters, model_member, model_shop, inference

    copy_needed_model_files()
    model_member = word2vec.Word2Vec.load("%s/model_member" % MODEL_DIR_TARGET)
    model_shop = word2vec.Word2Vec.load("%s/model_shop" % MODEL_DIR_TARGET)

    inference = recommender()

    cost = paddle.layer.square_error_cost(
        input=inference,
        label=paddle.layer.data(
            name='score', type=paddle.data_type.dense_vector(1)))

    parameters = paddle.parameters.create(cost)

    optimizer = paddle.optimizer.Adam(
        learning_rate=5e-5,
        regularization=paddle.optimizer.L2Regularization(rate=8e-4))

    trainer = paddle.trainer.SGD(
        cost=cost,
        parameters=parameters,
        update_equation=optimizer
    )

    feeding = {
        'shop_name': 0,
        'user_name': 1,
        'shop_id': 2,
        'user_id': 3,
        'continuous': 4,
        'sparse_input': 5,
        # 'wished_shops':5,
        'review_updated': 6,
        'shop_category': 7,
        'shop_district': 8,
        'user_vip': 9,
        'user_gender': 10,
        'score': 11
    }

    obj = DataLoader()

    trainer.train(
        reader=paddle.batch(paddle.reader.shuffle(obj.train(), 1024), batch_size=100),
        event_handler=event_handler,
        feeding=feeding,
        num_passes=10
    )

    print "Training done..."
    with open('../models/dnn/parameters.tar', 'w') as f:
        parameters.to_tar(f)
        print "Saving parameters done..."


def infer(member_id, shop_id):
    global parameters, model_member, model_shop, inference

    if not model_member:
        print "Loading item2vec model for member..."
        model_member = word2vec.Word2Vec.load("%s/model_member" % MODEL_DIR_TARGET)
    if member_id not in model_member:
        raise ValueError("member_id not in model_member")

    if not model_shop:
        print "Loading item2vec model for shop..."
        model_shop = word2vec.Word2Vec.load("%s/model_shop" % MODEL_DIR_TARGET)
    if shop_id not in model_shop:
        raise ValueError("shop_id not in model_shop")

    if not parameters:
        print "Loading saved parameters..."
        with open(PARAM_TAR, 'r') as f:
            parameters = paddle.parameters.Parameters.from_tar(f)

    if not inference:
        inference = paddle.layer.fc(input=dnn_part(), size=1, act=paddle.activation.Relu())

    user = model_member[member_id]
    shop = model_shop[shop_id]

    infer_dict = {
        'user_id': 0,
        'shop_id': 1
    }

    prediction = paddle.infer(
        output_layer=inference,
        parameters=parameters,
        input=[[user, shop]],
        feeding=infer_dict)

    print prediction


parameters, model_member, model_shop, inference = None, None, None, None


if __name__ == '__main__':
    train()
