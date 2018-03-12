import paddle.v2 as paddle
import os
import time
import random
import shutil
import numpy as np
import datetime
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
distinct_shop = DAO.get_all("review").distinct("shop-id")
fm_size = len(distinct_shop)


def transfer_timestamp(time_str):
    time_format = "%Y-%m-%d"
    if len(time_str) > 10:
        time_format = "%Y-%m-%d %H:%M"
    timestamp = time.mktime(time.strptime(time_str, time_format))
    return timestamp


class DataLoader:
    def __init__(self):
        pass

    def _train_reader(self, is_infer):
        def reader():
            today = datetime.datetime.now()
            with DAO.get_all("review") as cursor:
                member_met_shops = {}
                for item in cursor:
                    member_id, shop_id = item["member-id"].encode('utf-8'), item["shop-id"].encode('utf-8')

                    if member_id not in model_member or shop_id not in model_shop\
                            or "star" not in item\
                            or len(item["create-time"]) < 10:
                        continue

                    score = item["star"]

                    shop_info = DAO.get_one("shop", id=shop_id)
                    user_info = DAO.get_one("member", id=member_id)

                    create_timestamp = transfer_timestamp(item["create-time"])

                    if member_id not in member_met_shops:
                        reviewed_shops = []
                        for review in DAO.get_all("review", **{"member-id": member_id}):
                            reviewed_shops.append((transfer_timestamp(review["create-time"]),
                                                   distinct_shop.index(review["shop-id"])))
                        reviewed_shops.sort(key=lambda x: x[0])

                        wished_shops = []
                        for wish in DAO.get_all("wishlist", **{"member-id": member_id}):
                            wished_shops.append((transfer_timestamp(wish["time"]),
                                                 distinct_shop.index(wish["shop-id"])))
                        wished_shops.sort(key=lambda x: x[0])

                        member_met_shops[member_id] = {
                            "reviewed": reviewed_shops,
                            "wished": wished_shops
                        }

                    review_updated = int("update-time" in item)
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


def dnn_network():
    uid = paddle.layer.data(
        name='user_id',
        type=paddle.data_type.dense_vector(EMB_FEATURE_DIM))

    sid = paddle.layer.data(
        name='shop_id',
        type=paddle.data_type.dense_vector(EMB_FEATURE_DIM))

    hidden0 = paddle.layer.fc(
        input=[uid, sid],
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
    u_s_emb = dnn_network()

    reviewed_sparse = paddle.layer.data(
        name='reviewed_shops',
        type=paddle.data_type.sparse_binary_vector(len(distinct_shop))
    )

    review_days = paddle.layer.data(
        name='review_days',
        type=paddle.data_type.integer_value(6)
    )

    updated = paddle.layer.data(
        name='updated',
        type=paddle.data_type.integer_value(2)
    )

    fm = fm_layer(
        input=reviewed_sparse,
        factor_size=fm_size)

    predict = paddle.layer.fc(
        input=[u_s_emb, fm, review_days, updated],
        size=1,
        act=paddle.activation.Sigmoid()
    )

    return predict


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

    trainer = paddle.trainer.SGD(
        cost=cost,
        parameters=parameters,
        update_equation=paddle.optimizer.Adam(
            learning_rate=5e-5,
            regularization=paddle.optimizer.L2Regularization(rate=8e-4)))

    feeding = {
        'user_id': 0,
        'shop_id': 1,
        'reviewed_shops': 2,
        'review_days': 3,
        'updated': 4,
        'score': 5
    }

    def event_handler(event):
        if isinstance(event, paddle.event.EndIteration):
            if event.batch_id % 100 == 0:
                print "Pass %d, Batch %d, Cost %.2f" % (
                    event.pass_id, event.batch_id, event.cost)

    trainer.train(
        reader=paddle.batch(paddle.reader.shuffle(DataLoader().train(), 1024), batch_size=100),
        event_handler=event_handler,
        feeding=feeding)

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
        inference = paddle.layer.fc(input=dnn_network(), size=1, act=paddle.activation.Relu())

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
