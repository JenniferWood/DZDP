import paddle.v2 as paddle
import os
import datetime
from paddle.v2.plot import Ploter

EMB_FEATURE_DIM = 256
RELU_NUM = [512, 256, 128]
PARAM_TAR = '../models/dnn/parameters.tar'
CATEGORY_NUM = 36
DISTRICT_NUM = 18
CATEGORICAL_FEATURE_DIMS = [2, CATEGORY_NUM, DISTRICT_NUM, 2, 3]
FM_SIZE = 50957 + sum(CATEGORICAL_FEATURE_DIMS)
EMB_SIZE = 64

with_gpu = os.getenv('WITH_GPU', '0') != '0'
paddle.init(use_gpu=with_gpu)

title_train = "Train"
title_test = "Test"
ploter = Ploter(title_train, title_test)


class DataLoader:
    def __init__(self, data_file):
        self.data_file = data_file

    def _train_reader(self, is_infer):
        def reader():
            with open(self.data_file, 'r') as file_read:
                for item in file_read:
                    features = item.strip().split(',')

                    shop_id = map(float, features[2].split(' '))
                    user_id = map(float, features[3].split(' '))
                    continuous = map(float, features[4].split(' '))
                    sparse = map(int, features[5].split(' '))
                    categorical = map(int, features[6:11])
                    score = float(features[11])

                    data = [shop_id, user_id, continuous,
                            sparse] + categorical

                    if not is_infer:
                        data += [[score]]

                    yield data

        return reader

    def train(self):
        return self._train_reader(False)

    def infer(self):
        return self._train_reader(True)


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
        print "\n[%s] Pass %d, Batch %d, Cost %.2f" % (
            datetime.datetime.now(), event.pass_id, event.batch_id, event.cost)
        ploter.append(title_train, event.batch_id, event.cost)
        ploter.plot('./train.png')


def train(data_file):
    global parameters, inference

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
        'shop_id': 0,
        'user_id': 1,
        'continuous': 2,
        'sparse_input': 3,
        # 'wished_shops':5,
        'review_updated': 4,
        'shop_category': 5,
        'shop_district': 6,
        'user_vip': 7,
        'user_gender': 8,
        'score': 9
    }

    obj = DataLoader(data_file)

    trainer.train(
        reader=paddle.batch(paddle.reader.shuffle(obj.train(), 1024), batch_size=100),
        event_handler=event_handler,
        feeding=feeding
    )

    print "Training done..."
    with open('../models/dnn/parameters.tar', 'w') as f:
        parameters.to_tar(f)
        print "Saving parameters done..."


parameters, inference = None, None


if __name__ == '__main__':
    train('./train_data_1.csv')
