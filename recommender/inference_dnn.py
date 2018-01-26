import os
import paddle.v2 as paddle
from gensim.models import word2vec
from train import dnn_network

with_gpu = os.getenv('WITH_GPU', '0') != '0'
model_member = word2vec.Word2Vec.load('../models/embedding/model_member')
model_shop = word2vec.Word2Vec.load('../models/embedding/model_shop')


def main(user_id, shop_id):
    if user_id not in model_member or shop_id not in model_shop:
        raise ValueError("The input data is invalid.")

    paddle.init(use_gpu=with_gpu)
    predict = dnn_network()

    with open('../models/dnn/parameters.tar', 'r') as f:
        parameters = paddle.parameters.Parameters.from_tar(f)

    user = model_member[user_id]
    shop = model_shop[shop_id]

    infer_dict = {
        'user_id': 0,
        'shop_id': 1
    }

    prediction = paddle.infer(
        output_layer=predict,
        parameters=parameters,
        input=[[user, shop]],
        feeding=infer_dict)

    print prediction


if __name__ == "__main__":
    uid = "40321595"
    sid = "93605024"

    main(uid, sid)