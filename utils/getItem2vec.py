import time
import os.path as opath
from db import mongo
from concurrent import futures
from sentences import MySentences
from gensim.models import word2vec

ID_FILE_PATH = "../data/embedding/%s_id"
MODEL_PATH = "../models/model_%s"
DAO = mongo.MyMongoDb("dzdp")


def write_list_to_file(fw, a_list):
    if len(a_list) == 0:
        return

    fw.write('%s\n' % (' '.join(a_list)))


def get_lists(dim):
    file_name = ID_FILE_PATH % dim
    symmetrical_dim = {"shop": "member", "member": "shop"}.get(dim)
    symmetrical_key_name = "%s-id" % symmetrical_dim
    dim_key_name = "%s-id" % dim

    with open(file_name, 'w') as fwrite:
        i = 0
        cursor = DAO.get_all(symmetrical_dim)
        print "Get %s[%d] now..." % (file_name, cursor.count())
        for item in cursor:
            i += 1
            if i % 100 == 0:
                print "[%s] %d" % (dim, i)

            dim_items = DAO.get_all("wishlist", **{symmetrical_key_name: item["id"]})

            wish_list = [dim_item[dim_key_name] for dim_item in dim_items]
            write_list_to_file(fwrite, wish_list)

            good_review = []
            bad_review = []
            for review_item in DAO.get_all("review", **{symmetrical_key_name: item["id"]}):
                dim_id = review_item[dim_key_name]

                score = 0
                num = 0
                if "star" in review_item:
                    score = review_item["star"]
                    num += 1
                if "score" in review_item and len(review_item["score"]) > 0:
                    for concrete_rate in review_item["score"]:
                        if concrete_rate < 0:
                            continue
                        score += concrete_rate
                        num += 1
                if num > 0:
                    score /= num

                if score >= 3.0:
                    good_review.append(dim_id)
                else:
                    bad_review.append(dim_id)

            write_list_to_file(fwrite, good_review)
            write_list_to_file(fwrite, bad_review)


def timing(info):
    def decorate(func):
        def operate(*args, **kwargs):
            start_time = time.time()
            func(*args, **kwargs)
            print "%s: %fsec." % (info, time.time()-start_time)
        return operate
    return decorate


@timing('Done training model')
def train(dim):
    data_file = opath.abspath(ID_FILE_PATH % dim)
    sentences = MySentences(data_file)
    model = word2vec.Word2Vec(sentences, size=256, sg=1, negative=100, iter=50)
    model.save(MODEL_PATH % dim)


@timing('Done getting lists')
def main():
    with futures.ThreadPoolExecutor(2) as pool:
        pool.map(get_lists, ["shop", "member"])


if __name__ == '__main__':
    main()
