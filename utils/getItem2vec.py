import time
import threading
import os.path as ospath
from db import mongo
from concurrent import futures
from gensim.models import word2vec

ID_FILE_PATH = "../data/embedding/%s_id"
MODEL_PATH = "../models/model_%s"
DAO = mongo.MyMongoDb("dzdp")
THREAD_LOCAL = threading.local()

BATCH_LINES = 5000
END_FLAG = False


def write_list_to_file(fw, a_list, min_sent_len=0):
    if len(a_list) <= min_sent_len:
        return

    fw.write('%s\n' % (' '.join(a_list)))
    THREAD_LOCAL.sents.append(a_list)


def timing(func):
    def operate(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        print "-----[%s]-----> %f seconds." % (func.__name__, time.time()-start_time)
    return operate


@timing
def train(dim, times, sentences):
    print "train(dim=%s, times=%d, sentences_len=%d...)" % (dim, times, len(sentences))
    model_file = MODEL_PATH % dim
    if not ospath.exists(model_file):
        print "First Training..."
        model = word2vec.Word2Vec(sentences, size=256, sg=1, negative=100, iter=10)
    else:
        model = word2vec.Word2Vec.load(model_file)
        model.train(sentences, total_examples=len(sentences), epochs=model.iter)
    model.save(model_file)


def get_lists(dim):
    file_name = ID_FILE_PATH % dim
    symmetrical_dim = {"shop": "member", "member": "shop"}.get(dim)
    symmetrical_key_name = "%s-id" % symmetrical_dim
    dim_key_name = "%s-id" % dim

    THREAD_LOCAL.train_cnt = 0
    THREAD_LOCAL.sents = []
    THREAD_LOCAL.dim = dim

    with open(file_name, 'w') as fwrite:
        query = {"$nor": [{"item2vec": True}]}
        cursor = DAO.get_all(symmetrical_dim, **query)
        print "Get %s now. %d %ss in total." % (file_name, cursor.count(), symmetrical_dim)

        try:
            for item in cursor:
                if END_FLAG:
                    print "%s is closing..." % threading.currentThread().getName()
                    break

                if len(THREAD_LOCAL.sents) >= BATCH_LINES:
                    train(THREAD_LOCAL.dim, THREAD_LOCAL.train_cnt, THREAD_LOCAL.sents)
                    THREAD_LOCAL.train_cnt += 1
                    THREAD_LOCAL.sents = []

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

                DAO.update(symmetrical_dim, item, {"item2vec": True})
        except Exception, e:
            print "%s: %s" % (threading.currentThread().getName(), e)
        finally:
            train(THREAD_LOCAL.dim, THREAD_LOCAL.train_cnt, THREAD_LOCAL.sents)


@timing
def main(minutes):
    global END_FLAG
    with futures.ThreadPoolExecutor(2) as pool:
        pool.map(get_lists, ["shop", "member"])

        for i in range(1, 6):
            time.sleep(minutes*12)
            print "%f minutes over." % (minutes*0.2*i)
        END_FLAG = True


if __name__ == '__main__':
    main(120)
