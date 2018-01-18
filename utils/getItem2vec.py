import time
import threading
import os.path as ospath
from db import mongo
from concurrent import futures
from gensim.models import word2vec
from utils import FileSentences

ID_FILE_PATH = "../data/embedding/%s_id"
MODEL_PATH = "../models/embedding/model_%s"
DAO = mongo.MyMongoDb("dzdp")

LEAST_REVIEW_THRESHOLD = 10
END_FLAG = False


def write_list_to_file(fw, a_list, min_sent_len=0):
    if len(a_list) <= min_sent_len:
        return

    fw.write('%s\n' % (' '.join(a_list)))


def write_lists(fw, *lists):
    for lst in lists:
        write_list_to_file(fw, lst)


def timing(func):
    units = ("h", "m", "s")

    def operate(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        second = int(time.time()-start_time)

        hour = second / 3600
        second %= 3600

        minute = second / 60
        second %= 60

        time_split = (str(hour), str(minute), str(second))
        time_expression = ' '.join(map(''.join, zip(time_split, units)))

        print "-----[%s]-----> %s." % (func.__name__, time_expression)
    return operate


@timing
def train(dim):
    print "Training model for %s..." % dim
    model_file = MODEL_PATH % dim
    sentences = FileSentences(ospath.abspath(ID_FILE_PATH % dim))

    model = word2vec.Word2Vec(sentences, size=256, window=15, min_count=1, negative=15, iter=10, workers=4)

    model.save(model_file)


def get_lists(dim):
    file_name = ID_FILE_PATH % dim
    sym_dim = {"shop": "member", "member": "shop"}.get(dim)
    sym_key_name = "%s-id" % sym_dim
    dim_key_name = "%s-id" % dim

    with open(file_name, 'a') as fwrite:
        query = {"$nor": [{"item2vec": True}]}
        cursor = DAO.get_all(sym_dim, **query)
        print "Get %s now. %d %ss in total." % (file_name, cursor.count(), sym_dim)

        try:
            i = 0
            for item in cursor:
                if END_FLAG:
                    break

                wishlist_cursor = DAO.get_all("wishlist", **{sym_key_name: item["id"]})
                review_cursor = DAO.get_all("review", **{sym_key_name: item["id"]})
                if wishlist_cursor.count() + review_cursor.count() <= LEAST_REVIEW_THRESHOLD:
                    DAO.move_to_last(sym_dim, **item)
                    continue

                if i % 100 == 0:
                    print "[%s] %d" % (sym_dim, i)
                i += 1

                wish_list = [dim_item[dim_key_name] for dim_item in wishlist_cursor]
                review_list = [review_item[dim_key_name] for review_item in review_cursor]

                write_lists(fwrite, wish_list, review_list)
                DAO.update(sym_dim, item, {"item2vec": True})
        except Exception, e:
            print "%s: %s" % (threading.currentThread().getName(), e)
        finally:
            print "%s is closing..." % threading.currentThread().getName()
            train(dim)
            with open('../models/NEW_MODEL', 'w') as flag_write:
                flag_write.write("embedding 1")


@timing
def main(minutes):
    global END_FLAG

    print "Whole time: {0} mins.".format(minutes)
    with futures.ThreadPoolExecutor(2) as pool:
        pool.map(get_lists, ["shop", "member"])

        for i in range(1, 6):
            time.sleep(minutes*12)
            print "%f minutes over." % (minutes*0.2*i)
        END_FLAG = True


if __name__ == '__main__':
    main(100)
