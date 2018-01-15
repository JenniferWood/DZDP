# -*- coding: utf-8 -*-

import time
import os.path
from gensim.models import word2vec
from db import mongo
from utils import FileSentences
from segword import WordSegmentation

TRAIN_FILE = '../data/review_words/review_words.dat'
STOP_WORD_FILE = './stopwords.txt'


def init(user_dict):
    global word_seg
    global dao
    user_dict = os.path.abspath(user_dict)
    word_seg = WordSegmentation(os.path.abspath(STOP_WORD_FILE), user_dict)
    dao = mongo.MyMongoDb("dzdp")


def get_raw_data():
    print "Get Review Data..."

    with open(TRAIN_FILE, 'a') as f_write:
        i = 0
        try:
            review_query = {"$nor": [{"got": True}]}
            for review_item in dao.get_all("review", **review_query):
                shop_id = review_item["shop-id"]
                if not dao.exists("shop", id=shop_id):
                    dao.move_to_last("review", **review_item)
                    continue

                i += 1
                print "\n=========== %s ============" % review_item["id"]

                if "recommend" in review_item:
                    word_seg.add_words_to_dict(review_item["recommend"], 'nz')

                word_list = word_seg.cut(review_item["comment"])
                f_write.write("%s\n" % ' '.join(word_list).encode('utf-8'))

                dao.update("review", review_item, {"got": True})
        except KeyboardInterrupt:
            raise
        except Exception, ex:
            print "Exception:%s" % ex
            return
        finally:
            return i


def train_model(model_file, has_model=False, sentence_num=None):
    # sentences = word2vec.Text8Corpus(TRAIN_FILE)
    print "Training Model..."

    sentences = FileSentences(TRAIN_FILE)
    start = time.time()

    if has_model:
        if not isinstance(sentence_num, int):
            raise ValueError('need sentence_num')
        model = word2vec.Word2Vec.load(model_file)
        update_num = model.train(sentences, total_examples=sentence_num, epochs=model.iter)
        print 'Update %d Words' % update_num
    else:
        # model = word2vec.Word2Vec(sentences, size=200, min_count=20, sg=1, hs=1, iter=10, workers=4)
        model = word2vec.Word2Vec(sentences, size=200, min_count=50, workers=4, negative=15)

    model.save(model_file)
    end = time.time()
    print 'Model Training is over: %f seconds.' % (end - start)


if __name__ == '__main__':
    # train_model("../models/model_review_words_cbow_ns")
    train_model("../models/model_review_words_skipgram_hs")

    init('./fooddict.txt')

    try:
        while True:
            sent_num = get_raw_data()
            print "\nGet Sentences : %d" % sent_num

            if sent_num == 0: break
            time.sleep(120)
    except KeyboardInterrupt:
        print "See You Tomorrow...O(∩_∩)O~~"
