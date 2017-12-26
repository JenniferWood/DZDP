# -*- coding: utf-8 -*-

import jieba
import re
import sys
import time
import os
from gensim.models import word2vec
from db import mongo

MODEL_FILE = '../models/fasttext_model_review_word_skipgram'
Train_File_Format = '../data/review_words/review_words_%s.data'
Stopwords_File = './stopwords.txt'

MAX_FILE_SIZE = 40

SPLIT_KEY = [u'，', u',', u'。', u'.', u'！', u'!', u'？', u'?', u' ', u' ', u'~', u'～', u'、', u':', u'：', u':', u'；', u';']

EMOJI_PATTERN = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
    "+", flags=re.UNICODE)


class MySentences(object):
    """docstring for MySentences"""

    def __init__(self, filename):
        self._file_name = filename

    def __iter__(self):
        with open(self._file_name, 'r') as fopen:
            for line in fopen:
                yield line.split()


def get_stop_words():
    with open(Stopwords_File, 'r') as fopen:
        return [line.strip().decode('utf-8') for line in fopen]


def remove_emoji(text):
    return EMOJI_PATTERN.sub(r'', text.strip())


def get_raw_data(tick):
    stopwords = get_stop_words()
    dao = mongo.MyMongoDb("dzdp")

    print "Get Review Data..."

    train_file_path = Train_File_Format % tick

    with open(train_file_path, 'a') as fwrite:
        i = 0
        wid = dao.get_data_size("word")

        try:
            for review_item in dao.get_all("review", got=False):
                shop_id = review_item["shop-id"]
                if not dao.exists("shop", id=shop_id):
                    continue

                review = remove_emoji(review_item["comment"])
                seg_list = jieba.cut(review, cut_all=False)
                word_id_list = []
                sent_contain_new_word = False
                for word in seg_list:
                    if word in SPLIT_KEY or word.strip == '':
                        if len(word_id_list) == 0:
                            continue
                        s = ' '.join(word_id_list)
                        # print s
                        fwrite.write("%s\n" % s)
                        i += 1
                        word_id_list = []

                        if sent_contain_new_word:
                            print ""
                            sent_contain_new_word = False
                    elif word not in stopwords:
                        existed = dao.get_one("word", word=word)
                        if existed is None:
                            word_id_list.append(str(wid))
                            dao.insert("word", word=word, id=wid)
                            wid += 1

                            print "[%s %s]" % (word, wid),
                            sent_contain_new_word = True
                        else:
                            word_id_list.append(str(existed["id"]))

                if len(word_id_list) > 0:
                    fwrite.write("%s\n" % (' '.join(word_id_list)))
                    i += 1

                    if sent_contain_new_word:
                        print ""

                dao.update("review", review_item, {"got": True})
        except KeyboardInterrupt:
            raise
        except Exception, ex:
            print "Exception:%s" % ex
            return
        finally:
            print "Get Sentences : %d" % i
            return i


def train_model(train_file_path, sentence_num, has_model=False):
    # sentences = word2vec.Text8Corpus(train_file_path)
    print "Training Model..."

    sentences = MySentences(train_file_path)
    start = time.time()

    if has_model:
        model = word2vec.Word2Vec.load(MODEL_FILE)
        update_num = model.train(sentences, total_examples=sentence_num, epochs=model.iter)
        print 'Update %d Words' % update_num
    else:
        model = word2vec.Word2Vec(sentences, size=256, window=10, min_count=64, sg=1, hs=1, iter=10, workers=4)

    model.save(MODEL_FILE)
    end = time.time()
    print 'Model Training is over: %f seconds.' % (end - start)


if __name__ == '__main__':
    time_tick = time.strftime("%Y%m%d%H%M%S", time.localtime())
    whole_sent_num = 0

    if len(sys.argv) >= 2:
        time_tick = sys.argv[1]
        whole_sent_num = int(sys.argv[2])

    train_file = Train_File_Format % time_tick
    print "Train file %s, line %d" % (train_file, whole_sent_num)

    try:
        while True:
            cur_sent_num = get_raw_data(time_tick)
            whole_sent_num += cur_sent_num
            file_size = os.path.getsize(train_file) / 1024.0 / 1024.0
            print "--------------------------\n%f M and %d Lines\n--------------------------" % \
                  (file_size, whole_sent_num)
            if file_size >= MAX_FILE_SIZE:
                break
            time.sleep(120)

        train_model(train_file, whole_sent_num, True)
        os.rename(train_file, train_file + "_t")

    except KeyboardInterrupt:
        print "See You Tomorrow...O(∩_∩)O~~"