# -*- coding: utf-8 -*-
import sys
import re
import jieba
import jieba.posseg as posseg
from jieba import analyse
from db import mongo

reload(sys)
sys.setdefaultencoding('utf-8')

DAO = mongo.MyMongoDb("dzdp")
WORD_DICT = "worddict"
PUNCTUATIONS = u'[.,;!?~。，；！？～\n]'
SPECIAL_CHAR_PATTERN = re.compile(
    u"([\u0000-\u0040])|"
    u"([\u005b-\u0060])|"
    u"([\u007b-\u4dff])|"
    u"([\u9fc0-\uffff])"
    "+", flags=re.UNICODE)


def load_stop_words(file_path):
    res = set()
    if not file_path:
        return res
    with open(file_path, 'r') as f_open:
        for word in f_open:
            res.add(word.strip().decode())
    return res


class WordSegmentation:
    def __init__(self, stop_word=None, user_dict=None):
        if user_dict:
            jieba.load_userdict(user_dict)

        self._stop_words = load_stop_words(stop_word)

    def add_words_to_dict(self, words, tag=None):
        if isinstance(words, list):
            for word in words:
                jieba.add_word(word, tag=tag)

    def split_into_sentences(self, p):
        res = re.split(PUNCTUATIONS, p, flags=re.UNICODE)
        return res

    def remove_special_char(self, s):
        if not isinstance(s, unicode):
            s = s.decode()
        return SPECIAL_CHAR_PATTERN.sub('', s.strip())

    def cut(self, s):
        words = jieba.cut(s)

        wid = DAO.get_data_size(WORD_DICT)
        for word in words:
            word = self.remove_special_char(word)
            if not word or word in self._stop_words:
                continue
            if not DAO.get_one(WORD_DICT, word=word):
                DAO.insert(WORD_DICT, _id=wid, word=word)
                print "[%s %d]" % (word, wid)
                wid += 1
            yield word

    def cut_with_pos(self, s, with_pos=False):
        words = posseg.cut(s)

        wid = DAO.get_data_size(WORD_DICT)
        for word in words:
            w = self.remove_special_char(word.word)
            if not w or w in self._stop_words:
                continue
            if not DAO.get_one(WORD_DICT, word=w):
                DAO.insert(WORD_DICT, _id=wid, word=w)
                print "[%s %d]" % (w, wid)
                wid += 1
            yield (w, word.flag)

    def extract_key_words(self, paragraph, top_k, allow_pos=None):
        return analyse.extract_tags(paragraph, top_k, allowPOS=allow_pos)
