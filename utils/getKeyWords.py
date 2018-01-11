# -*- coding: utf-8 -*-

import random
import threading
import numpy as np
import jieba.posseg as posseg
from gensim.models import word2vec
from collections import Counter
from db import mongo
from jieba import analyse
tfidf = analyse.extract_tags
textrank = analyse.textrank

THREAD_LOCAL = threading.local()
DAO = mongo.MyMongoDb('dzdp')
END_FLAG = False

w2v_model = word2vec.Word2Vec.load('../models/model_review_words')

stop_words = [word.strip().decode('utf-8') for word in open('stopwords.txt')]
expected_pos = ['Ag', 'a', 'ad', 'an', 'd', 'i', 'l', 'Ng', 'n', 'ns', 'nt', 'nz', 't', 'vn', 'vg', 'vd']


def extract_key_words(paragraph, top_k):
    key_words = tfidf(paragraph, top_k)
    res = []
    for word in key_words:
        _wid = DAO.get_one("word", word=word)
        if _wid is None or str(_wid["id"]) not in w2v_model:
            continue
        res.append(str(_wid["id"]))
    return res

def predict_proba(i_word, o_word):
    i_word_vec = w2v_model[i_word]
    o_word = w2v_model.wv.vocab[o_word]
    o_word_l = w2v_model.syn1[o_word.point].T
    dot = np.dot(i_word_vec, o_word_l)
    l_prob = -sum(np.logaddexp(0, -dot) + o_word.code*dot)
    return l_prob


def get_key_words_from(paragraph):
    word_weight = {}

    w_cnt = 0
    for w_pos in posseg.cut(paragraph):
        w = w_pos.word
        if w in stop_words:
            continue

        wid = DAO.get_one('word', word=w)
        if wid is None or str(wid["id"]) not in w2v_model:
            continue

        w_cnt += 1

        wid = str(wid["id"])
        p_w = 0
        cache = {}
        for existed_wid in word_weight:
            _e_w = "_".join([existed_wid, wid])
            _w_e = "_".join([wid, existed_wid])

            if _e_w in cache:
                p_ew = cache[_e_w]
            else:
                p_ew = cache[_e_w] = predict_proba(existed_wid, wid)

            if _w_e in cache:
                p_we = cache[_w_e]
            else:
                p_we = cache[_w_e] = predict_proba(wid, existed_wid)

            word_weight[existed_wid] += p_ew
            p_w += p_we

        if w_pos.flag not in expected_pos:
            continue

        if wid in word_weight:
            word_weight[wid] = p_w * 2
        else:
            word_weight[wid] = p_w

    word_weight_pairs = Counter(word_weight).most_common(max(5, int(w_cnt*0.5)))
    return [pair[0] for pair in word_weight_pairs]


def get_points_distance(vec1, vec2):
    return np.linalg.norm(vec1 - vec2)


def get_near(key_words, eps, i):
    vec0 = w2v_model[key_words[i]]
    near = []
    for j in range(len(key_words)):
        if j == i:
            continue
        vec1 = w2v_model[key_words[j]]
        if get_points_distance(vec0, vec1) <= eps:
            near.append(j)
    return near


def get_optimal_eps(key_words, min_pts):
    distances = []
    near = {}
    e = []
    for i in range(len(key_words)):
        vec_i = w2v_model[key_words[i]]
        d_i = sorted(
            [(get_points_distance(vec_i, w2v_model[key_words[j]]), j) for j in range(len(key_words)) if j != i])
        distances.append(d_i)
        e.append(d_i[min_pts-1][0])
        near[i] = [d_i[x][1] for x in range(min_pts)]

    e = sorted(e)
    eps = e[int(0.2 * len(key_words))]

    for i in near:
        d_i = distances[i]
        if d_i[min_pts-1][0] < eps:
            x = min_pts
            while x < len(d_i):
                if d_i[x][0] > eps:
                    break
                near[i].append(d_i[x][1])
                x += 1
        else:
            x = min_pts - 1
            while x >= 0:
                if d_i[x][0] <= eps:
                    break
                near[i].remove(d_i[x][1])
                x -= 1

    return near


def db_scan(key_words):
    if key_words is None or len(key_words) == 0:
        return {}

    k = 0
    min_pts = 5

    near = get_optimal_eps(key_words, min_pts)

    m = {}
    _m = {}
    unvisited_points = range(len(key_words))

    while len(unvisited_points) > 0:
        i = random.choice(unvisited_points)
        unvisited_points.remove(i)

        if i in m:
            continue

        t = list(near[i])

        if len(t) < min_pts:
            m[i] = 0
            continue

        # len(t) >= min_pts branch
        k += 1
        m[i] = k
        _m.setdefault(k, [])
        _m[k].append(key_words[i])

        while len(t) > 0:
            j = random.choice(t)
            t.remove(j)

            if j in m and m[j] > 0:
                continue

            m[j] = k
            _m.setdefault(k, [])
            _m[k].append(key_words[j])

            if len(near[j]) >= min_pts:
                for j_n in near[j]:
                    if j_n not in t:
                        t.append(j_n)

    return _m


def mearge_list(list1, list2):
    if not isinstance(list1, list) or not isinstance(list2, list):
        raise ValueError("Input should be list.")

    for item in list2:
        if item not in list1:
            list1.append(item)


def main(dim, threshold=5):
    dim_key_name = "%s-id" % dim

    THREAD_LOCAL.train_cnt = 0
    THREAD_LOCAL.sents = []
    THREAD_LOCAL.dim = dim

    query = {"$nor": [{"keyWords": True}]}
    cursor = DAO.get_all(dim, **query)
    print "Start. %d %ss in total." % (cursor.count(), dim)

    try:
        for item in cursor:
            if END_FLAG:
                print "%s is closing..." % threading.currentThread().getName()
                break

            review_cursor = DAO.get_all("review", **{dim_key_name: item["id"]})
            if review_cursor.count() <= threshold:
                continue

            whole_review = '\n'.join([review_item["comment"] for review_item in review_cursor])
            top_k = max(20, 2*review_cursor.count())
            key_words = extract_key_words(whole_review, top_k)
            # mearge_list(key_words, textrank(whole_review, top_k))

            clu1 = db_scan(key_words)


            '''
            res = {'dim': dim,
                   'id': item["id"],
                   'good_key_words': get_key_words_from(' '.join(good_review)),
                   'bad_key_words': get_key_words_from(' '.join(bad_review))}

            DAO.insert('keywords', **res)
            DAO.update(dim, item, {"keyWords": True})
            '''
    except Exception, e:
        print "%s: %s" % (threading.currentThread().getName(), e)


if __name__ == '__main__':
    main('shop')
