# -*- coding: utf-8 -*-

import random
import threading
import numpy as np
import jieba.posseg as posseg
from gensim.models import word2vec
from collections import Counter
from utils import FileSentences, DbDataProcess
from db import mongo

THREAD_LOCAL = threading.local()
DAO = mongo.MyMongoDb('dzdp')
END_FLAG = False

w2v_model = word2vec.Word2Vec.load('../models/model_review_words')

stop_words = [word.strip().decode('utf-8') for word in open('stopwords.txt')]
expected_pos = ['Ag', 'a', 'ad', 'an', 'd', 'i', 'l', 'Ng', 'n', 'ns', 'nt', 'nz', 't', 'vn', 'vg', 'vd']


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
        for existed_wid in word_weight:
            p_ew = predict_proba(existed_wid, wid)
            p_we = predict_proba(wid, existed_wid)
            word_weight[existed_wid] += p_ew
            p_w += p_we

        if w_pos.flag not in expected_pos:
            continue

        if wid in word_weight:
            word_weight[wid] = p_w * 2
        else:
            word_weight[wid] = p_w

    return Counter(word_weight).most_common(max(5, int(w_cnt*0.5)))


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


def get_candidates(key_word_set, key_words_weights):
    if not isinstance(key_word_set, set):
        raise ValueError("Input key_word_set should be a set.")
    for word_weight in key_words_weights:
        key_word_set.add(word_weight[0])


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

            good_key_words = set()
            bad_key_words = set()
            review_cursor = DAO.get_all("review", **{dim_key_name: item["id"]})
            if review_cursor.count() <= threshold:
                continue

            for review_item in review_cursor:
                review_content = review_item["comment"]
                score = DbDataProcess.get_review_overall_score(
                        review_item.get('star', None), review_item.get('score', None))

                sent_key_words = get_key_words_from(review_content)
                if score >= 3.0:
                    get_candidates(good_key_words, sent_key_words)
                else:
                    get_candidates(bad_key_words, sent_key_words)

            good_key_words = list(good_key_words)
            bad_key_words = list(bad_key_words)

            clu1 = db_scan(good_key_words)
            clu2 = db_scan(bad_key_words)


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
    test_list = ['16476', '693', '3924', '2847', '661', '2473', '137', '16374', '956', '284', '8056', '4909', '2322', '1544', '1771', '21', '22', '6575', '6640', '2664', '407', '0', '2248', '342', '1676', '96', '814', '7196', '287', '2331', '3832', '1093', '5146', '2850', '8635', '2468', '2669', '5140', '330', '120', '2295', '3402', '10202', '23329', '6156', '1498', '8339', '54', '4640', '1065', '511', '53', '2411', '10848', '416', '410', '412', '2140', '827', '8708', '6257', '1415', '8976', '1129', '4899', '199', '2472', '1324', '195', '337', '311', '67', '987', '193', '115', '19', '273', '279', '4836', '2165', '984', '583', '6728', '4041', '4807', '2302', '1855', '428', '1526', '1105', '1513', '12792', '2178', '2077', '424', '1013', '3280', '585', '6586', '6584', '300', '2608', '2284', '1136', '244', '2052', '6702', '6672', '8695', '3799', '100', '104', '33060', '1647', '2278', '842', '6276', '375', '2574', '30', '788', '3017', '34', '1536', '5913', '1008', '1249', '437', '434', '1004', '2371', '431', '3593', '4265', '1220', '1447', '3517', '571', '577', '8742', '333', '6220', '3815', '10554', '4102', '64', '66', '3169', '69', '19684', '23636', '484', '603', '5941', '1638', '3694', '3516', '5786', '5634', '6044', '6048', '503', '2117', '2429', '468', '23139', '465', '2421', '467', '9339', '39', '463', '3633', '99', '9583', '1863', '2089', '315', '4770', '12023', '1869', '225', '6888', '3462', '7081', '4170', '1624', '3445', '6263', '1282', '19224', '349', '727', '116', '723', '1100', '1744', '3502', '1742', '558', '157', '2814', '2868', '554', '2430', '2739', '1202', '13343', '13500', '1896', '2049', '8511', '1050', '5459', '2347', '1817', '7303', '954', '1610', '1196', '1', '1697', '320', '321', '5', '14392', '675', '1118', '328', '1698', '774', '203', '141', '2676', '290', '1179', '615', '77', '76', '1885', '20578', '3358', '21495', '1880', '12367', '1919', '2122', '2125', '319', '98', '1977', '13700', '1714', '942', '1597', '1971', '4663', '472', '1680', '1471', '3193', '1360', '475', '267', '6237']
    temp = db_scan(test_list)
