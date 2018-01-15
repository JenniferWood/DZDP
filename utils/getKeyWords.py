# -*- coding: utf-8 -*-

import time
import random
import threading
import heapq
import numpy as np
from gensim.models import word2vec
from segword import WordSegmentation
from db import mongo

THREAD_LOCAL = threading.local()
DAO = mongo.MyMongoDb('dzdp')
END_FLAG = False
WORDVEC_SIZE = 200

w2v_model = word2vec.Word2Vec.load('../models/model_review_words_cbow_ns')
user_dict = "./fooddict.txt"
stop_words = "./stopwords.txt"
# expected_pos = ('Ag', 'a', 'ad', 'an', 'd', 'i', 'l', 'Ng', 'n', 'ns', 'nt', 'nz', 't', 'vn', 'vg', 'vd')
expected_pos = ('a', 'ad', 'an', 'i', 'l', 'n', 'ns', 'nz', 'vn', 'vg', 'vd')

word_seg = WordSegmentation(stop_words, user_dict)


def create_kd_tree(words):
    if not words or len(words) == 0:
        return None

    root = KdTreeNode(w2v_model[words[0]], words[0], 0)
    for i in range(1, len(words)):
        root.insert_into_kd_tree(KdTreeNode(w2v_model[words[i]], words[i]))
    return root


def get_trace(kd_tree_root, point):
    trace = []
    cur = kd_tree_root
    while cur:
        trace.append(cur)
        if point[cur.dim] > cur.sample[cur.dim]:
            cur = cur.right
        else:
            cur = cur.left
    return trace


def get_k_near(kd_tree_root, point, k, contain_the_point=False):
    k_near_neg = []
    trace = get_trace(kd_tree_root, point)

    while len(trace) > 0:
        parent = trace.pop()
        if np.array_equal(point, parent.sample) and not contain_the_point:
            continue

        dis_with_parent = -get_points_distance(parent.sample, point)
        if len(k_near_neg) < k:
            heapq.heappush(k_near_neg, (dis_with_parent, parent))
        elif dis_with_parent > k_near_neg[0][0]:
            k_near_neg[0] = (dis_with_parent, parent)
            heapq.heapify(k_near_neg)
        else:
            continue

        dis_with_plane = abs(parent.sample[parent.dim] - point[parent.dim])
        if dis_with_plane < -k_near_neg[0][0]:
            if point[parent.dim] > parent.sample[parent.dim]:
                parent = parent.left
            else:
                parent = parent.right
            trace += get_trace(parent, point)

    k_near = []
    while k_near_neg:
        pair = heapq.heappop(k_near_neg)
        k_near.append((-pair[0], pair[1]))
    return k_near[::-1]


def get_dis_near(kd_tree_root, point, dis):
    dis_near_heap = []
    trace = get_trace(kd_tree_root, point)

    while len(trace) > 0:
        parent = trace.pop()
        if np.array_equal(point, parent.sample):
            continue

        dis_with_parent = get_points_distance(parent.sample, point)
        if dis_with_parent <= dis:
            dis_near_heap.append((dis_with_parent, parent))
        else:
            continue

        dis_with_plane = abs(parent.sample[parent.dim] - point[parent.dim])
        if dis_with_plane < dis:
            if point[parent.dim] > parent.sample[parent.dim]:
                parent = parent.left
            else:
                parent = parent.right
            trace += get_trace(parent, point)

    return sorted(dis_near_heap)


def extract_key_words(paragraph, top_k, allow_pos=None):
    key_words = word_seg.extract_key_words(paragraph, top_k, allow_pos)
    res = []
    for word in key_words:
        word = word.encode('utf-8')
        if word not in w2v_model:
            continue
        res.append(word)
    return res


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


def get_optimal_eps(key_words, kd_tree, min_pts):
    near = {}
    e = []

    for word in key_words:
        k_near = get_k_near(kd_tree, w2v_model[word], min_pts)
        e.append(k_near[min_pts - 1][0])

    e = sorted(e)
    eps = e[int(0.1 * len(key_words))]

    for i in range(len(key_words)):
        near[key_words[i]] = [node[1].word for node in get_dis_near(kd_tree, w2v_model[key_words[i]], eps)]

    return near


def db_scan(kd_tree, key_words, min_pts=4):
    if key_words is None or len(key_words) == 0:
        return {}

    k = 0

    near = get_optimal_eps(key_words, kd_tree, min_pts)

    m = {}
    _m = {}

    while key_words:
        i = random.choice(key_words)
        key_words.remove(i)

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
        _m[k].append(i)

        while t:
            j = random.choice(t)
            t.remove(j)

            if j not in m:
                if len(near[j]) >= min_pts:
                    for j_n in near[j]:
                        if j_n not in t:
                            t.append(j_n)

            if j not in m or m[j] == 0:
                m[j] = k
                _m[k].append(j)

    return _m


def mearge_list(list1, list2):
    if not isinstance(list1, list) or not isinstance(list2, list):
        raise ValueError("Input should be list.")

    for item in list2:
        if item not in list1:
            list1.append(item)


def get_time_interval(string):
    global start_time
    _time = time.time()
    print "%s: %f sec." % (string, _time - start_time)
    start_time = _time


def main(dim, threshold=5):
    global start_time
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
            get_time_interval("Get whole reviews for %s" % item["id"])

            top_k = max(20, 2*review_cursor.count())
            key_words = extract_key_words(whole_review, top_k, expected_pos)
            get_time_interval("Get top %d key words" % top_k)
            # mearge_list(key_words, textrank(whole_review, top_k))

            if len(key_words) > WORDVEC_SIZE:
                kd_tree = create_kd_tree(key_words)
                get_time_interval("Create kd tree")

                cluster = db_scan(kd_tree, key_words)
                get_time_interval("DBSCAN")

                for c in cluster:
                    if len(cluster[c]) == 0:
                        continue

                    if len(cluster[c]) > 1:
                        center_coor = sum([w2v_model[word] for word in cluster[c]]) / len(cluster[c])
                        center_word = get_k_near(kd_tree, center_coor, 1, True)[0][1].word
                    else:
                        center_word = cluster[c][0]

                    key_words.append(center_word)
                print " ".join(key_words)
                get_time_interval("Update key word list")




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


class KdTreeNode:
    def __init__(self, sample, word, dim=-1):
        self.sample = sample
        self.word = word
        self.dim = dim
        self.left = None
        self.right = None

    def __repr__(self):
        return "{0}_{1}".format(self.dim, self.word)

    def compare(self, other_sample):
        if other_sample is None:
            raise ValueError("Can't compare a node with None.")
        if len(other_sample) != len(self.sample):
            raise ValueError("Wrong format for other_sample.")

        if other_sample[self.dim] > self.sample[self.dim]:
            return 1
        else:
            return -1

    def points_distance(self, other_node):
        if not isinstance(other_node, KdTreeNode):
            raise ValueError("Input type error.")

        return get_points_distance(self.sample, other_node.sample)

    def point_plane_distance(self, other_node):
        if not isinstance(other_node, KdTreeNode):
            raise ValueError("Input type error.")

        return abs(other_node.sample[other_node.dim] - self.sample[other_node.dim])

    def insert_into_kd_tree(self, node):
        if node.sample[self.dim] > self.sample[self.dim]:
            if not self.right:
                node.dim = (self.dim + 1) % WORDVEC_SIZE
                self.right = node
            else:
                self.right.insert_into_kd_tree(node)
        else:
            if not self.left:
                node.dim = (self.dim + 1) % WORDVEC_SIZE
                self.left = node
            else:
                self.left.insert_into_kd_tree(node)


if __name__ == '__main__':
    start_time = time.time()
    main('shop')
