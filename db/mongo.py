import pymongo

COLL_KEY = {
    "member": (1, "id"),
    "review": (1, "id"),
    "shop": (1, "id"),
    "unfinished": (1, "url"),
    "urllist": (1, "url"),
    "wishlist": (2, "member-id", "shop-id")
}


class MyMongoDb:
    _con = pymongo.MongoClient("localhost", 27017)

    def __init__(self, db_name):
        self._db = MyMongoDb._con[db_name]

    def insert(self, collection_name, **kv):
        self._db[collection_name].insert(kv)

    def insert_many(self, collection_name, documents):
        self._db[collection_name].insert_many(documents)

    def remove(self, collection_name, **kv):
        self._db[collection_name].remove(kv)

    def update(self, collection_name, query_dic, update_dic, upsert=True, multi=True):
        self._db[collection_name].update(query_dic, {'$set': update_dic}, upsert=upsert, multi=multi)

    def insert_with_update(self, collection_name, json):
        query = self.get_key_query(collection_name, json)
        self.update(collection_name, query, json)

    def get_all(self, collection_name, **kv):
        return self._db[collection_name].find(kv)

    def get_and_limit(self, collection_name, limit, **kv):
        _all_cursor = self.get_all(collection_name, **kv)
        if not limit:
            return _all_cursor
        assert isinstance(limit, int), "Mongo limit must be integer."
        return _all_cursor[0:limit]

    def get_and_sort(self, collection_name, sort_key, sort_order=pymongo.DESCENDING, **kv):
        return self.get_all(collection_name, **kv).sort(sort_key, sort_order)

    def get_max_or_min(self, collection_name, key, get_max, **query):
        if key not in query:
            query[key] = {"$exists": True}
        elif type(query[key]) is dict:
            query[key]["$exists"] = True
        else:
            return self.get_one(collection_name, **query)

        return self.get_and_sort(collection_name, key,
                                 sort_order=pymongo.DESCENDING if get_max else pymongo.ASCENDING,
                                 **query)[0]

    def get_one(self, collection_name, **kv):
        return self._db[collection_name].find_one(kv)

    def get_iter(self, collection_name, limit_num, **kv):
        res = self.get_all(collection_name, **kv)
        count = 0
        for item in res:
            if limit_num and count >= limit_num:
                break
            yield item["url"]

            count += 1

    def get_data_size(self, collection_name, **kv):
        return self.get_all(collection_name, **kv).count()

    def exists(self, collection_name, **kv):
        return self.get_one(collection_name, **kv) is not None

    def exists_by_key(self, collection_name, dic):
        query = self.get_key_query(collection_name, dic)
        return self.exists(collection_name, **query)

    def move_to_last(self, collection_name, **kv):
        documents = self.get_all(collection_name, **kv)
        for doc in documents:
            self.remove(collection_name, **doc)
            self.insert(collection_name, **doc)

    def count_column_num(self, collection_name, field):
        return self._db[collection_name].aggregate([{"$group": {"_id": "$%s"%field, "num": {"$sum": 1}}}])

    @staticmethod
    def get_key_query(collection_name, dic):
        query = {}
        for i in range(COLL_KEY[collection_name][0]):
            key = COLL_KEY[collection_name][i + 1]
            query[key] = dic[key]
        return query
