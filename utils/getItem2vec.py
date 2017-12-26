import time
from db import mongo
from concurrent import futures

ID_FILE_PATH = "../data/embedding/%s.ids"

dao = mongo.MyMongoDb("dzdp")


def get_lists(dim):
    file_name = ID_FILE_PATH % dim
    symmetrical_dim = {"shop": "member", "member": "shop"}.get(dim)
    symmetrical_key_name = "%s-id" % symmetrical_dim
    dim_key_name = "%s-id" % dim

    print "Get %s now..." % file_name
    with open(file_name, 'w') as fwrite:
        for item in dao.get_all(symmetrical_dim):
            dim_items = dao.get_all("wishlist", **{symmetrical_key_name: item["id"]})
            wish_list = [dim_item[dim_key_name] for dim_item in dim_items]
            fwrite.write('%s\n' % (' '.join(wish_list)))

            good_review = []
            bad_review = []
            for review_item in dao.get_all("review", **{symmetrical_key_name: item["id"]}):
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

            if len(good_review) > 0:
                fwrite.write('%s\n' % (' '.join(good_review)))
            if len(bad_review) > 0:
                fwrite.write('%s\n' % (' '.join(bad_review)))


def main():
    start_time = time.time()
    with futures.ThreadPoolExecutor(2) as pool:
        pool.map(get_lists, ["shop", "member"])
    print "Process finished, time consuming %f seconds." % (time.time() - start_time)


if __name__ == '__main__':
    main()