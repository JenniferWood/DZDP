from pymongo import errors
from db import mongo

if __name__ == "__main__":
    obj = mongo.MyMongoDb("dzdp")

    count, last_count = 0, 0
    while True:
        try:
            for item in obj.get_all("wishlist_f"):
                count += 1
                if count % 10 == 0:
                    print count
                member_id = item["member-id"]
                wish_list = item["wishlist"]
                for shop in wish_list:
                    json = {"member-id": member_id, "shop-id": shop}
                    if obj.exists_by_key("wishlist", json):
                        continue
                    obj.insert("wishlist", **json)
                obj.remove("wishlist_f", **item)
        except errors.CursorNotFound, ex:
            print ex
        finally:
            print count
            if count == last_count:
                break
            last_count = count
