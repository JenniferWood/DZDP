from db import mongo

if __name__ == "__main__":
    obj = mongo.MyMongoDb("dzdp")

    count = 0
    for item in obj.get_all("wishlist_f"):
        count += 1
        if count % 100 == 0:
            print count
        member_id = item["member-id"]
        wish_list = item["wishlist"]
        for shop in wish_list:
            json = {"member-id": member_id, "shop-id": shop}
            if obj.exists_by_key("wishlist", json):
                continue
            obj.insert("wishlist", **json)
        obj.remove("wishlist_f", **item)
    print count
