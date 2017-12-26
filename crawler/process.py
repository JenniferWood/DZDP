import crawler_future

if __name__ == "__main__":
    obj = crawler_future.CrawlerClass("dzdp")

    count = 0
    for item in obj.get_entry("wishlist_f"):
        count += 1
        if count % 1000 == 0:
            print count
        member_id = item["member-id"]
        wish_list = item["wishlist"]
        for shop in wish_list:
            json = {"member-id": member_id, "shop-id": shop}
            if obj.exists_by_key("wishlist", json):
                continue
            obj.insert("wishlist", json)
    print count
