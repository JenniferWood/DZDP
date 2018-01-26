from mongo import MyMongoDb


if __name__ == "__main__":
    dao = MyMongoDb("dzdp")
    i = 0
    for r in dao.get_all("review"):
        if i >= 10:
            break
        print r["member-id"],r["shop-id"],r.get("star","nan"), r.get("score","nan")
        i += 1
