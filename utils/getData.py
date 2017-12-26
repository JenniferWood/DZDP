from pymongo import MongoClient as mc

shopIdPath = '../data/embedding/shop.ids'
userIdPath = '../data/embedding/user.ids'

con = mc("localhost",27017)
db = con["dzdp"]


def main():
    col_wish = db["wishlist"]
    col_review = db["review"]
    user_emb = {}

    with open(shopIdPath, 'w') as fwrite:
        i = 0
        for user in db["member"].find():
            if i % 1000 == 0: print i
            i += 1
            item = col_wish.find_one({"member-id":user["id"]})
            if item is None: continue
            wishlist = item["wishlist"]
            if len(wishlist) > 0:
                fwrite.write('%s\n' % (' '.join(wishlist)))
                for shop in wishlist:
                    user_emb.setdefault(shop,[])
                    user_emb[shop].append(user["id"])

            goodReview = []
            badReview = []
            for item in col_review.find({"reviewer-id":user["id"]}):
                shopId = item["shop-id"]
                user_emb.setdefault(shopId,[])
                user_emb[shopId].append(user["id"])

                #if db["shop"].find_one({"id":shopId}) is None: continue
                score = 0
                num = 0
                if "star" in item:
                    score = item["star"]
                    num += 1
                if "score" in item and len(item["score"]) > 0:
                    score += sum(item["score"])/3
                    num += 1
                if num > 0:
                    score /= num
                if score >= 3.0: goodReview.append(shopId)
                else: badReview.append(shopId)

            if len(goodReview) > 0:
                fwrite.write('%s\n' % (' '.join(goodReview)))
            if len(badReview) > 0:
                fwrite.write('%s\n' % (' '.join(badReview)))

    with open(userIdPath, 'w') as fwrite_1:
        for shop in user_emb:
            fwrite_1.write('%s\n' % (' '.join(user_emb[shop])))


if __name__ == '__main__':
    main()