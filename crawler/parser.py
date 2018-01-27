# -*- coding: utf-8 -*-  

import re
import random
import math
from urldata import UrlData
from urlparse import urljoin

REVIEW = "review"
SHOP = "shop"
MEMBER = "member"
WISH_LIST = "wishlist"

THIS_YEAR = 2018

key_map = {u"口味": "flavor", u"环境": "env", u"服务": "service"}
des_value = {u"非常好": 5.0, u"很好": 4.0, u"好": 3.0, u"一般": 2.0, u"差": 1.0, u"很差": 1.0}


class ParserFactory:
    def __init__(self, url_data, soup):
        self._url_data = url_data
        self._next_links = []

        self.soup = soup
        self.skip = False

    def get_links(self):
        self.update_links()
        random.shuffle(self._next_links)
        return self._next_links

    @staticmethod
    def search_by_regex(pattern, text):
        pattern = re.compile(pattern)
        res = re.search(pattern, text)
        if res is None:
            return None
        return res.groups()

    @staticmethod
    def supplement_time_format(time):
        if time.count('-') == 2:
            if time.index('-') == 2:
                time = "20%s" % time
        else:
            time = "%d-%s" % (THIS_YEAR, time)
        return time

    def expand_by_page_randomly(self, max_page, page_format, _type, _col, _id):
        prob = random.uniform(0.5, 1)
        page_limit = int(math.ceil(max_page * prob))
        
        last_pg = 1
        for i in range(1, page_limit):
            pg = random.randint(2, max_page)
            pg_url = page_format % pg
            pg_url = urljoin(self._url_data.url, pg_url)
            ref = page_format % last_pg
            ref = urljoin(self._url_data.url, ref)

            self._next_links.append(UrlData(pg_url, ref, type=_type, collection=_col, id=_id))

            last_pg = pg

    def expand_by_page(self, max_page, page_format, _type, _col, _id):
        for pg in range(2, max_page+1):
            pg_url = page_format % pg
            pg_url = urljoin(self._url_data.url, pg_url)
            ref = page_format % (pg-1)
            ref = urljoin(self._url_data.url, ref)

            self._next_links.append(UrlData(pg_url, ref, type=_type, collection=_col, id=_id))

    def parse(self):
        return []

    def update_links(self):
        if self.skip:
            return
        links = self.soup('a')
        for link in links:
            if link.has_attr('href'):
                url = urljoin(self._url_data.url, link['href'])
                # ignore invalid url
                if url.find("'") != -1:
                    continue

                url = url.split('#')[0]
                # ignore picture url
                if url.endswith(('jpg', 'jpeg', 'svg', 'png', 'gif', 'bmp')):
                    continue

                url_data = UrlData(url, self._url_data.url)
                if url_data.type == '':
                    continue
                if url_data.type in ['shop', 'member', 'review'] and url_data.collection == '':
                    continue

                self._next_links.append(url_data)


class ShopParser(ParserFactory):
    def parse(self):
        if self.soup.find(class_="shop-closed"):
            self.skip = True
            print "Shop %s is closed. -> %s" % (self._url_data.id, self._url_data.url)
            return []

        breadcrumb = self.soup.find(class_="breadcrumb")
        categories = breadcrumb('a')
        if categories[0].text.strip() != u'北京美食':
            self.skip = True
            print "Shop %s is not in Beijing. -> %s" % (self._url_data.id, self._url_data.url)
            return []

        # How to optimize this 'category' thing?
        shop = {
            "id": self._url_data.id,
            "category": categories[1].text.strip(),
            "district": categories[2].text.strip(),
            "full-name": breadcrumb.span.text.strip(),
            "name": self.search_by_regex(r'shopName:\s*"(.*)",', self.soup.text)[0],
        }

        script_pattern = r'%s:\s*"([^"]*)",'
        shop.update({
            "name": self.search_by_regex(script_pattern % "shopName", self.soup.text)[0],
            "coordinate": [
                float(self.search_by_regex(script_pattern % "shopGlat", self.soup.text)[0]),
                float(self.search_by_regex(script_pattern % "shopGlng", self.soup.text)[0])
            ]
        })

        return [shop]

    def update_links(self):
        if self.skip:
            return

        for review_type in ["good", "middle", "bad"]:
            review_suffix = "queryType=reviewGrade&queryVal=%s" % review_type
            self._next_links.append(
                UrlData("%s/review_all?%s" % (self._url_data.url, review_suffix),
                        self._url_data.url,
                        type=SHOP, collection=REVIEW, id=self._url_data.id, suffix=review_suffix))

        links = self.soup.find_all(attrs={"itemprop": "url"})
        for link in links:
            self._next_links.append(UrlData(link['href'], self._url_data.url))


class MemberParser(ParserFactory):
    def parse(self):
        member = {
            "id": self._url_data.id,
            "gender": 0,
            "is-vip": (self.soup.find(class_="vip").a is not None),
            "name": self.soup.select(".tit .name")[0].text.strip()
        }

        if self.soup.find(class_="woman"):
            member["gender"] = 2
        elif self.soup.find(class_="man"):
            member["gender"] = 1

        user_tags = [em.text.strip() for em in self.soup.select("#J_usertag em")]
        if user_tags:
            member["tags"] = user_tags

        user_time = self.soup.select(".user-time p")
        member["contri-value"] = int(user_time[0].select("#J_col_exp")[0].text.strip())

        register_date_res = self.search_by_regex(r'(\d{4}-\d{2}-\d{2})', user_time[2].text)
        member["register-date"] = register_date_res[0]

        return [member]

    def update_links(self):
        member_reviews_url = self._url_data.url+"/reviews?reviewCityId=2&reviewShopType=10"
        self._next_links.append(
            UrlData(member_reviews_url, self._url_data.url, type=MEMBER, collection=REVIEW, id=self._url_data.id))
        member_wish_url = self._url_data.url+"/wishlists?favorTag=s10_c2_t-1"
        self._next_links.append(
            UrlData(member_wish_url, self._url_data.url, type=MEMBER, collection=WISH_LIST, id=self._url_data.id))


class ReviewParser(ParserFactory):
    def parse(self):
        if self.soup.find(class_="not-found"):
            raise ValueError("Crawler has been captured !!!")

        review = {"id": self._url_data.id}

        nav_w = self.soup.select(".detail-crumb a")
        if nav_w[0].text.strip() != u'北京美食':
            self.skip = True
            print "Review %s is not for shop in Beijing. -> %s" % (self._url_data.id, self._url_data.url)
            return []

        id_pattern = r'(\d+)'

        # shop id
        shop_info = nav_w[-1]
        shop_id_res = self.search_by_regex(id_pattern, shop_info['href'])
        review["shop-id"] = shop_id_res[0]

        review_content_block = self.soup.find(class_="review-content")
        
        # member id
        reviewer_info = review_content_block.find(class_="dper-photo-aside")
        review["member-id"] = reviewer_info["data-user-id"]

        # Review rank
        review_rank = review_content_block.find(class_="review-rank")
        star_block = review_rank.find(class_="star")
        if star_block is not None:
            review["star"] = float(star_block['class'][1][-2:]) / 10

        score_list = review_rank.select(".score .item")
        key_val_pattern = u'(.+)\s*[：:]\s*(.+)'
        if len(score_list) > 0:
            for _ in score_list:
                key_val = self.search_by_regex(key_val_pattern, _.text.strip())
                if key_val[0] not in key_map and key_val[0] == u'人均':
                    review["pay"] = int(self.search_by_regex(r'(\d+)', key_val[1])[0])
                else:
                    review[key_map[key_val[0]]] = des_value[key_val[1]]

        # Review words
        review["comment"] = review_content_block.find(class_="review-words").get_text(' ', 'br/')

        # Commend
        recommend_block = review_content_block.find(class_="review-recommend")
        if recommend_block:
            review["recommend"] = [dish.text.strip() for dish in recommend_block.select(".col-exp")]

        # Time
        time_raw_str = review_content_block.find(class_="time").text.strip()
        times = time_raw_str.split(u'更新于')
        review["create-time"] = self.supplement_time_format(times[0].strip())
        if len(times) > 1:
            review["update-time"] = self.supplement_time_format(times[1])

        # Heart
        heart_num_block = review_content_block.find(class_="favor").find_previous_sibling('em')
        if heart_num_block is not None:
            review["heart-num"] = int(heart_num_block.text.strip("(|)"))
        else:
            review["heart-num"] = 0

        return [review]


class ShopReviewsParser(ParserFactory):
    def parse(self):
        if self.soup.find(class_="not-found"):
            raise ValueError("Crawler has been captured !!!")

        res = []
        if self.soup.find(class_="errorMessage") is not None:
            self.skip = True
            print "Shop %s is closed. -> %s" % (self._url_data.id, self._url_data.url)
            return []

        nav_w = self.soup.select(".list-crumb a")
        if nav_w[0].text.strip() != u'北京美食':
            self.skip = True
            print "Review %s is not for shop in Beijing. -> %s" % (self._url_data.id, self._url_data.url)
            return []

        comment_list = self.soup.select(".reviews-items > ul > li")
        if len(comment_list) == 0:
            raise ValueError("The page is not what we want.")

        for comment_block in comment_list:
            review = {
                "id": comment_block.find(class_='report')['data-id'],
                "shop-id": self._url_data.id,
                "member-id": comment_block.find(class_='dper-photo-aside')['data-user-id']
            }

            member_url = "http://www.dianping.com/member/%s" % review["member-id"]
            self._next_links.append(
                UrlData(member_url, self._url_data.url, type=MEMBER, collection=MEMBER, id=review["member-id"]))

            # Review rank
            review_rank = comment_block.find(class_="review-rank")
            star_block = review_rank.find(class_="star")
            if star_block is not None:
                review["star"] = float(star_block['class'][1][-2:]) / 10

            score_list = review_rank.select(".score .item")
            key_val_pattern = u'(.+)\s*：\s*(.+)'
            if len(score_list) > 0:
                for _ in score_list:
                    key_val = self.search_by_regex(key_val_pattern, _.text.strip())
                    if key_val[0] not in key_map and key_val[0] == u'人均':
                        review["pay"] = int(self.search_by_regex(r'(\d+)', key_val[1])[0])
                    else:
                        review[key_map[key_val[0]]] = des_value[key_val[1]]

            # Review words
            review["comment"] = comment_block.find(class_="review-words").get_text(' ', 'br/')

            # Commend
            recommend_block = comment_block.find(class_="review-recommend")
            if recommend_block:
                review["recommend"] = [dish.text.strip() for dish in recommend_block.select(".col-exp")]

            # Time
            time_raw_str = comment_block.find(class_="time").text.strip()
            times = time_raw_str.split(u'更新于')
            review["create-time"] = self.supplement_time_format(times[0].strip())
            if len(times) > 1:
                review["update-time"] = self.supplement_time_format(times[1])

            # Heart
            heart_num_block = comment_block.find(class_="reply").find_previous_sibling('em')
            if heart_num_block is not None:
                review["heart-num"] = int(heart_num_block.text.strip("(|)"))
            else:
                review["heart-num"] = 0

            res.append(review)
        return res

    def update_links(self):
        if self._url_data.url.find('pageno') != -1:
            return
        page_numbers = self.soup.select(".reviews-pages a")
        if page_numbers:
            max_page = int(page_numbers[-2].text)
            pg_format = "?pageno=%d"
            if self._url_data.suffix:
                pg_format += "&%s" % self._url_data.suffix
            self.expand_by_page_randomly(max_page, pg_format, SHOP, REVIEW, self._url_data.id)


class MemberReviewsParser(ParserFactory):
    def parse(self):
        res = []

        comment_list = self.soup.select(".txt.J_rptlist")
        if len(comment_list) == 0:
            raise ValueError("The page is not what we want.")

        for comment_block in comment_list:
            j_report = comment_block.find(class_="j_report")
            shop_id = str(j_report["data-sid"])
            shop_url = comment_block.find(class_="J_rpttitle")["href"]
            self._next_links.append(UrlData(shop_url, self._url_data.url, type=SHOP, collection=SHOP, id=shop_id))

            review_id = j_report["data-id"]

            review = {
                "shop-id": shop_id,
                "member-id": self._url_data.id,
                "id": review_id}
            score_spans = comment_block.select(".mode-tc.comm-rst span")
            if len(score_spans) == 0 or not score_spans[0].has_attr('class'):
                review_url = "http://www.dianping.com/review/%s" % review_id
                self._next_links.append(
                    UrlData(review_url, self._url_data.url, type=REVIEW, collection=REVIEW, id=review_id))
                continue

            star_str = score_spans[0]['class'][1][-2:]
            review["star"] = float(star_str)/10.0

            if len(score_spans) > 1:
                pay = self.search_by_regex(r'(\d+)', score_spans[1].text.strip())[0]
                review["pay"] = int(pay)
            review["comment"] = comment_block.find(class_="mode-tc comm-entry").text.strip()

            create_time = comment_block.find(class_="mode-tc info").span.text.strip()
            review["create-time"] = self.supplement_time_format(create_time[3:])
            res.append(review)

        return res

    def update_links(self):
        if self._url_data.url.find('pg') != -1:
            return
        page_numbers = self.soup.select(".pages-num a")
        if len(page_numbers) > 2:
            max_page = int(page_numbers[-2].text)
            self.expand_by_page(
                max_page,
                "?pg=%d&reviewCityId=2&reviewShopType=10", MEMBER, REVIEW, self._url_data.id)


class MemberWishlistParser(ParserFactory):
    def parse(self):
        res = []
        if self.soup.select(".modebox.p-tabs-box") is None:
            raise ValueError("The page is not what we want.")

        favor_list = self.soup.select(".pic-txt.favor-list li")

        for favorShop in favor_list:
            if favorShop.find(class_="tag-stop") is not None:
                break
            favor = {
                "member-id": self._url_data.id,
                "shop-id": str(favorShop.find(class_="J_favor")["referid"])
            }

            favor_time = favorShop.find(class_="time").text.strip()
            favor["time"] = self.supplement_time_format(favor_time)

            shop_url = "http://www.dianping.com/shop/%s" % favor["shop-id"]
            self._next_links.append(
                UrlData(shop_url, self._url_data.url, type=SHOP, collection=SHOP, id=favor["shop-id"]))

            res.append(favor)

        return res

    def update_links(self):
        if self._url_data.url.find('pg') != -1:
            return
        page_numbers = self.soup.select(".pages-num a")
        if len(page_numbers) > 2:
            max_page = int(page_numbers[-2].text)
            self.expand_by_page(max_page, "?pg=%d&favorTag=s10_c2_t-1", MEMBER, WISH_LIST, self._url_data.id)


class ListParser(ParserFactory):
    def parse(self):
        pass

    def update_links(self):
        pass


def getparser(url_data, soup):
    key = "%s-%s" % (url_data.type, url_data.collection)

    class_ = {
        "review-review": ReviewParser,
        "shop-shop": ShopParser,
        "shop-review": ShopReviewsParser,
        "member-member": MemberParser,
        "member-review": MemberReviewsParser,
        "member-wishlist": MemberWishlistParser
    }.get(key, ParserFactory)

    # print "Get parse class %s" % class_
    return class_(url_data, soup)
