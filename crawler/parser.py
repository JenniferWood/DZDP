# -*- coding: utf-8 -*-  

import re
import random
import math
from urldata import UrlData
from urlparse import urljoin


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
            time = "2017-%s" % time
        return time

    def expand_by_page(self, max_page, page_format, _type, _col, _id):
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
        # print "SHOP:",self.soup.text
        if self.soup.find(class_="shop-closed") is not None:
            self.skip = True
            return []

        breadcrumb = self.soup.find(class_="breadcrumb")
        categories = breadcrumb('a')
        if categories[0].text.strip() != u'北京美食':
            self.skip = True
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
        self._next_links.append(
            UrlData(self._url_data.url+"/review_all", self._url_data.url, type="shop", 
                    collection="review", id=self._url_data.id))

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

        user_tags = []
        for em in self.soup.select("#J_usertag em"):
            user_tags.append(em.text.strip())

        user_time = self.soup.select(".user-time p")
        member["contri-value"] = int(user_time[0].select("#J_col_exp")[0].text.strip())

        register_date_res = self.search_by_regex(r'(\d{4}-\d{2}-\d{2})', user_time[2].text)
        member["register-date"] = register_date_res[0]

        return [member]

    def update_links(self):
        member_reviews_url = self._url_data.url+"/reviews?reviewCityId=2&reviewShopType=10"
        self._next_links.append(
            UrlData(member_reviews_url, self._url_data.url, type="member", collection="review", id=self._url_data.id))
        member_wish_url = self._url_data.url+"/wishlists?favorTag=s10_c2_t-1"
        self._next_links.append(
            UrlData(member_wish_url, self._url_data.url, type="member", collection="wishlist", id=self._url_data.id))


class ReviewParser(ParserFactory):
    def parse(self):
        review = {"id": self._url_data.id}

        nav_w = self.soup.select(".nav_w .B")
        if len(nav_w) == 0 or nav_w[0].text.strip() != u'北京美食':
            self.skip = True
            return []

        id_pattern = r'(\d+)'

        # shop id
        shop_info = nav_w[-1]
        shop_id_res = self.search_by_regex(id_pattern, shop_info['href'])
        review["shop-id"] = shop_id_res[0]

        cont_list = self.soup.find(class_="cont_list J_reviewRoot")
        
        # member id
        reviewer_info = cont_list.select(".B")[0]
        reviewer_id_res = self.search_by_regex(id_pattern, reviewer_info['href'])
        review["reviewer-id"] = reviewer_id_res[0]

        # review score info
        start_block = cont_list.find(class_=re.compile('msstar'))
        if start_block is not None:
            star_str = cont_list.find(class_=re.compile('msstar'))['class'][0][-2:]
            review["star"] = float(star_str)/10.0

        score = []
        comment_rest = cont_list.select(".comment-rst .rst")
        for rst in comment_rest:
            score_res = self.search_by_regex(id_pattern, rst.text)
            if score_res is None:
                score.append(0.0)
            else:
                score.append(float(score_res[0]))
        review["score"] = score
        
        # review text
        review["comment"] = cont_list.find(class_="cont_list-con").text.strip()

        # pay
        comment_unit = cont_list.select(".comment-unit li")
        pay_pattern = r'人均.+(\d+)元'
        for e in comment_unit:
            extra_comment_str = e.text.strip()
            pay_str_res = self.search_by_regex(pay_pattern, extra_comment_str)
            if pay_str_res is not None:
                review["pay"] = int(pay_str_res[0])
                break

        cont_list_fn = self.soup.find(class_="cont_list-fn")

        # create-time
        review["create-time"] = self.supplement_time_format(cont_list_fn.li.text.strip())
        
        # heart-num
        heart_id = "btnFlower%s" % self._url_data.id
        heart_res = self.search_by_regex(id_pattern, cont_list_fn.find(id=heart_id).text.strip())
        if heart_res is None:
            review["heart-num"] = 0
        else:
            review["heart-num"] = int(heart_res[0])

        return [review]


class ShopReviewsParser(ParserFactory):
    def parse(self):
        res = []
        if self.soup.find(class_="errorMessage") is not None:
            self.skip = True
            return []

        comment_list = self.soup.select(".reviews-items > ul > li")
        if len(comment_list) == 0:
            raise ValueError("The page is not what we want.")

        for comment_block in comment_list:
            review = {
                "id": comment_block.find(class_='report')['data-id'],
                "shop-id": self._url_data.id,
                "reviewer-id": comment_block.find(class_='dper-photo-aside')['data-user-id']
            }

            member_url = "http://www.dianping.com/member/%s" % review["reviewer-id"]
            self._next_links.append(
                UrlData(member_url, self._url_data.url, type="member", collection="member", id=review["reviewer-id"]))

            # Review rank
            review_rank = comment_block.find(class_="review-rank")
            start_block = review_rank.find(class_="star")
            if start_block is not None:
                review["star"] = float(start_block['class'][1][-2:]) / 10

            score_list = review_rank.select(".score .item")
            key_val_pattern = u'(.*)\s*：\s*(\d+)'
            if len(score_list) > 0:
                score = [0.0] * 3
                key_pos = {u"口味": 0, u"环境": 1, u"服务": 2}
                for _ in score_list:
                    key_val = self.search_by_regex(key_val_pattern, _.text.strip())
                    if key_val[0] not in key_pos and key_val[0] == u'人均':
                        review["pay"] = int(key_val[1])
                    else:
                        score[key_pos[key_val[0]]] = float(key_val[1])
                review["score"] = score

            # Review words
            review["comment"] = comment_block.find(class_="review-words").get_text('\n', 'br/')

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
            pass
        page_numbers = self.soup.select(".Pages a")
        if len(page_numbers) > 2:
            max_page = int(page_numbers[-2].text)
            self.expand_by_page(max_page, "?pageno=%d", "shop", "review", self._url_data.id)


class MemberReviewsParser(ParserFactory):
    def parse(self):
        res = []

        comment_list = self.soup.select(".txt.J_rptlist")
        for comment_block in comment_list:
            j_report = comment_block.find(class_="j_report")
            shop_id = str(j_report["data-sid"])
            shop_url = comment_block.find(class_="J_rpttitle")["href"]
            self._next_links.append(UrlData(shop_url, self._url_data.url, type="shop", collection="shop", id=shop_id))

            review_id = j_report["data-id"]

            review = {
                "shop-id": shop_id,
                "reviewer-id": self._url_data.id,
                "id": review_id}
            score_spans = comment_block.select(".mode-tc.comm-rst span")
            if len(score_spans) == 0 or not score_spans[0].has_attr('class'):
                review_url = "http://www.dianping.com/review/%s" % review_id
                self._next_links.append(
                    UrlData(review_url, self._url_data.url, type="review", collection="review", id=review_id))
                continue

            star_str = score_spans[0]['class'][1][-2:]
            review["star"] = float(star_str)/10.0

            if len(score_spans) > 1:
                pay = self.search_by_regex(r'(\d+)', score_spans[1].text.strip())[0]
                review["pay"] = int(pay)
            review["comment"] = comment_block.find(class_="mode-tc comm-entry").text.strip()

            review["create-time"] = self.search_by_regex(
                r'(\d+)',
                comment_block.find(class_="mode-tc info").span.text.strip())[0]
            res.append(review)

        return res

    def update_links(self):
        if self._url_data.url.find('pg') != -1:
            pass
        page_numbers = self.soup.select(".pages-num a")
        if len(page_numbers) > 2:
            max_page = int(page_numbers[-2].text)
            self.expand_by_page(
                max_page,
                "?pg=%d&reviewCityId=2&reviewShopType=10", "member", "review", self._url_data.id)


class MemberWishlistParser(ParserFactory):
    def parse(self):
        res = []
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
                UrlData(shop_url, self._url_data.url, type="shop", collection="shop", id=favor["shop-id"]))

            res.append(favor)

        return res

    def update_links(self):
        if self._url_data.url.find('pg') != -1:
            pass
        page_numbers = self.soup.select(".pages-num a")
        if len(page_numbers) > 2:
            max_page = int(page_numbers[-2].text)
            self.expand_by_page(max_page, "?pg=%d&favorTag=s10_c2_t-1", "member", "wishlist", self._url_data.id)


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
