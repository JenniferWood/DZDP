# -*- coding: utf-8 -*-

import parser
import datetime


# supplement_str_time
def test_supplement_str_time():
    now = datetime.datetime.now()
    print "Now", now

    time_list = ["10分钟前", "2小时前", "昨天15:23", "前天07:20", "17-10-23 19:00", "03-20 10:23"]
    for time in time_list:
        print time
        print parser.ParserFactory.supplement_str_time(time)


if __name__ == "__main__":
    test_supplement_str_time()