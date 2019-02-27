import sys
import requests
from pyquery import PyQuery as pq
# from lxml import etree
import logging
import time
import numpy
import argparse
from argparse import RawTextHelpFormatter
import json

# pip3 install requests numpy pyquery

# for network debug
# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def process_abnormal_character(s):
    return "".join(s.split())


ua_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15"
]

common_headers = {'User-Agent': ua_list[0],
                  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
                  }


def start_spider(book_url, proxy={}):
    # 爬图书标题、作者、目录等信息
    infos = spider_book_detail(book_url, proxy)
    chapter_list = infos["chapter_list"]
    print("获取图书信息：", chapter_list)
    # 伪装睡眠
    sleep_time = abs(100*numpy.random.normal())
    print("伪装睡眠：", sleep_time, "秒")
    time.sleep(sleep_time)

    # 遍历目录爬取
    for chapter in chapter_list:
        content_title = chapter[0]
        content_url = chapter[1]
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
              "开始爬：", chapter[0], chapter[1])
        # 爬文章内容
        res = spider_content(content_url, book_url, proxy)
        print("爬到标题："+res[0])
        print("爬到内容：", res[1])
        # 伪装睡眠
        sleep_time = abs(len(res[1])/30*numpy.random.normal())
        print("伪装睡眠：", sleep_time, "秒")
        time.sleep(sleep_time)


"""
返回图书信息，章节
Returns:
    dict -- 包括
    {
        "title": 书名,
        "author": 作者,
        "last_chapter_info": 最新更新章节的信息,
        "words": 小说字数,
        "update_status": 状态（连载中）,
        "update_time": 最近更新事件,
        "chapter_list": [(章节标题1, 内容url), (章节标题1, 内容url), (章节标题1, 内容url)......],
    }
"""


def spider_book_detail(book_url, proxy={}):
    book_headers = {**common_headers, 'If-None-Match': str(int(time.time()))}
    response = requests.get(book_url, headers=book_headers, proxies=proxy)
    doc = pq(response.content)
    # doc = pq(etree.fromstring(response.text))
    # print(doc.html())
    infos = parse_book_info(doc)
    chapter_list = parse_chapter_list(doc, book_url)
    infos["chapter_list"] = chapter_list
    infos["book_url"] = book_url
    return infos


def parse_book_info(doc):
    title = doc(".bookinfo .btitle")("h1").text()
    author = doc(".bookinfo .btitle")("em")[0].text
    last_chapter_info = doc(".bookinfo .stats .fl").text()
    # doc(".bookinfo .stats .fr").text()
    number_words, status, update_time = [
        e.text for e in doc(".bookinfo .stats .fr i")]
    book_intro = process_abnormal_character(doc(".bookinfo .intro").text())
    return {
        "title": title,
        "author": author,
        "last_chapter_info": last_chapter_info,
        "words": number_words,
        "update_status": status,
        "update_time": update_time,
        "book_intro": book_intro
    }


def parse_chapter_list(doc, book_url):
    chapter_list = [(e.text(), e.attr['href']) for e in doc(
        ".chapterlist dd a").make_links_absolute(book_url).items()]
    return chapter_list


"""
获取章节内容
输入：章节url，图书url(非必需)
Returns:
    (标题, 内容)
"""


def spider_content(content_url, book_url="", proxy={}):
    content_headers = {**common_headers,
                       "Referer": book_url, "Cache-Control": "max-age=0"}
    try:
        content_response = requests.get(content_url, headers=content_headers, proxies=proxy)
    except requests.RequestException as e:
        return (None, None)
    content_doc = pq(content_response.content)
    content_title = content_doc(".article h1").text()
    content = content_doc(".article #BookText").text()
    return (content_title, content)


# 设置重试次数
# from requests.adapters import HTTPAdapter
# s = requests.Session()
# s.mount('http://www.aoyuge.com', HTTPAdapter(max_retries=50))


def check_args():
    pass
    desc = """
    爬虫命令 基本的格式是 `python ./spider-origin.py cmd json_arg` ，其中

    参数说明
    * cmd表示爬虫执行的命令包括detail和content，
        * detail 是获取图书信息，如书名，作者，章节列表，更新时间等
        * content 是获取具体的小说内容

    * json_arg是json的字符串作为cmd命令的参数
        * 当cmd==detail，必传参数为图书目录页的url 
            ```json
            {
                "book_url": "http://www.xxx.com/xxxx"
            }
            ```
        * 当cmd==content，必传参数为章节页面完整的content_url，注意这里统一用的list
            * content_url是list（一个内容也可以）
                ```json
                {
                    "content_url": [
                        "http://www.book.com/123/1.html"
                    ]
                }
                ```
            * 支持多个url，可选参数加上可选的book_url,这个有些爬虫用来作为refer,interval为-1时随机间隔，单位ms（默认无间隔0）
                ```json
                {
                    "content_url": [
                    "http://www.book.com/123/1.html",
                    "http://www.book.com/123/2.html"
                    ],
                    "interval": -1,
                    "book_url": "http://www.book.com/123/"
                }
                ```
    返回值说明：
    * cmd==detail：返回类型如下
        ```json
        {   
            "title": 书名,
            "chapter_list": [(章节标题1, 内容完整url), (章节标题1, 内容完整url), (章节标题1, 内容完整url)......]
            "author": 作者,
            "last_chapter_info": 最新更新章节的信息,
            "words": 小说字数,
            "update_status": 状态（连载中）,
            "update_time": 最近更新事件,
            "book_intro",小说介绍, 
            "book_icon",小说图标
        }
        ```

    * cmd==content：返回类型如下,约定contents的长度与输入的content_url相同，如果没爬到，则改项为`{}`，数组长度不变
        ```json
        {
            "contents": [
                {
                "title": "章节标题1",
                "content": "章节内容1"
                },
                {
                "title": "章节标题2",
                "content": "章节内容2"
                }
            ]
        }
        ```

    """
    parser = argparse.ArgumentParser(
        description=desc, formatter_class=RawTextHelpFormatter)
    parser.add_argument('cmd', help="""命令：可选包括detail或者content""", choices=[
                        "detail", "content", "test"])
    parser.add_argument('json_arg', help="""命令参数,是一个json""")

    args = parser.parse_args()
    cmd = args.cmd
    try:
        cmd_arg = json.loads(args.json_arg)
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        eprint("spider:", "解析 JSON 参数失败(看看是不是引号问题):", args.json_arg)
        exit(-1)

    if cmd == "detail" and 'book_url' not in cmd_arg:
        eprint("spider:", cmd + "没有包含 book_url，当前参数为：" + str(cmd_arg))
        exit(-1)
    elif cmd == "content" and 'content_url' not in cmd_arg:
        eprint("spider:", cmd + "没有包含 content_url，当前参数为：" + str(cmd_arg))
        exit(-1)

    return (cmd, cmd_arg)


# 输出两个部分
(cmd, cmd_arg) = check_args()
if cmd == "detail":
    eprint("spider:"+cmd + " " + str(cmd_arg))
    proxy = cmd_arg.get("proxy", {})
    res = spider_book_detail(cmd_arg['book_url'], proxy)
    print(json.dumps(res, ensure_ascii=False))
elif cmd == "content":

    book_url = cmd_arg.get("book_url", "")
    interval = int(cmd_arg.get("interval", "0"))
    proxy = cmd_arg.get("proxy", {})

    eprint("spider:"+cmd + " " + str(cmd_arg), book_url,
           cmd_arg['content_url'], "间隔:", interval)
    res = dict({"contents": []})

    for i, url in enumerate(cmd_arg['content_url']):
        eprint("spider:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
               "开始爬第", i, "个：", url)
        t, c = spider_content(url, book_url, proxy)
        if t is not None:
            res["contents"].append({
                "title": t,
                "content": c
            })
        else:
            res["contents"].append({
            })
        sleep_time = interval
        if interval == -1:
            if c is None:
                c = ""
            sleep_time = abs(max(300, len(c))/30*numpy.random.normal())
        if i != (len(cmd_arg['content_url'])-1) and sleep_time > 0:
            eprint("spider:", "爬到：", t)
            eprint("spider:", "伪装睡眠：", sleep_time, "秒",)
            time.sleep(sleep_time)

    print(json.dumps(res, ensure_ascii=False))
    # if 'book_url' in cmd_arg:
    #     spider_content(cmd_arg['book_url'],cmd_arg)
    # else:
    #     spider_content(cmd_arg['book_url'])
elif cmd == "test":
    if 'book_url' in cmd_arg:
        start_spider(cmd_arg['book_url'])
    pass


# start_spider('http://www.aoyuge.com/34/34380/index.html')
# proxies = {
#   "http": "http://10.10.1.10:3128",
#   "https": "http://10.10.1.10:1080",
# }
# r=requests.get("http://icanhazip.com", proxies=proxies)

# print(response.content.decode('gbk'))

# 测试UA

# GET /book-34380.html HTTP/1.1
# Host: www.aoyuge.com
# Connection: keep-alive
# Cache-Control: max-age=0
# Upgrade-Insecure-Requests: 1
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36
# Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
# Referer: http://www.aoyuge.com/modules/article/search.php?searchkey=%C5%AE%B5%DB
# Accept-Encoding: gzip, deflate
# Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
# Cookie: Hm_lvt_7528740976f50394455eb0cdea6d3526=1548319096; jieqiVisitTime=jieqiArticlesearchTime%3D1548321581; jieqiVisitId=article_articleviews%3D34380; Hm_lpvt_7528740976f50394455eb0cdea6d3526=1548321454
# If-None-Match: 1548321589|


# GET /book-34380.html HTTP/1.1
# Host: www.aoyuge.com
# Connection: keep-alive
# Upgrade-Insecure-Requests: 1
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36
# Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
# Accept-Encoding: gzip, deflate
# Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
# Cookie: Hm_lvt_7528740976f50394455eb0cdea6d3526=1548319096; jieqiVisitTime=jieqiArticlesearchTime%3D1548321581; jieqiVisitId=article_articleviews%3D34380; Hm_lpvt_7528740976f50394455eb0cdea6d3526=1548321461
# If-None-Match: 1548321595|


# conent
# Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
# Accept-Encoding: gzip, deflate
# Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
# Cache-Control: max-age=0
# Connection: keep-alive
# Cookie: Hm_lvt_7528740976f50394455eb0cdea6d3526=1548319096; Hm_lpvt_7528740976f50394455eb0cdea6d3526=1548333881
# Host: www.aoyuge.com
# Referer: http://www.aoyuge.com/34/34380/index.html
# Upgrade-Insecure-Requests: 1
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36
