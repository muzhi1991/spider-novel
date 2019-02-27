import os
import sys
import json
import time
import numpy

import zlib
import pymysql
import redis
import requests
import traceback
import subprocess


query_list = [
    {"book_url": "http://www.aoyuge.com/34/34380/index.html",
     "spider_name": "spider-origin.sh"},
     {"book_url": "http://www.aoyuge.com/15/15779/index.html",
     "spider_name": "spider-origin.sh"}
]

origintype = 2  # 凡是用网页爬虫的来源类型都定义为2
# output= os.popen("""python3 ./spider-origin.py detail '{
#   "book_url": "http://www.aoyuge.com/34/34380/index.html"
# }'""")

# 提交内容的地址
POST_CONTENT_URL = "https://test.hotreader.ml/ebook/incontent"

# 证书位置
BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CACERT_PATH = BASE_PATH+"/cert/cacert.pem"
CLIENT_CRT_PATH = BASE_PATH+"/cert/client.crt"
CLIENT_KEY_PATH = BASE_PATH+"/cert/client.key"

SCRIPT_DIR_PATH = BASE_PATH+"/scripts"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def start_spider_detail(query_list):
    bookIdList = []
    for query_item in query_list:
        spider_name = query_item['spider_name']
        book_url = query_item['book_url']
        cmd_detail = """sh %s detail '{
        "book_url": "%s"
        }'""" % (os.path.join(SCRIPT_DIR_PATH, spider_name), book_url)
        try:
            output = os.popen(cmd_detail)
            x = output.read()

            # infos=json.loads(x)
            # chapter_list = infos["chapter_list"]
            # print(chapter_list)

            print("start_spider_detail:", "使用 ",
                  spider_name, "查询：", book_url, " 成功")
            # 入库 detail
            bookId = abstractInDB(book_url, x)
            bookIdList.append({**query_item, 'bookId': bookId})
            print("start_spider_detail:",  "入库图书目录：", bookId, " 成功")

        except Exception as e:
            eprint("start_spider_detail:", e)
            traceback.print_exc()
            bookIdList.append(None)
    return bookIdList
    # eprint(e)


def start_spider_content(bookId, spider_name, interval=-1, book_url=""):
    need_spider_chapters = getSpiderList(bookId)
    if len(need_spider_chapters) == 0:
        print("start_spider_content: 所有目录中的文章已经爬取完成")
        return
    for i, chapter in enumerate(need_spider_chapters):
        chapter_id = chapter['chapterId']
        content_title = chapter['title']
        content_url = chapter['url']
        print("start_spider_content:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
              "开始爬：", chapter_id, content_title, content_url)
        # 爬文章内容
        # interval=-1 随机间隔，这个0
        cmd_content = """sh %s content '{
                        "content_url": [
                        "%s"
                        ],
                        "interval": 0,
                        "book_url": "%s"
                    }'""" % (os.path.join(SCRIPT_DIR_PATH, spider_name), content_url, book_url)
        eprint("start_spider_content:", cmd_content)
        x_content = None
        try:
            output_content = os.popen(cmd_content)
            x_content = output_content.read()
            res = json.loads(x_content)
            # for item in res['contents']:
            #     print("爬到标题：", item['title'])
            #     print("爬到内容：", item['content'])
            #     # 入库逻辑
            if len(res['contents']) == 1:
                item = res['contents'][0]
                if 'content' in item and 'title' in item:
                    print("start_spider_content:", "爬到标题：", item['title'])
                    print("start_spider_content:", "爬到内容长度:", len(
                        item['content']), "内容预览：", item['content'][:25], "......")
                    postContentIntoDb(bookId, chapter_id,
                                      item['title'], item['content'])
                else:
                    raise Exception('标题或者内容返回了空')
            else:
                raise Exception('这里应该返回一个值')
        except Exception as e:
            eprint("start_spider_content:", e)
            traceback.print_exc()

        # 伪装睡眠
        sleep_time = interval
        if interval == -1:
            if x_content is None:
                x_content = ""
            sleep_time = abs(max(300, len(x_content))/30*numpy.random.normal())
        if i != (len(need_spider_chapters)-1) and sleep_time > 0:
            print("start_spider_content:", "伪装睡眠：", sleep_time, "秒")
            time.sleep(sleep_time)
    return


def start_spider_content_distributed(book_id, spider_name, interval=-1, book_url=""):
    need_spider_chapters = getSpiderList(book_id)
    if len(need_spider_chapters) == 0:
        print("start_spider_content_distributed: 所有目录中的文章已经爬取完成")
        return
    chunk_size = 3
    chunks = [need_spider_chapters[x:x+chunk_size]
              for x in range(0, len(need_spider_chapters), chunk_size)]
    print("start_spider_content_distributed:需要爬取", len(need_spider_chapters), "个章节;", "一共要提交", len(
        chunks), "个任务;", "每个任务平均爬取", chunk_size, "个章节")
    for i, chunk in enumerate(chunks):
        content_url_list = [item['url'] for item in chunk]
        chapter_id_list = [item['chapterId'] for item in chunk]
        arg_dict = {
            "content_url": content_url_list,
            "interval": interval,
            "book_url": book_url,
            "book_id": book_id,
            "chapter_id": chapter_id_list
        }
        arg_str = json.dumps(arg_dict, ensure_ascii=False)
        print("start_spider_content_distributed:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
              "提交第" + str(i)+"/"+str(len(chunks)) + "个任务，大小为" + str(len(chunk)), "; 具体信息:", book_id, chapter_id_list, content_url_list)
        # cmd_content = """arg='%s' && java -cp %s worker.Main %s content '$arg' '-workerTimeout  1000000' """ \
        # % (arg_str, "./akka-distributed-workers-assembly-1.0.jar", spider_name)

        # cmd_content = """arg='%s' && echo $arg """ % (arg_str)
        # eprint("start_spider_content_distributed: cmd:", cmd_content)
        try:
            # output_content = os.popen(cmd_content)
            # x_content = output_content.read()
            output_content = subprocess.run(["java", "-cp", "./akka-distributed-workers-assembly-1.0.jar",
                                             "worker.Main", spider_name, "content", arg_str, '-workerTimeout 1000000'], stdout=subprocess.PIPE)
            print("start_spider_content_distributed: result:",
                  output_content.returncode, output_content.stdout)
        except Exception as e:
            eprint("start_spider_content_distributed:", e)
            traceback.print_exc()
    return


# 获取需要爬去的章节（过滤掉content表中已经存在的）
def getSpiderList(bookId):
    sql = "select chapterList from t_bookabstract where online=1 and bookId=%s"
    db = pymysql.connect("localhost", "root",
                         "MyBook@123456", "bookzip", charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql, bookId)
    db.commit()
    res = cursor.fetchone()
    if res is None:
        return []
    (chapterList,) = res
    chapters = json.loads(zlib.decompress(
        chapterList, wbits=zlib.MAX_WBITS | 32))
    chapterIdList = [item['chapterId'] for item in chapters]

    # 查询content表&&求差集
    table = bookId % 128
    queryInSql = "select chapterId from t_bookcontent_%s where bookId=%d and chapterId in (%s)" % (
        table, bookId, ",".join(map(str, chapterIdList)))
    cursor.execute(queryInSql)
    db.commit()
    res = cursor.fetchall()
    if res is None:
        return chapters
    needSpiderID = set(chapterIdList)-set([e[0] for e in res])
    return [item for item in chapters if item['chapterId'] in needSpiderID]


def prepareHandle(bookName):
    # 先查询是否存在该书名，有的话就复制一下id
    sql = "select bookId,bookIcon,bookType,typeId from t_bookabstract where online=1 and bookName=%s"
    db = pymysql.connect("localhost", "root",
                         "MyBook@123456", "bookzip", charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql, bookName)
    db.commit()
    id = 0
    icon = ""
    bookType = "都市"
    typeId = 6
    result = cursor.fetchone()
    db.close()
    eprint(result)
    if(result == None):
        r = redis.Redis(host='127.0.0.1', port=6379, password='MyBook@123456')
        isExist = r.exists("appnovel_bookId_autogen_20190215")
        if(isExist == 0):
            r.set('appnovel_bookId_autogen_20190215', 19999)
        id = r.incr('appnovel_bookId_autogen_20190215')
        return (id, icon, bookType, typeId)
    else:
        return result


def chapterListHandle(chapterList, bookId):
    result_list = []
    chapterId = 1
    for each in chapterList:
        node = {
            "bookId": bookId,
            "chapterId": chapterId,
            "url": each[1],
            "type": origintype,
            "title": each[0],
        }
        chapterId += 1
        result_list.append(node)
    return {
        "list": result_list
    }


def abstractInDB(book_url, abstract):
    data = json.loads(abstract)
    bookName = data["title"]
    author = data["author"]
    words = data["words"]
    status = data["update_status"]
    updateTime = data["update_time"]
    desc = data["book_intro"]
    bookUrl = book_url
    (bookId, bookIcon, bookType, typeId) = prepareHandle(bookName)
    clist = data["chapter_list"]
    jlist = chapterListHandle(clist, bookId)
    ct = bytes(json.dumps(jlist["list"]), encoding="utf8")
    chapterList = zlib.compress(ct)
    sql = "replace into t_bookabstract   (bookName,bookId,bookAuthor,bookDesc,bookWords,bookType,typeId,updateTime,origintype,sourceurl,online,chapterList)\
 VALUES('%s',%d,'%s','%s',%d,'%s',%d,'%s',%d,'%s',%d" % (bookName, bookId, author, desc, int(words), bookType, typeId, updateTime, origintype, bookUrl, 1)
    sql += ",%s)"
    # 打开数据库连接
    db = pymysql.connect("localhost", "root",
                         "MyBook@123456", "bookzip", charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    cursor.execute(sql, chapterList)
    db.commit()
    result = cursor.fetchone()
    offlinemysql = "update t_bookabstract set online=0 where bookId=%d and sourceurl!='%s'" % (
        bookId, bookUrl)
    cursor.execute(offlinemysql)
    db.commit()
    offlineresult = cursor.fetchone()
    # 关闭数据库连接
    db.close()
    return bookId


def postContentIntoDb(book_id, chapter_id, title, content):
    payload = "bookId=%d&chapterId=%d&title=%s&content=%s" % (
        book_id, chapter_id, title, content)
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
    }

    response = requests.request("POST", POST_CONTENT_URL, data=payload.encode(
        "utf-8"), headers=headers, cert=(CLIENT_CRT_PATH, CLIENT_KEY_PATH), verify=CACERT_PATH)
    print("postContentIntoDb:", "提交数据库成功-", "bookid:", book_id,
          "chapter_id:", chapter_id, "title:", title, "\nreponse:", response.text)


bookIdList = start_spider_detail(query_list)
for item in bookIdList:
    start_spider_content(
        item['bookId'], item['spider_name'], -1, item['book_url'])
