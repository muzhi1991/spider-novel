import asyncio
import threading

import proxy_list_async
import uuid
import sys
import time
import traceback
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor

import os
import subprocess
import json
import random
# import requests
from requests_futures.sessions import FuturesSession

# for db
import zlib
import pymysql
import redis

origintype = 2  # 凡是用网页爬虫的来源类型都定义为2

__GLOBAL_EXECUTOR__ = ThreadPoolExecutor(max_workers=50)
__GLOBAL_FUTURE_REQUEST_SESSION__ = FuturesSession(executor=__GLOBAL_EXECUTOR__)

## 如果队列里面爬的任务太对，把爬章节的内容放队列后面，控制长度
__MAX_DETAIL_WAIT_QUEUE_SIZE__ = 100000

# 提交内容的地址
POST_CONTENT_URL = "https://test.hotreader.ml/ebook/incontent"

# 证书位置
BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CACERT_PATH = BASE_PATH + "/cert/cacert.pem"
CLIENT_CRT_PATH = BASE_PATH + "/cert/client.crt"
CLIENT_KEY_PATH = BASE_PATH + "/cert/client.key"

SCRIPT_DIR_PATH = BASE_PATH + "/scripts"

sys.path.insert(0, SCRIPT_DIR_PATH)
spider_list = {
    "spider-origin": __import__("spider-origin")
}

query_list = [
    {"book_url": "http://www.aoyuge.com/34/34380/index.html",
     "spider_name": "spider-origin"},
    # {"book_url": "http://www.aoyuge.com/15/15779/index.html",
    #  "spider_name": "spider-origin"}
]


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class MyException(Exception):
    def __init__(self, message, errors, *args, **kwargs):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        self.errors = errors
        self.args = args
        self.kwargs = kwargs


class ProxyAvailableException(MyException):
    def __init__(self, message, *args, **kwargs):
        super().__init__(message, Exception("need to proxy switch"), *args, **kwargs)


class SpiderTask:
    loop = None
    executor = None

    def __init__(self, loop, executor):
        SpiderTask.loop = loop
        SpiderTask.executor = executor
        self.try_cnt = 0
        self.id = uuid.uuid4()
        self.consumer_id = ""
        pass

    def increase_try_cnt(self):
        self.try_cnt += 1
        return self.try_cnt

    def get_try_cnt(self):
        return self.try_cnt


class SpiderDetailTask(SpiderTask):
    args = []

    """
     构造Task的*args, **kwargs 参数就是调用spider_one_detail需要的（除了proxy）
    """

    def __init__(self, loop, executor, *args):
        super().__init__(loop, executor)
        self.args = args

    async def start(self, proxy):
        self.increase_try_cnt()
        # self.kwargs.update({"proxy": proxy})
        (book_url, spider_name, proxy) = (*self.args, proxy)
        spider_total_cnt = 1
        spider_success_cnt = 0
        new_tasks = []
        print("consumer {} - task {}:".format(self.consumer_id, self.id),
              "SpiderDetailTask -- Start:", " arg is",
              (book_url, spider_name, proxy))

        # 数据爬取
        book_info = None
        try:
            book_info = await SpiderTask.loop.run_in_executor(SpiderTask.executor,
                                                              SpiderDetailTask.spider_one_detail,
                                                              *(book_url, spider_name, proxy,
                                                                self.consumer_id, self.id))
            spider_success_cnt += 1
        except Exception as e:
            traceback.print_exc()
            eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                   "SpiderDetailTask -- Error:", e)

        if book_info is None:
            # 代理问题切换爬虫重试
            eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                   "SpiderDetailTask -- Error: Detail捕获到爬取任务异常，换proxy重试，重新放入queue")
            spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt  # spider的成功率是0
            new_tasks.append(self)
            return spider_success_ratio, new_tasks

        # 数据入库，获取书id
        book_id = None
        try:
            book_id = await self.abstractInDB(book_url, book_info)
        except Exception as e:
            eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                   "SpiderDetailTask -- Error:", e)

        if book_id is None:
            eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                   "SpiderDetailTask -- Error: abstractInDB捕获到数据插入问题，直接重新放入queue")
            # 代理问题切换爬虫重试
            spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt  # spider的成功率是1
            new_tasks.append(self)
            return spider_success_ratio, new_tasks

        # diff章节查询，需要爬的内容
        chapter_list = None
        try:
            chapter_list = await self.getSpiderList(book_id)
        except Exception as e:
            eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                   "SpiderDetailTask -- Error:", e)

        # 非代理问题重试
        if chapter_list is None:
            eprint(
                "consumer {} - task {}: getSpiderList捕获到数据查询问题，直接重新放入queue".format(self.consumer_id,
                                                                                   self.id),
                "SpiderDetailTask -- Error:")
            spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt  # spider的成功率是1
            new_tasks.append(self)
            return spider_success_ratio, new_tasks
        if len(chapter_list) == 0:
            book_title = book_info['title']
            book_chapter_len = len(book_info['chapter_list'])
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "SpiderDetailTask --所有没有新章节要爬取 book_id:{} title:{},包含章节:{}章".format(book_id,
                                                                                      book_title,
                                                                                      book_chapter_len))
        else:
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "SpiderDetailTask --有新章节要爬取 book_id:{} 需要爬{}章".format(book_id, len(chapter_list)))

        # 正常逻辑，产生所有问题
        spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt
        new_tasks.extend(self.generate_chunk_content_task_list(chapter_list, spider_name, book_url))
        return spider_success_ratio, new_tasks

        # self.add_content_task_to_queue(chapter_list)

    async def getSpiderList(self, book_id):
        ## only test
        # return [{'bookId': 1304, 'chapterId': 362,
        #          'url': 'http://www.aoyuge.com/34/34380/17347782.html', 'type': 2,
        #          'title': '第三百四十六章 黔驴技穷'}, {'bookId': 1304, 'chapterId': 363,
        #                                     'url': 'http://www.aoyuge.com/34/34380/17352438.html',
        #                                     'type': 2, 'title': '第三百四十七章 82-2手雷。'},
        #         {'bookId': 1304, 'chapterId': 364,
        #          'url': 'http://www.aoyuge.com/34/34380/17352992.html', 'type': 2,
        #          'title': '第三四十八章 我的钱竟然被人抢了'}, {'bookId': 1304, 'chapterId': 365,
        #                                         'url': 'http://www.aoyuge.com/34/34380/17354156.html',
        #                                         'type': 2, 'title': '第三百四十九章 陛下，你的钱被人抢了！'},
        #         {'bookId': 1304, 'chapterId': 366,
        #          'url': 'http://www.aoyuge.com/34/34380/17379329.html', 'type': 2,
        #          'title': '第三百五十章 投毒'}, {'bookId': 1304, 'chapterId': 367,
        #                                  'url': 'http://www.aoyuge.com/34/34380/17379923.html',
        #                                  'type': 2, 'title': '第三百五十一章 快递炸弹计划'},
        #         {'bookId': 1304, 'chapterId': 368,
        #          'url': 'http://www.aoyuge.com/34/34380/17384133.html', 'type': 2,
        #          'title': '第三百五十二章 你们已经被包围了'}, {'bookId': 1304, 'chapterId': 369,
        #                                         'url': 'http://www.aoyuge.com/34/34380/17384890.html',
        #                                         'type': 2, 'title': '第三百五十三章 因为刚好遇见你'},
        #         {'bookId': 1304, 'chapterId': 370,
        #          'url': 'http://www.aoyuge.com/34/34380/17388064.html', 'type': 2,
        #          'title': '第三百五十四章 破营'}, {'bookId': 1304, 'chapterId': 371,
        #                                   'url': 'http://www.aoyuge.com/34/34380/17389211.html',
        #                                   'type': 2, 'title': '第三百五十五章 推土机'}]
        sql = "select chapterList from t_bookabstract where online=1 and bookId=%s"
        db = pymysql.connect("localhost", "root",
                             "MyBook@123456", "bookzip", charset='utf8')
        cursor = db.cursor()
        cursor.execute(sql, book_id)
        db.commit()
        res = cursor.fetchone()
        if res is None:
            return []
        (chapterList,) = res
        chapters = json.loads(zlib.decompress(
            chapterList, wbits=zlib.MAX_WBITS | 32))
        chapterIdList = [item['chapterId'] for item in chapters]

        # 查询content表&&求差集
        table = book_id % 128
        queryInSql = "select chapterId from t_bookcontent_%s where bookId=%d and chapterId in (%s)" % (
            table, book_id, ",".join(map(str, chapterIdList)))
        cursor.execute(queryInSql)
        db.commit()
        res = cursor.fetchall()
        if res is None:
            return chapters
        needSpiderID = set(chapterIdList) - set([e[0] for e in res])
        return [item for item in chapters if item['chapterId'] in needSpiderID]

    @staticmethod
    def spider_one_detail(book_url, spider_name, proxy, consumer_id, task_id):

        infos = None
        try_num = 1
        while try_num <= 3 and infos is None:
            try:
                print("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                  threading.get_ident()),
                      "spider_one_detail -- RUN:",
                      time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                      "try_num(相同代理):", try_num,
                      "开始爬：", book_url, spider_name, proxy)
                # infos = spider_list[spider_name].spider_book_detail(book_url, proxy)
                infos = SpiderDetailTask.spider_one_detail_cmd(book_url, spider_name, proxy)
                print("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                  threading.get_ident()),
                      "spider_one_detail -- Result:",
                      spider_name, "查询：", book_url, " 成功", infos)
            except Exception as e:
                try_num = try_num + 1
                eprint("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                   threading.get_ident()),
                       "spider_one_detail -- Error:", e)
                traceback.print_exc()
        if infos is None:
            raise Exception("spider_one_detail " + str(spider_name) + "#" + str(
                book_url) + " failed using proxy:" + proxy)
        return infos

    @staticmethod
    def spider_one_detail_cmd(book_url, spider_name, proxy):
        arg_dict = {
            "book_url": book_url,
            "proxy": proxy
        }
        arg_str = json.dumps(arg_dict, ensure_ascii=False)
        output_content = subprocess.run(
            ["sh", os.path.join(SCRIPT_DIR_PATH, spider_name + ".sh"), "detail", arg_str],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # if output_content.stderr is not None:
        #     eprint(str(output_content.stderr.decode("utf-8")))
        if output_content.returncode != 0:
            raise Exception(str(output_content.stderr.decode("utf-8")))
        x = output_content.stdout
        return json.loads(x)

    def prepareHandle(self, bookName):
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
        eprint("prepareHandle", result)
        if (result == None):
            r = redis.Redis(host='127.0.0.1', port=6379, password='MyBook@123456')
            isExist = r.exists("appnovel_bookId_autogen_20190215")
            if (isExist == 0):
                r.set('appnovel_bookId_autogen_20190215', 19999)
            id = r.incr('appnovel_bookId_autogen_20190215')
            return (id, icon, bookType, typeId)
        else:
            return result

    def chapterListHandle(self, chapterList, bookId):
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

    async def abstractInDB(self, book_url, book_info):
        # # Todo: 入库 detail，获得id
        # #               book_id = abstractInDB(book_url, x)
        # book_id = 1304
        # return book_id
        data = book_info
        bookName = data["title"]
        author = data["author"]
        words = data["words"]
        status = data["update_status"]
        updateTime = data["update_time"]
        desc = data["book_intro"]
        bookUrl = book_url
        (bookId, bookIcon, bookType, typeId) = self.prepareHandle(bookName)
        clist = data["chapter_list"]
        jlist = self.chapterListHandle(clist, bookId)
        ct = bytes(json.dumps(jlist["list"]), encoding="utf8")
        chapterList = zlib.compress(ct)
        sql = "replace into t_bookabstract   (bookName,bookId,bookAuthor,bookDesc,bookWords,bookType,typeId,updateTime,origintype,sourceurl,online,chapterList)\
         VALUES('%s',%d,'%s','%s',%d,'%s',%d,'%s',%d,'%s',%d" % (
            bookName, bookId, author, desc, int(words), bookType, typeId, updateTime, origintype,
            bookUrl, 1)
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
        print("consumer {} - task {}:".format(self.consumer_id, self.id), "abstractInDB:",
              "入库图书目录：",
              bookId, " 成功")
        return bookId

    def generate_chunk_content_task_list(self, chapter_list, spider_name, book_url):
        chunk_size = 3
        chunks = [chapter_list[x:x + chunk_size]
                  for x in range(0, len(chapter_list), chunk_size)]
        chunk_tasks = []
        for chunk in chunks:
            arg_chunks = []
            for chapter in chunk:
                book_id = chapter['bookId']
                chapter_id = chapter['chapterId']
                content_title = chapter['title']
                content_url = chapter['url']
                arg_chunks.append((book_id, spider_name, chapter_id, content_url, book_url))
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "generate_chunk_content_task_list:chunks--", arg_chunks)
            chunk_tasks.append(SpiderContentTask(self.loop, self.executor, arg_chunks))
        return chunk_tasks


class SpiderContentTask(SpiderTask):
    args = []
    kwargs = {}

    def __init__(self, loop, executor, *args, **kwargs):
        super().__init__(loop, executor)
        self.args = args
        self.kwargs = kwargs

    async def start(self, proxy):
        self.increase_try_cnt()
        (arg_chunks, proxy) = (*self.args, proxy)
        spider_total_cnt = len(arg_chunks)
        spider_success_cnt = 0
        failed_arg_chunks = []

        print("consumer {} - task {}:".format(self.consumer_id, self.id),
              "SpiderContentTask -- Start:", (arg_chunks, proxy))

        for i, task_arg in enumerate(arg_chunks):
            (book_id, spider_name, chapter_id, content_url, book_url, proxy) = (*task_arg, proxy)
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "SpiderContentTask -- Start {}/{} -- arg:".format(i, spider_total_cnt),
                  (book_id, spider_name, chapter_id, content_url, book_url, proxy))
            future = SpiderTask.loop.run_in_executor(SpiderTask.executor, self.spider_one_content,
                                                     *(book_id, spider_name, chapter_id,
                                                       content_url,
                                                       book_url, proxy,
                                                       self.consumer_id, self.id))
            t = None
            c = None
            try:
                (t, c) = await future
                spider_success_cnt += 1
            except Exception as e:
                eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                       "SpiderContentTask -- Error:", e)
            if t is None or c is None:
                # 代理问题切换爬虫重试
                eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                       "SpiderContentTask -- Error:", "爬虫执行错误，换proxy重试，重新放入queue")
                failed_arg_chunks.append(task_arg)

            # 提交内容
            post_content_result = False
            try:
                post_content_result = await self.postContentIntoDb(book_id, chapter_id, t, c)
            except Exception as e:
                eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                       "SpiderContentTask -- Error:", e)
            if not post_content_result:
                # 不是代理问题，直接重试
                eprint("consumer {} - task {}:".format(self.consumer_id, self.id),
                       "SpiderContentTask -- Error:", "postContentIntoDb数据库入库错误，直接重新放入queue")
                failed_arg_chunks.append(task_arg)

            # todo 随机睡眠，在生成task时分配这个值？还是在这直接诶搞
            sleep_time = random.randint(2, 6)
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "睡眠了{}s".format(sleep_time))
            await asyncio.sleep(sleep_time)

        new_tasks = []
        spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt
        # 注意：正常情况没必要
        if len(failed_arg_chunks) > 0:
            self.args = (failed_arg_chunks,)
            new_tasks.append(self)
        return spider_success_ratio, new_tasks

    @staticmethod
    def spider_one_content(book_id, spider_name, chapter_id, content_url, book_url, proxy,
                           consumer_id, task_id):

        t = None
        c = None
        try_num = 1
        while try_num <= 2 and (t is None or c is None):
            try:
                print("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                  threading.get_ident()),
                      "spider_one_content -- RUN:",
                      time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                      "try_num(相同代理):", try_num,
                      "开始爬：", book_id, chapter_id, content_url, proxy)
                # t, c = spider_list[spider_name].spider_content(content_url, book_url, proxy)
                t, c = SpiderContentTask.spider_one_content_cmd(book_url, spider_name, content_url,
                                                                proxy)
                # 入库逻辑
                if t is not None and c is not None:
                    print("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                      threading.get_ident()),
                          "start_spider_content -- Result:", "爬到标题：", t)
                    print("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                      threading.get_ident()),
                          "start_spider_content -- Result:", "爬到内容长度:", len(c), "内容预览：", c[:25],
                          "......")
            except Exception as e:
                eprint("consumer {} - task {} - thread {}:".format(consumer_id, task_id,
                                                                   threading.get_ident()),
                       "start_spider_content -- Error:", e)
                try_num = try_num + 1
                traceback.print_exc()

        if t is None or c is None:
            raise Exception("spider content " + str(book_id) + "#" + str(
                chapter_id) + content_url + " failed using proxy:" + str(proxy))
        return t, c

    @staticmethod
    def spider_one_content_cmd(book_url, spider_name, content_url, proxy):
        arg_dict = {
            "content_url": [content_url],
            "interval": 0,
            "book_url": book_url,
            "proxy": proxy
        }
        arg_str = json.dumps(arg_dict, ensure_ascii=False)
        output_content = subprocess.run(
            ["sh", os.path.join(SCRIPT_DIR_PATH, spider_name + ".sh"), "content", arg_str],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # if output_content.stderr is not None:
        #     eprint("spider_one_content_cmd",str(output_content.stderr.decode("utf-8")))
        if output_content.returncode != 0:
            raise Exception(str(output_content.stderr.decode("utf-8")))
        x_content = output_content.stdout
        res = json.loads(x_content)
        if len(res['contents']) == 1:
            item = res['contents'][0]
            if 'content' in item and 'title' in item:
                return item['title'], item['content']
            else:
                raise Exception('标题或者内容返回了空')
        else:
            raise Exception('这里应该返回一个值')

    async def postContentIntoDb(self, book_id, chapter_id, title, content):
        payload = "bookId=%d&chapterId=%d&title=%s&content=%s" % (
            book_id, chapter_id, title, content)
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
        }
        response_f = __GLOBAL_FUTURE_REQUEST_SESSION__.request("POST", POST_CONTENT_URL,
                                                               data=payload.encode(
                                                                   "utf-8"),
                                                               headers=headers,
                                                               cert=(
                                                                   CLIENT_CRT_PATH,
                                                                   CLIENT_KEY_PATH),
                                                               verify=CACERT_PATH)
        response = await asyncio.wrap_future(response_f)
        if response.status_code == 200:
            print("consumer {} - task {}:".format(self.consumer_id, self.id),
                  "SpiderContentTask -- postContentIntoDb:", "提交数据库成功-", "book_id:",
                  book_id,
                  "chapter_id:", chapter_id, "title:", title, "\nresponse:", response.text)
            return True
        return False


async def consumer(consumer_id, task_q, proxy):
    print('consumer {}: waiting for task,using proxy - {}'.format(consumer_id, proxy))
    total_task = 0
    proxy_success_total_ration = 0
    total_task_unique = 0  # 不包括重试任务
    error_task_unique = 0  # 不包括重试任务
    while True:
        print('consumer {}: waiting for task'.format(consumer_id))
        task = None
        try:
            task = await task_q.get()
        except asyncio.CancelledError as e:
            # 退出点3
            raise MyException("consumer {}:正常退出", e, error_task_unique, total_task_unique)

        print('consumer {}: has task {}'.format(consumer_id, task.id))
        # 在这个程序中 None 是个特殊的值，表示终止信号(这里没有用)
        if task is None:
            task_q.task_done()
            break
        else:
            # 控制长度
            print('consumer {}: qsize={}..... '.format(consumer_id, task_q.qsize()))
            if isinstance(task,
                          SpiderDetailTask) and task_q.qsize() > __MAX_DETAIL_WAIT_QUEUE_SIZE__:
                print('consumer {}: reshuffle SpiderDetailTask{} to tail of queue..... '.format(
                    consumer_id, task.id))
                task_q.task_done()
                task_q.put_nowait(task)
                continue
            task.consumer_id = consumer_id
            total_task += 1
            # todo 这个异常不用截获？？？
            try:
                if task.try_cnt == 0:
                    total_task_unique += 1
                proxy_success_ration, new_need_tasks = await task.start(proxy)
                proxy_success_total_ration += proxy_success_ration
                print("consumer {}: result -- 本次任务proxy请求成功率{} 返回新任务数量{}".format(consumer_id,
                                                                                 proxy_success_ration,
                                                                                 len(
                                                                                     new_need_tasks)))
                new_put_cnt = 0
                for new_task in new_need_tasks:
                    if new_task.try_cnt == 0:
                        print("consumer {}: result -- 添加新任务 {} try_cnt {}".format(consumer_id,
                                                                                  new_task.id,
                                                                                  new_task.try_cnt))
                    else:
                        # 重试任务复用了相同的id！！！
                        print("consumer {}: result -- 试图添加重试任务 {} try_cnt {}".format(consumer_id,
                                                                                     new_task.id,
                                                                                     new_task.try_cnt))
                    if new_task.try_cnt <= 3:
                        new_put_cnt += 1
                        new_task.consumer_id = ""
                        await task_q.put(new_task)  # 相当于递归生成新的任务
                    else:
                        error_task_unique += 1
                        eprint("consumer {}: ERROR result --".format(consumer_id),
                               "task:{} 该任务**完全失败**，停止执行，具体信息：{}".format(new_task.id, new_task))

                print("consumer {}: result -- 新加入队列任务/期望总任务 {}/{}".format(consumer_id, new_put_cnt,
                                                                          len(new_need_tasks)))

            except Exception as e:
                eprint("consumer {}:!!!!!!!未预测到的Exception:".format(consumer_id), e)
                # 退出点1
                task_q.task_done()
                raise MyException("consumer {}:!!!!!!!未预测到的Exception:", e, error_task_unique,
                                  total_task_unique)

            print("consumer {}: result -- 代理平均成功率{}".format(consumer_id,
                                                            proxy_success_total_ration / total_task))

            # todo 任务先休息1s
            task_q.task_done()
            await asyncio.sleep(1)
            # if total_task == 2:
            #     print("!!!!测一下")
            #     raise ProxyAvailableException("proxy error")
            if total_task >= 3 and proxy_success_total_ration / total_task < 0.5:
                eprint("consumer {}: result -- proxy问题，计划退出".format(consumer_id))
                # 退出点2
                raise ProxyAvailableException("proxy error", error_task_unique, total_task_unique)

    print('consumer {}: ending'.format(consumer_id))


async def producer(q, num_workers):
    print('producer: starting')
    # 向队列中添加一些数据
    for i in range(num_workers * 3):
        await q.put(i)
        print('producer: added task {} to the queue'.format(i))

    # 通过 None 这个特殊值来通知消费者退出
    print('producer: adding stop signals to the queue')
    for i in range(num_workers):
        await q.put(None)
    print('producer: waiting for queue to empty')
    await q.join()
    print('producer: ending')


async def main(loop, query_list):
    proxy_list = await proxy_list_async.get_proxy_pool(3)
    # print(proxy_list)
    num_proxy = len(proxy_list)
    # 创建指定大小的队列，这样的话生产者将会阻塞
    # 直到有消费者获取数据
    task_q = asyncio.Queue()

    executor = __GLOBAL_EXECUTOR__
    # 统计信息
    total_task_unique = 0  # 不包括重试任务
    error_task_unique = 0  # 不包括重试任务

    for query_item in query_list:
        book_url = query_item['book_url']
        spider_name = query_item['spider_name']
        task_q.put_nowait(SpiderDetailTask(event_loop, executor, book_url, spider_name))

    # 用create_task直接启动了消费者
    consumers_tasks = dict()
    for i in range(num_proxy):
        task = loop.create_task(consumer(i, task_q, proxy_list[i]))
        consumers_tasks[task] = i

    # 等待所有 coroutines 都完成
    while not task_q.empty() or task_q._unfinished_tasks != 0:
        done, pending = await asyncio.wait(consumers_tasks.keys(), return_when=FIRST_COMPLETED,
                                           timeout=1)
        print("main loop: waiting status : done-{} pending-{} ".format(len(done), len(pending)))
        # 不论什么原因返回，全部启动
        if len(done) != 0:
            print("main loop: {}个custom退出, check need to restart consumer using new proxy ".format(
                len(done)))
            print("!!!!来拉")
            for done_task in done:
                consumer_id = consumers_tasks.get(done_task, -1)
                try:
                    res = done_task.result()
                    print("main loop: customer {}正常退出，未重启，信息：".format(consumer_id), res)
                except ProxyAvailableException as e:
                    (error_cnt, total_cnt) = e.args
                    error_task_unique += error_cnt
                    total_task_unique += total_cnt
                    new_proxy = await proxy_list_async.get_proxy_avaliable()
                    new_consumer = consumer(consumer_id, task_q, new_proxy)
                    new_consumer_task = loop.create_task(new_consumer)
                    print("main loop: 切换代理 restart consumer {} using new proxy {} ".format(
                        consumer_id,
                        new_proxy))
                    consumers_tasks.pop(done_task, None)
                    consumers_tasks[new_consumer_task] = consumer_id
                except MyException as e:
                    (error_cnt, total_cnt) = e.args
                    error_task_unique += error_cnt
                    total_task_unique += total_cnt
                    traceback.print_exc()
                    consumer_id = consumers_tasks.get(done_task, -1)
                    eprint("main loop: !!!!!!!未预测到的Exception退出customer{}:".format(consumer_id), e)

    print("main loop: queue is empty, exiting waiting queue")

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await task_q.join()
    total_slept_for = time.monotonic() - started_at
    print("main loop: total ruing time {}".format(total_slept_for))
    for task in consumers_tasks:
        task.cancel()
    results = await asyncio.gather(*consumers_tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, MyException):
            (error_cnt, total_cnt) = res.args
            error_task_unique += error_cnt
            total_task_unique += total_cnt

    print("main loop: final result {} ".format(results))
    success_ratio = 1
    if total_task_unique > 0:
        success_ratio = (total_task_unique - error_task_unique) * 1.0 / total_task_unique
    print("main loop: final result total task:{} error task:{} success_ratio:{}% ".format(
        total_task_unique, error_task_unique, success_ratio * 100))


event_loop = asyncio.get_event_loop()
try:
    event_loop.run_until_complete(main(event_loop, query_list))
finally:
    event_loop.close()
