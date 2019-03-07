import os
import sys
import asyncio
import threading
import logging
import logging_config
import spider_common_info

import proxy_list_async
from status_monitor import StatusMonitor
import uuid

import time
from datetime import datetime

# 设置时区
os.environ['TZ'] = 'Asia/Shanghai'
time.tzset()
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor

import subprocess
import json
import random
import hashlib
# import requests
from requests_futures.sessions import FuturesSession

# for db

from pathlib import Path

__ORIGIN_TYPE_SPIDER__ = 2  # 凡是用网页爬虫的来源类型都定义为2

__GLOBAL_EXECUTOR__ = ThreadPoolExecutor(max_workers=50)
__GLOBAL_FUTURE_REQUEST_SESSION__ = FuturesSession(executor=__GLOBAL_EXECUTOR__)

## 如果队列里面爬的任务太对，把爬章节的内容放队列后面，控制长度
__MAX_DETAIL_WAIT_QUEUE_SIZE__ = 100000

# 证书位置
__BASE_PATH__ = os.path.dirname(os.path.realpath(__file__))
__CACERT_PATH__ = __BASE_PATH__ + "/cert/cacert.pem"
__CLIENT_CRT_PATH__ = __BASE_PATH__ + "/cert/client.crt"
__CLIENT_KEY_PATH__ = __BASE_PATH__ + "/cert/client.key"

# 本地图书目录检测&&新建
__BOOK_STORE_PATH__ = os.path.join(__BASE_PATH__, "book_store")
os.path.exists(__BOOK_STORE_PATH__) or os.makedirs(__BOOK_STORE_PATH__)

# 导入爬虫脚本的目录
__SCRIPT_DIR_PATH__ = __BASE_PATH__ + "/scripts"
sys.path.insert(0, __SCRIPT_DIR_PATH__)

# 配置日志输出
logging_config.configure_root_logger(
    os.path.join(__BASE_PATH__, "logs", os.path.splitext(os.path.basename(__file__))[0], "app.log"))
logging.getLogger().setLevel(logging.CRITICAL)
logger = logging.getLogger("online")
# logger.disabled = True

__spider_list__ = {
    "spider-origin": __import__("spider-origin")
}

query_list_path = os.path.join(__BOOK_STORE_PATH__, "booklist.json")

__query_list__ = json.load(open(query_list_path))[:2]
__query_list__ = [item for sublist in __query_list__ for item in sublist][:1]


# [
#     {"book_url": "http://www.aoyuge.com/16/16977/index.html",
#      "spider_name": "spider-origin", "name": "超级越界强者"},
#     # {"book_url": "http://www.aoyuge.com/34/34380/index.html",
#     #  "spider_name": "spider-origin", "name": "女帝家的小白脸"},
#     # {"book_url": "http://www.aoyuge.com/15/15779/index.html",
#     #  "spider_name": "spider-origin", "name": "万古神帝"}
# ]


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
        session = FuturesSession(executor=__GLOBAL_EXECUTOR__)
        spider_total_cnt = 1
        spider_success_cnt = 0
        new_tasks = []
        logging.debug(
            "consumer {} - task {}: SpiderDetailTask -- Start: args={} ".format(self.consumer_id,
                                                                                self.id,
                                                                                (book_url,
                                                                                 spider_name,
                                                                                 proxy)))

        # 数据爬取
        book_info = None
        try:
            # book_info = await SpiderTask.loop.run_in_executor(SpiderTask.executor,
            #                                                   SpiderDetailTask.spider_one_detail,
            #                                                   *(book_url, spider_name, proxy,
            #                                                     self.consumer_id, self.id))
            book_info = await self.spider_detail(session, book_url, spider_name, proxy)
            spider_success_cnt += 1
        except Exception as e:
            logging.exception(
                "consumer {} - task {}: SpiderDetailTask -- 爬图书详情错误！！！！".format(self.consumer_id,
                                                                                self.id))

        if book_info is None:
            # 代理问题切换爬虫重试
            logging.warning(
                "consumer {} - task {}: Detail爬虫任务异常，换proxy重试，重新放入queue".format(self.consumer_id,
                                                                                self.id))
            spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt  # spider的成功率是0
            new_tasks.append(self)
            return spider_success_ratio, new_tasks

        try:
            self.add_to_bookstore_catalog_file(book_info)
            self.add_to_book_manifest_detail_file(book_info)
        except Exception as e:

            logging.exception(
                "consumer {} - task {}: SpiderDetailTask -- 添加书籍相关catalog/manifest文件错误，直接退出！！！".format(
                    self.consumer_id,
                    self.id))
            raise e

        # diff章节查询本地，需要爬的内容
        chapter_list = None
        book_dir_path = None
        try:
            book_dir_path = self.get_book_dir_path(book_info)
            chapter_list = self.get_need_spider_chapter(book_dir_path,
                                                        book_info["chapter_list"])
        except Exception as e:
            logging.exception(
                "consumer {} - task {}: SpiderDetailTask -- get_need_spider_chapter捕获到数据查询问题，直接退出！！！".format(
                    self.consumer_id,
                    self.id))
            raise e

        # # 非代理问题重试
        # if chapter_list is None or book_dir_path is None:
        #     logging.warning(
        #         "consumer {} - task {}: get_need_spider_chapter捕获到数据查询问题，直接重新放入queue".format(
        #             self.consumer_id,
        #             self.id))
        #     spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt  # spider的成功率是1
        #     new_tasks.append(self)
        #     return spider_success_ratio, new_tasks
        if len(chapter_list) == 0:
            # 是否完结，下载完了大done标记
            if await SpiderDetailTask.check_status_done(session, spider_name, book_info, proxy):
                self.add_done_flag_to_local_book(book_dir_path)
            book_title = book_info['title']
            book_chapter_len = len(book_info['chapter_list'])
            logging.info("consumer {} - task {}:".format(self.consumer_id, self.id)
                         + " SpiderDetailTask --所有没有新章节要爬取 book_dir_path:{} title:{},包含章节:{}章"
                         .format(book_dir_path, book_title, book_chapter_len))
            logger.info("consumer {} - task {}:".format(self.consumer_id, self.id)
                        + " SpiderDetailTask --所有没有新章节要爬取 book_dir_path:{} title:{},包含章节:{}章"
                        .format(book_dir_path, book_title, book_chapter_len))
        else:
            all_book_chapter_len = len(book_info['chapter_list'])
            logging.info("consumer {} - task {}:".format(self.consumer_id, self.id)
                         + " SpiderDetailTask --有新章节要爬取 book_dir_path:{} 需要爬{}章"
                         .format(book_dir_path, len(chapter_list)))
            logger.info("consumer {} - task {}:".format(self.consumer_id, self.id)
                        + " SpiderDetailTask --有新章节要爬取 book_dir_path:{} 需要爬{}章"
                        .format(book_dir_path, len(chapter_list)))
            StatusMonitor.set_monitor(book_dir_path, all_book_chapter_len, "章",
                                      all_book_chapter_len - len(chapter_list))

        # 正常逻辑，产生所有问题
        spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt
        new_tasks.extend(self.generate_chunk_content_task_list(chapter_list, spider_name, book_url))
        return spider_success_ratio, new_tasks

    @staticmethod
    def request_url_async(session, url, proxy):
        book_headers = {**(spider_common_info.__common_headers__),
                        'If-None-Match': str(int(time.time()))}
        f = session.get(url, headers=book_headers, proxies=proxy, timeout=30)
        return asyncio.wrap_future(f)

    async def spider_detail(self, session, book_url, spider_name, proxy):
        infos = None
        try_num = 1
        last_exception = None
        while try_num <= 3 and infos is None:
            try:
                resp = await self.request_url_async(session, book_url, proxy)
            except Exception as e:
                # logging.error(
                #     "consumer {} - task {}:网络请求异常 url:{}".format(self.consumer_id, self.id,
                #                                                  book_url))
                try_num = try_num + 1
                last_exception = e
                continue

            if not resp.ok:
                error_info = "consumer {} - task {}:网络请求异常 url:{} status_code:{} content:{}".format(
                    self.consumer_id, self.id, book_url, resp.status_code,
                    resp.content.decode("utf8"))
                # logging.error(error_info)
                try_num = try_num + 1
                last_exception = Exception(error_info)
                continue

            try:
                infos = __spider_list__[spider_name].spider_parse_detail(book_url, resp.content)
            except Exception as e:
                # logging.error(
                #     "consumer {} - task {}:解析网页异常 content:{}".format(self.consumer_id, self.id,
                #                                                      resp.content.decode("utf8")))
                try_num = try_num + 1
                last_exception = e
                continue
        if infos is None:
            raise last_exception
        return infos

    @staticmethod
    def spider_one_detail(book_url, spider_name, proxy, consumer_id, task_id):

        infos = None
        try_num = 1
        while try_num <= 3 and infos is None:
            try:
                logging.debug(
                    "consumer {} - task {} - thread {}: spider_one_detail -- "
                    "RUN: try_num(相同代理):{} 开始爬：book_url:{},spider_name:{},proxy:{}"
                        .format(consumer_id, task_id,
                                threading.get_ident(), try_num, book_url, spider_name, proxy))
                infos = __spider_list__[spider_name].spider_book_detail(book_url, proxy)
                # infos = SpiderDetailTask.spider_one_detail_cmd(book_url, spider_name, proxy)
                logging.debug(
                    "consumer {} - task {} - thread {}: spider_one_detail --"
                    " Result: {} 查询：{} 成功 {}"
                        .format(consumer_id, task_id,
                                threading.get_ident(), spider_name, book_url, infos))
            except Exception as e:
                try_num = try_num + 1
                logging.exception(
                    "consumer {} - task {} - thread {} spider_one_detail --"
                    " Error:{}".format(consumer_id, task_id, threading.get_ident(), e))
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
            ["sh", os.path.join(__SCRIPT_DIR_PATH__, spider_name + ".sh"), "detail", arg_str],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if output_content.returncode != 0:
            raise Exception(str(output_content.stderr.decode("utf-8")))
        x = output_content.stdout
        return json.loads(x)

    @staticmethod
    def get_book_dir_path(book_info):
        if not book_info["title"] or not book_info["title"].strip():
            raise Exception("必须有书名")

        title = book_info["title"].strip()
        book_type = book_info["book_type"]
        if book_type and book_type.strip():
            book_type = book_type.strip()
        else:
            book_type = "other_type"

        author = book_info["author"]
        if author and author.strip():
            author = author.strip()
        else:
            author = "default_author"
        words = book_info["words"]
        status = book_info["update_status"]
        updateTime = book_info["update_time"]
        desc = book_info["book_intro"]

        book_dir_path = os.path.join(__BOOK_STORE_PATH__, book_type, author, title)
        return book_dir_path

    @staticmethod
    def get_need_spider_chapter(book_dir_path, chapter_list):
        # fixme main thread 因为有__MANIFEST_DOWNLOADED__文件的操作
        book_dir = Path(book_dir_path)

        if not book_dir.exists():
            # 新书
            book_dir.mkdir(parents=True, exist_ok=True)
            return [{'book_dir_path': book_dir_path, 'chapter_id': i, 'chapter_title': chapter[0],
                     'chapter_url': chapter[1]} for i, chapter in enumerate(chapter_list)]
        book_done_file = Path(
            os.path.join(book_dir_path, "__DONE__"))
        if book_done_file.exists():
            # 非连载图书完成
            return []

        ## diff过滤，对过滤的结果重新生成__MANIFEST_DOWNLOADED__文件（保证一致性）
        downloaded_manifest_path = os.path.join(book_dir_path, "__MANIFEST_DOWNLOADED__")
        os.path.exists(downloaded_manifest_path) and os.remove(downloaded_manifest_path)

        chapter_index_name_dict = dict([(i, chapter) for i, chapter in enumerate(chapter_list)])
        result = []

        with os.scandir(book_dir_path) as it, open(downloaded_manifest_path,
                                                   'a', encoding='utf8') as downloaded_manifest:
            for entry in it:
                chapter_file_name = entry.name
                if chapter_file_name.startswith("__") and chapter_file_name.endswith("__"):
                    continue
                if chapter_file_name.endswith(".tmp"):
                    continue
                chapter_id = None
                chapter_title_hash_str = None
                # try:
                (chapter_id, chapter_title_hash_str) = SpiderDetailTask.parse_chapter_file_name(
                    chapter_file_name)
                # except Exception as e:
                #     logging.exception(e)
                if chapter_id is None or chapter_title_hash_str is None:
                    # os.remove(os.path.join(book_dir_path, chapter_file_name))
                    continue

                # 正常情况：如果本地的index和标题都对的上从set里面删除，不用再下载
                if chapter_id in chapter_index_name_dict and \
                        SpiderDetailTask.convert_chapter_title_to_safe_str(
                            chapter_index_name_dict[chapter_id][0]) == chapter_title_hash_str:
                    chapter_index_name_dict.pop(chapter_id)
                    downloaded_manifest.write(str(chapter_id) + "\n")
                    continue

                # 处理本地图书的特殊情况：index有，但是章节名称对不上，删了重新下载
                if chapter_id in chapter_index_name_dict and \
                        SpiderDetailTask.convert_chapter_title_to_safe_str(
                            chapter_index_name_dict[chapter_id][0]) != chapter_title_hash_str:
                    logging.warning("本地图书 {} 的特殊情况：图书目录顺序发生了变换，删了重新下载".format(entry.name))
                    os.remove(os.path.join(book_dir_path, chapter_file_name))
                    continue
                # 处理本地图书的特殊情况：本地有爬虫没有（本地内容更新）--先不处理，免得删错了
                if chapter_id not in chapter_index_name_dict:  # 本地的index不在最新的里面？？什么情况
                    logging.warning("本地图书 {} 的特殊情况：本地有爬虫没有（本地内容更新）--先不处理，免得删错了".format(entry.name))
                    continue

        for chapter_id, chapter in chapter_index_name_dict.items():
            result.append(
                {'book_dir_path': book_dir_path, 'chapter_id': chapter_id,
                 'chapter_title': chapter[0],
                 'chapter_url': chapter[1]})
        return result

    @staticmethod
    def add_done_flag_to_local_book(book_dir_path):
        try:
            Path(os.path.join(book_dir_path, "__DONE__")).touch()
        except Exception as e:
            logging.warning(
                "touch {} file failed:{}".format(os.path.join(book_dir_path, "__DONE__"), e))

    @staticmethod
    def convert_chapter_title_to_safe_str(chapter_title):
        return hashlib.md5(chapter_title.encode("utf-8")).hexdigest()[:8]

    @staticmethod
    def parse_chapter_file_name(chapter_file_name):
        chapter_file_name = os.path.splitext(chapter_file_name)[0]
        if not chapter_file_name.startswith("##"):
            raise Exception("命名错误:没有##开头，格式：##indext##tiltle_hash_str.txt")
        t = chapter_file_name.split("##")
        if len(t) < 3:
            raise Exception("命名错误:格式：##indext##tiltle")
        chapter_id = -1
        chapter_title_hash_str = "##".join(t[2:])
        try:
            chapter_id = int(t[1])
        except Exception as e:
            raise Exception("命名错误：索引错误" + str(e))
        return chapter_id, chapter_title_hash_str

    @staticmethod
    def generate_chapter_file_name(chapter_id, chapter_title):
        """
        生成存储的文件名
        :param chapter_info: {'book_dir_path': book_dir_path, 'chapter_id': chapter_id, 'chapter_title': chapter[0],
        #                            'chapter_url': chapter[1]}
        :return: 格式：##indext##tiltle"
        """
        return "##" + str(chapter_id) + "##" + SpiderDetailTask.convert_chapter_title_to_safe_str(
            chapter_title) + ".txt"

    def generate_chunk_content_task_list(self, chapter_list, spider_name, book_url):
        """
        chapter_info: {'book_dir_path': book_dir_path, 'chapter_id': chapter_id, 'chapter_title': chapter[0],
                                    'chapter_url': chapter[1]}
        :param chapter_list:
        :param spider_name:
        :param book_url:
        :return:
        """
        chunk_size = 3
        chunks = [chapter_list[x:x + chunk_size]
                  for x in range(0, len(chapter_list), chunk_size)]
        chunk_tasks = []
        for chunk in chunks:
            arg_chunks = []
            for chapter in chunk:
                book_dir_path = chapter['book_dir_path']
                content_url = chapter['chapter_url']
                chapter_id = chapter['chapter_id']
                chapter_title = chapter['chapter_title']
                chapter_file_name = self.generate_chapter_file_name(chapter_id, chapter_title)
                arg_chunks.append(
                    (book_dir_path, spider_name, chapter_id, chapter_title, content_url, book_url))
            logging.debug("consumer {} - task {}:generate_chunk_content_task_list:chunks--".format(
                self.consumer_id, self.id, arg_chunks))
            chunk_tasks.append(SpiderContentTask(self.loop, self.executor, arg_chunks))
        return chunk_tasks

    @staticmethod
    def add_to_bookstore_catalog_file(book_info):
        """
         放在根目录下面的目录，记录了所有的书
        :param book_info:
        :return:
        """
        # todo 加载内存判断？？
        catalog_path = os.path.join(__BOOK_STORE_PATH__, "__CATALOG__")
        book_dir_path = SpiderDetailTask.get_book_dir_path(book_info)
        rel_book_path = os.path.relpath(book_dir_path, __BOOK_STORE_PATH__)
        logging.debug(catalog_path)
        os.path.exists(catalog_path) or Path(catalog_path).touch()
        # Opens a file for both reading and writing.
        # The file pointer will be at the beginning of the file.
        with open(catalog_path, "r+", encoding='utf8') as file:
            for line in file:
                if line.startswith(rel_book_path):
                    break
            else:  # not found, we are at the eof
                file.write(rel_book_path + "\n")  # append missing data

    @staticmethod
    def add_to_book_manifest_detail_file(book_info):
        """
         写detail文件到manifest中，书的清单
        :param book_info:
        :return:
        """
        book_dir_path = SpiderDetailTask.get_book_dir_path(book_info)
        manifest_path = os.path.join(book_dir_path, "__MANIFEST_DETAIL__")
        Path(book_dir_path).mkdir(parents=True, exist_ok=True)
        # Opens a file for both writing and reading. Overwrites the existing file if the file exists.
        # If the file does not exist, creates a new file for reading and writing.
        with open(manifest_path, 'w+', encoding='utf8') as f:
            # Note that f has now been truncated to 0 bytes, so you'll only
            # be able to read data that you write after this point
            f.write(json.dumps(book_info, ensure_ascii=False))
            # f.seek(
            #     0)  # Important: return to the top of the file before reading, otherwise you'll just read an empty string
            # data = f.read()  # Returns 'somedata\n'

    @staticmethod
    async def check_status_done(session, spider_name, book_info, proxy):
        if book_info["update_status"] == 1:
            return True
        try:
            fmt = "%Y-%m-%d %H:%M:%S"
            update_time = book_info['update_time']
            now = datetime.now()
            tdelta = now - datetime.strptime(update_time, fmt)
            if tdelta.days > 180:
                return True

            book_url = book_info["book_url"]
            m_book_url = (("/".join(book_url.split("/")[:-1])) + "/").replace('www', 'm', 1)
            f = SpiderDetailTask.request_url_async(session, m_book_url, proxy)
            resp = await f
            if __spider_list__[spider_name].spider_parse_detail_m(m_book_url, resp.content)[
                "update_status"] == 1:
                return True
        except Exception as e:
            logging.exception("检测图书状态错误")
            pass
        return False


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
        session = FuturesSession(executor=__GLOBAL_EXECUTOR__)
        spider_total_cnt = len(arg_chunks)
        spider_success_cnt = 0
        failed_arg_chunks = []

        logging.debug("consumer {} - task {}: SpiderContentTask -- Start: chunk_args is {}"
                      .format(self.consumer_id, self.id, (arg_chunks, proxy)))
        for i, task_arg in enumerate(arg_chunks):
            (book_dir_path, spider_name, chapter_id, chapter_title, content_url, book_url, proxy) = \
                (*task_arg, proxy)
            logging.debug(
                "consumer {} - task {}: SpiderContentTask -- Start {}/{} -- "
                "arg: book_dir_path:{}, spider_name:{}, chapter_id:{}, chapter_title:{},"
                "content_url:{}, book_url:{}, proxy:{}".format(self.consumer_id, self.id,
                                                               i, spider_total_cnt,
                                                               book_dir_path,
                                                               spider_name,
                                                               chapter_id,
                                                               chapter_title,
                                                               content_url,
                                                               book_url,
                                                               proxy))
            # future = SpiderTask.loop.run_in_executor(SpiderTask.executor, self.spider_one_content,
            #                                          *(
            #                                              book_dir_path, spider_name,
            #                                              chapter_id,
            #                                              content_url,
            #                                              book_url, proxy,
            #                                              self.consumer_id, self.id))
            t = None
            c = None
            try:
                (t, c) = await self.spider_content(session, spider_name, content_url, book_url,
                                                   proxy)
                spider_success_cnt += 1
            except Exception as e:
                logging.exception(
                    "consumer {} - task {}: SpiderContentTask -- Error:".format(self.consumer_id,
                                                                                self.id, e))
            if t is None or c is None:
                # 代理问题切换爬虫重试
                logging.warning("consumer {} - task {}: Content爬虫执行错误，换proxy重试，重新放入queue".format(
                    self.consumer_id, self.id))
                failed_arg_chunks.append(task_arg)
            else:

                # 提交内容
                post_content_result = False
                try:
                    post_content_result = await self.post_to_local_file(book_dir_path, chapter_id,
                                                                        chapter_title,
                                                                        c) and await self.append_to_book_manifest_downloaded_file(
                        book_dir_path, chapter_id)
                    logging.info(
                        "consumer {} - task {}:爬book路径:{} chapter:{} 完成".format(
                            self.consumer_id,
                            self.id,
                            book_dir_path,
                            chapter_title))
                    StatusMonitor.update_monitor(book_dir_path)
                    # post_content_result = await self.postContentIntoDb(book_id, chapter_id, t, c)
                except Exception as e:
                    logging.exception(
                        "consumer {} - task {}: SpiderContentTask -- post_to_local_file错误 直接诶退出！！ Error:{}".format(
                            self.consumer_id,
                            self.id, e))
                    raise e
                # if not post_content_result:
                #     # 不是代理问题，直接重试
                #     logging.warning("consumer {} - task {}:"
                #                     "postContentIntoDb数据库入库错误，直接重新放入queue".format(
                #         self.consumer_id, self.id))
                #     failed_arg_chunks.append(task_arg)

            # todo 随机睡眠，在生成task时分配这个值？还是在这直接诶搞
            sleep_time = random.randint(2, 6)
            logging.info(
                "consumer {} - task {}:睡眠了{}s".format(self.consumer_id, self.id, sleep_time))
            await asyncio.sleep(sleep_time)

        new_tasks = []
        spider_success_ratio = spider_success_cnt * 1.0 / spider_total_cnt
        # 注意：正常情况没必要
        if len(failed_arg_chunks) > 0:
            self.args = (failed_arg_chunks,)
            new_tasks.append(self)
        return spider_success_ratio, new_tasks

    @staticmethod
    def request_url_async(session, url, proxy, headers={}):
        book_headers = {**spider_common_info.__common_headers__, **headers,
                        'If-None-Match': str(int(time.time()))}
        f = session.get(url, headers=book_headers, proxies=proxy, timeout=15)
        return asyncio.wrap_future(f)

    async def spider_content(self, session, spider_name, content_url, book_url, proxy):
        t = None
        c = None
        try_num = 1
        last_exception = None
        while try_num <= 3 and (t is None or c is None):
            try:
                resp = await self.request_url_async(session, content_url, proxy,
                                                    {"Referer": book_url,
                                                     "Cache-Control": "max-age=0"})
            except Exception as e:
                # logging.error(
                #     "consumer {} - task {} :网络请求异常 url:{}".format(self.consumer_id, self.id,
                #                                                   content_url))
                try_num = try_num + 1
                last_exception = e
                continue

            if not resp.ok:
                error_info = "consumer {} - task {}:网络请求异常 url:{} status_code:{} content:{}".format(
                    self.consumer_id, self.id, content_url, resp.status_code,
                    resp.content.decode("utf8"))
                # logging.error(error_info)
                try_num = try_num + 1
                last_exception = Exception(error_info)
                continue

            try:
                t, c = __spider_list__[spider_name].spider_parse_content(book_url, content_url,
                                                                         resp.content)
            except Exception as e:
                # logging.error(
                #     "consumer {} - task {}:解析网页异常 content:{}".format(self.consumer_id, self.id,
                #                                                      resp.content.decode("utf8")))
                try_num = try_num + 1
                last_exception = e
                continue

        if t is None or c is None:
            raise last_exception
        logging.debug(
            "consumer {} - task {} : start_spider_content -- "
            "Result: 爬到标题：{}".format(self.consumer_id, self.id, t))
        logging.debug(
            "consumer {} - task {} : start_spider_content -- "
            "Result: 爬到内容长度:{},内容预览：{} ......".format(
                self.consumer_id, self.id, len(c), c[:25]))
        return t, c

    @staticmethod
    def spider_one_content(book_dir_path, spider_name, chapter_id, content_url, book_url,
                           proxy,
                           consumer_id, task_id):

        t = None
        c = None
        try_num = 1
        while try_num <= 2 and (t is None or c is None):
            try:
                logging.debug(
                    "consumer {} - task {} - thread {}: spider_one_content -- RUN - try_num(相同代理):"
                    .format(consumer_id, task_id, threading.get_ident(), try_num)
                    +
                    "开始爬：book_dir_path:{}, chapter_id:{}, content_url{}, proxy{}"
                    .format(book_dir_path, chapter_id, content_url, proxy))
                t, c = __spider_list__[spider_name].spider_content(content_url, book_url, proxy)
                # t, c = SpiderContentTask.spider_one_content_cmd(book_url, spider_name, content_url,
                #                                                 proxy)
                # 入库逻辑
                if t is not None and c is not None:
                    logging.debug(
                        "consumer {} - task {} - thread {}: start_spider_content -- "
                        "Result: 爬到标题：{}".format(consumer_id, task_id, threading.get_ident(), t))
                    logging.debug(
                        "consumer {} - task {} - thread {}: start_spider_content -- "
                        "Result: 爬到内容长度:{},内容预览：{} ......".format(
                            consumer_id, task_id, threading.get_ident(), len(c), c[:25]))
            except Exception as e:
                try_num = try_num + 1
                logging.exception(
                    "consumer {} - task {} - thread {} start_spider_content -- Error:{}".format(
                        consumer_id, task_id,
                        threading.get_ident(), e))

        if t is None or c is None:
            raise Exception("spider content " + str(book_dir_path) + "#" + str(
                chapter_id) + " url:" + content_url + " failed using proxy:" + str(proxy))
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
            ["sh", os.path.join(__SCRIPT_DIR_PATH__, spider_name + ".sh"), "content", arg_str],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if output_content.returncode != 0:
            raise Exception(str(output_content.returncode) + " stderr:" + str(
                output_content.stderr.decode("utf-8")) + " stdout: " + output_content.stdout.decode(
                "utf-8"))
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

    @staticmethod
    def generate_chapter_file_name(chapter_id, chapter_title):
        """
        生成存储的文件名
        :param chapter_title:
        :param chapter_id:
        :param chapter_info: {'book_dir_path': book_dir_path, 'chapter_id': chapter_id, 'chapter_title': chapter[0],
        #                            'chapter_url': chapter[1]}
        :return: 格式：##indext##tiltle"
        """
        return "##" + str(chapter_id) + "##" + SpiderDetailTask.convert_chapter_title_to_safe_str(
            chapter_title) + ".txt"

    async def post_to_local_file(self, book_dir_path, chapter_id, chapter_title, content):
        def run():
            chapter_file_name = self.generate_chapter_file_name(chapter_id, chapter_title)
            # Opens a file for both writing and reading. Overwrites the existing file if the file exists.
            # If the file does not exist, creates a new file for reading and writing.
            file_path = os.path.join(book_dir_path, chapter_file_name)
            with open(file_path + ".tmp", 'w+', encoding='utf8') as f:
                # Note that f has now been truncated to 0 bytes, so you'll only
                # be able to read data that you write after this point
                f.write(content)
            os.rename(file_path + ".tmp", file_path)
            return True

        return await SpiderTask.loop.run_in_executor(SpiderTask.executor, run)

    async def append_to_book_manifest_downloaded_file(self, book_dir_path, chapter_id):
        """
         写已经下载完成的章节id（序号）文件到manifest中，书的清单
        :param book_info:
        :return:
        """
        # fixme main thread
        # manifest_path = os.path.join(book_dir_path, "__MANIFEST_DOWNLOADED__")
        # # Opens a file for appending. The file pointer is at the end of the file if the file exists.
        # # That is, the file is in the append mode. If the file does not exist, it creates a new file for writing.
        # with open(manifest_path, 'a', encoding='utf8') as downloaded_manifest:
        #     downloaded_manifest.write(str(chapter_id) + "\n")

        return True


async def consumer(consumer_id, task_q, proxy):
    logging.debug('consumer {}: waiting for task,using proxy - {}'.format(consumer_id, proxy))
    total_task = 0
    proxy_success_total_ration = 0
    total_task_unique = 0  # 不包括重试任务
    error_task_unique = 0  # 不包括重试任务
    while True:
        logging.debug('consumer {}: waiting for task'.format(consumer_id))
        task = None
        try:
            task = await task_q.get()
        except asyncio.CancelledError as e:
            # 退出点3
            raise MyException("consumer {}:正常退出", e, error_task_unique, total_task_unique)

        logging.debug('consumer {}: has task {}'.format(consumer_id, task.id))
        # 在这个程序中 None 是个特殊的值，表示终止信号(这里没有用)
        if task is None:
            task_q.task_done()
            break
        else:
            # 控制长度
            if isinstance(task,
                          SpiderDetailTask) and task_q.qsize() > __MAX_DETAIL_WAIT_QUEUE_SIZE__:
                logging.warning(
                    'consumer {}: 队列太长，暂停SpiderDetailTask{} reshuffle to tail of queue..... '.format(
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
                logging.debug(
                    "consumer {}: result -- 本次任务proxy请求成功率{} 返回新任务数量{}"
                        .format(consumer_id, proxy_success_ration, len(new_need_tasks)))
                new_put_cnt = 0
                for new_task in new_need_tasks:
                    if new_task.try_cnt == 0:
                        logging.debug(
                            "consumer {}: result -- 添加新任务 {} try_cnt {}"
                                .format(consumer_id, new_task.id, new_task.try_cnt))
                    else:
                        # 重试任务复用了相同的id！！！
                        logging.debug(
                            "consumer {}: result -- 试图添加重试任务 {} try_cnt {}"
                                .format(consumer_id, new_task.id, new_task.try_cnt))
                    if new_task.try_cnt <= 3:
                        new_put_cnt += 1
                        new_task.consumer_id = ""
                        await task_q.put(new_task)  # 相当于递归生成新的任务
                    else:
                        error_task_unique += 1
                        logging.critical(
                            "consumer {}: ERROR result -- task:{} 该任务**完全失败**，停止执行，具体信息：{}"
                                .format(consumer_id, new_task.id, new_task))

                logging.debug(
                    "consumer {}: result -- 新加入队列任务/期望加入任务 {}/{}"
                        .format(consumer_id, new_put_cnt, len(new_need_tasks)))

            except Exception as e:
                # 退出点1
                task_q.task_done()
                error_task_unique += 1
                raise MyException("consumer {}:!!!!!!!未预测到的Exception:", e, error_task_unique,
                                  total_task_unique)

            logging.debug("consumer {}: result -- {}代理平均成功率{}"
                          .format(consumer_id, proxy, proxy_success_total_ration / total_task))

            # todo 任务先休息1s 注意这里的顺序，或者捕获sleep的异常，因为可能出现cannel异常，我这里依赖异常返回值了
            await asyncio.sleep(1)
            task_q.task_done()

            if total_task >= 3 and proxy_success_total_ration / total_task < 0.5:
                # 退出点2
                raise ProxyAvailableException("proxy error", error_task_unique, total_task_unique)

    logging.debug('consumer {}: ending'.format(consumer_id))


async def producer(task_q, query_list, loop, executor):
    # print('producer: starting')
    # Add some numbers to the queue to simulate jobs
    start = 0
    end = 0
    chunk_size = 10
    while start < len(query_list):
        if task_q.qsize() < 10000:
            end = start + chunk_size
            if end > len(query_list):
                end = len(query_list)
            for i in range(start, end):
                query_item = query_list[i]
                book_url = query_item['book_url']
                spider_name = "spider-origin"  # query_item['spider_name']
                logging.debug("producer: put detail task book_url: {}".format(book_url))
                await task_q.put(SpiderDetailTask(loop, executor, book_url, spider_name))
                await asyncio.sleep(3)
            start = end
        await asyncio.sleep(10)


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
    StatusMonitor.set_monitor("book", len(query_list), "本")
    # for query_item in query_list:
    #     book_url = query_item['book_url']
    #     spider_name = "spider-origin"  # query_item['spider_name']
    #     task_q.put_nowait(SpiderDetailTask(loop, executor, book_url, spider_name))
    producer_task = loop.create_task(producer(task_q, query_list, loop, executor))

    # 用create_task直接启动了消费者
    consumers_tasks = dict()
    for i in range(num_proxy):
        task = loop.create_task(consumer(i, task_q, proxy_list[i]))
        consumers_tasks[task] = i

    while task_q.empty():
        await asyncio.sleep(0.1)

    # 等待所有 coroutines 都完成
    while not task_q.empty() or task_q._unfinished_tasks != 0:
        done, pending = await asyncio.wait(consumers_tasks.keys(), return_when=FIRST_COMPLETED,
                                           timeout=1)
        logging.debug(
            "main loop: waiting status : done-{} pending-{} ".format(len(done), len(pending)))
        # 不论什么原因返回，全部启动
        if len(done) != 0:
            logging.debug(
                "main loop: {}个custom退出, check need to restart consumer using new proxy ".format(
                    len(done)))
            logging.debug("!!!!来拉")
            for done_task in done:
                consumer_id = consumers_tasks.get(done_task, -1)
                try:
                    res = done_task.result()
                    logging.warning("main loop: customer {}正常退出，未重启，信息：{}"
                                    .format(consumer_id, res))
                except ProxyAvailableException as e:
                    logging.debug("!!!!state1:{}".format(e.args))
                    (error_cnt, total_cnt) = e.args
                    error_task_unique += error_cnt
                    total_task_unique += total_cnt
                    new_proxy = await proxy_list_async.get_proxy_avaliable()
                    new_consumer = consumer(consumer_id, task_q, new_proxy)
                    new_consumer_task = loop.create_task(new_consumer)
                    logging.warning(
                        "main loop: 切换代理 restart consumer {} using new proxy {} "
                            .format(consumer_id, new_proxy))
                    consumers_tasks.pop(done_task, None)  # 退出
                    consumers_tasks[new_consumer_task] = consumer_id  # 重新加入
                except MyException as e:
                    logging.debug("!!!!state2:{}".format(e.args))
                    (error_cnt, total_cnt) = e.args
                    error_task_unique += error_cnt
                    total_task_unique += total_cnt
                    # traceback.print_exc()
                    consumer_id = consumers_tasks.get(done_task, -1)
                    consumers_tasks.pop(done_task, None)  # 退出
                    # logging.exception(
                    #     "main loop: !!!!!!!未预测到的Exception退出customer{}:".format(consumer_id))
                    logging.critical(
                        "main loop: !!!!!!!未预测到的Exception退出customer{}:".format(consumer_id))

    logging.info("main loop: queue is empty, exiting waiting queue")

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await task_q.join()
    total_slept_for = time.monotonic() - started_at
    logging.info("main loop: total ruing time {}".format(total_slept_for))
    for task in consumers_tasks:
        task.cancel()
    results = await asyncio.gather(*consumers_tasks, return_exceptions=True)
    for res in results:
        logging.debug("!!!!state3:nothere {},{}".format(type(res), res))
        if isinstance(res, MyException):
            logging.debug("!!!!state4:{}".format(res.args))
            (error_cnt, total_cnt) = res.args
            error_task_unique += error_cnt
            total_task_unique += total_cnt

    logging.debug("main loop: final result {} ".format(results))
    success_ratio = 1
    if total_task_unique > 0:
        success_ratio = (total_task_unique - error_task_unique) * 1.0 / total_task_unique
    await producer_task
    logging.critical(
        "main loop: final result total task:{} error task:{} success_ratio:{}% ".format(
            total_task_unique, error_task_unique, success_ratio * 100))


event_loop = asyncio.get_event_loop()
try:
    event_loop.run_until_complete(main(event_loop, __query_list__))
finally:
    event_loop.close()