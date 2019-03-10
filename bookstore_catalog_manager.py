from collections import OrderedDict
from pathlib import Path
import os
import logging
import json


class BookCatalogManager:
    catalog = OrderedDict()
    done = set()
    onedrive = set()
    catalog_file = None
    done_file = None
    onedrive_file = None
    store_path = None

    urls = OrderedDict()

    def __enter__(self):
        return self

    def __exit__(self):
        try:
            if BookCatalogManager.catalog_file is not None:
                BookCatalogManager.catalog_file.close()
            if BookCatalogManager.done_file is not None:
                BookCatalogManager.done_file.close()
            if BookCatalogManager.onedrive_file is not None:
                BookCatalogManager.onedrive_file.close()
        except Exception as e:
            logging.exception("close file error")

    @staticmethod
    def load_catalog_info(bookstore_dir_path):
        BookCatalogManager.store_path = bookstore_dir_path
        catalog_path = os.path.join(bookstore_dir_path, "__CATALOG__")
        done_path = os.path.join(bookstore_dir_path, "__DONE__")
        onedrive_path = os.path.join(bookstore_dir_path, "__ONEDRIVE__")

        # Opens a file for appending. The file pointer is at the end of the file if the file exists.
        # That is, the file is in the append mode. If the file does not exist, it creates a new file for writing.

        # rel_book_path = os.path.relpath(book_dir_path, __BOOK_STORE_PATH__)
        # logging.debug(catalog_path)
        # os.path.exists(catalog_path) or Path(catalog_path).touch()
        # Opens a file for both reading and writing.
        # The file pointer will be at the beginning of the file.
        Path(catalog_path).touch()
        Path(done_path).touch()
        Path(onedrive_path).touch()
        BookCatalogManager.catalog_file = open(catalog_path, "r+", encoding='utf8')
        BookCatalogManager.done_file = open(done_path, "r+", encoding='utf8')
        BookCatalogManager.onedrive_file = open(onedrive_path, 'r+', encoding='utf8')
        for line in BookCatalogManager.catalog_file:
            if line:
                arr = line.strip().split("\t")  # 去除\n
                book_rel_path = None
                book_url = None
                if len(arr) == 1:
                    book_rel_path = arr[0]
                if len(arr) == 2:
                    book_rel_path = arr[0]
                    book_url = arr[1]
                if book_rel_path and not book_url:
                    # BookCatalogManager.catalog.add(book_rel_path)
                    book_detail = BookCatalogManager.load_manifest_detail(
                        os.path.join(bookstore_dir_path, book_rel_path))
                    if book_detail is not None:
                        BookCatalogManager.catalog[
                            book_rel_path] = book_detail["book_url"]
                        BookCatalogManager.urls[book_detail["book_url"]] = book_rel_path
                    else:
                        BookCatalogManager.catalog[book_rel_path] = None
                if book_rel_path and book_url:
                    BookCatalogManager.catalog[
                        book_rel_path] = book_url
                    BookCatalogManager.urls[book_url] = book_rel_path

        for line in BookCatalogManager.done_file:
            if line:
                arr = line.split()
                book_rel_path = None
                if len(arr) == 1:
                    book_rel_path = arr[0]
                if book_rel_path:
                    BookCatalogManager.done.add(book_rel_path)
        for line in BookCatalogManager.onedrive_file:
            if line:
                arr = line.split()
                book_rel_path = None
                if len(arr) == 1:
                    book_rel_path = arr[0]
                if book_rel_path:
                    BookCatalogManager.onedrive.add(book_rel_path)

    @staticmethod
    def add_book_to_catalog(book_dir_path, book_url):
        if BookCatalogManager.catalog_file is None:
            raise Exception("not init")
        rel_book_path = os.path.relpath(book_dir_path, BookCatalogManager.store_path)
        if rel_book_path in BookCatalogManager.catalog:
            return True
        if BookCatalogManager.catalog_file is not None:
            BookCatalogManager.catalog_file.write("\t".join([rel_book_path, book_url]) + "\n")
            BookCatalogManager.catalog_file.flush()
            BookCatalogManager.catalog[rel_book_path] = book_url
            BookCatalogManager.urls[book_url] = rel_book_path

    @staticmethod
    def is_manifest_detail_downloaded(book_url):
        if BookCatalogManager.urls is None:
            raise Exception("not init")
        if book_url and book_url in BookCatalogManager.urls:
            return True
        return False

    @staticmethod
    def load_manifest_detail_by_url(book_url):
        return BookCatalogManager.load_manifest_detail(
            os.path.join(BookCatalogManager.store_path, BookCatalogManager.urls[book_url]))

    @staticmethod
    def load_manifest_detail(book_path):
        path = os.path.join(book_path, "__MANIFEST_DETAIL__")
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(e)
            return None
        return None

    @staticmethod
    def add_done_to_catalog(book_url):
        if BookCatalogManager.done_file is None:
            raise Exception("not init")
        if book_url in BookCatalogManager.done:
            return
        if BookCatalogManager.done_file is not None:
            BookCatalogManager.done_file.write(book_url + "\n")
            BookCatalogManager.done_file.flush()
            BookCatalogManager.done.add(book_url)

    @staticmethod
    def is_status_done(book_url):
        if BookCatalogManager.done is None:
            raise Exception("not init")
        if book_url in BookCatalogManager.done:
            return True
        return False

    @staticmethod
    def append_to_book_onedrive_file(book_dir_path):
        """
        加入onedriver发送
        :param book_info:
        :return:
        """
        # fixme main thread
        rel_book_path = os.path.relpath(book_dir_path, BookCatalogManager.store_path)
        BookCatalogManager.onedrive_file.write(str(rel_book_path) + "\n")
        BookCatalogManager.onedrive_file.flush()
        BookCatalogManager.onedrive.add(rel_book_path)

    @staticmethod
    def is_status_onedrive_byurl(book_url):
        """
        是否已经onedriver发送
        :param book_url:
        :return:
        """
        # fixme main thread
        if book_url in BookCatalogManager.urls:
            if BookCatalogManager.urls[book_url] in BookCatalogManager.onedrive:
                return True
        return False

    @staticmethod
    def is_status_onedrive(book_path):
        """
        是否已经onedriver发送
        :param book_path:
        :return:
        """
        if book_path in BookCatalogManager.onedrive:
            return True
        return False

    @staticmethod
    def refresh_new_catalog(bookstore_dir_path):
        for k, v in BookCatalogManager.catalog.items():
            print(k, v)
        os.rename(os.path.join(bookstore_dir_path, "__CATALOG__"),
                  os.path.join(bookstore_dir_path, "__CATALOG__OLD__"))
        catalog_path = os.path.join(bookstore_dir_path, "__CATALOG__")
        # BookCatalogManager.catalog_file = open(catalog_path, "r+", encoding='utf8')
        # Opens a file for appending. The file pointer is at the end of the file if the file exists.
        # That is, the file is in the append mode. If the file does not exist, it creates a new file for writing.
        with open(catalog_path, 'a', encoding='utf8') as manifest:
            for k, v in BookCatalogManager.catalog.items():
                if k is not None and v is not None:
                    manifest.write("\t".join([k, v]) + "\n")
                else:
                    print("error:", k, v)
        pass

    # @staticmethod
    # def get_all_stat_cnt():
    #     # todo 使用walk实现
    #     catalog_path = os.path.join(BookCatalogManager.store_path, "__CATALOG__")
    #     sum_book = 0
    #     sum_chapter = 0
    #     with open(catalog_path, "r", encoding='utf8') as f:
    #         for line in f:
    #             if line:
    #                 arr = line.strip().split("\t")  # 去除\n
    #                 book_rel_path = None
    #                 book_url = None
    #                 if len(arr) == 2:
    #                     book_rel_path = arr[0]
    #                     book_url = arr[1]
    #                     # BookCatalogManager.catalog.add(book_rel_path)
    #                     if not BookCatalogManager.is_status_onedrive(book_rel_path):
    #                         book_detail = BookCatalogManager.load_manifest_detail(
    #                             os.path.join(BookCatalogManager.store_path, book_rel_path))
    #                         sum_book += 1
    #                         sum_chapter += len(book_detail["chapter_list"])
    #     # 减去booklist.json __CATALOG__ __DONE__三个
    #     dir_list = next(os.walk(BookCatalogManager.store_path))[1]
    #     # 各个类别的下载的文件数目
    #     total_down_file_cnt = [sum([len(files) for r, d, files in
    #                                 os.walk(os.path.join(BookCatalogManager.store_path, dir_item))])
    #                            for
    #                            dir_item in dir_list]
    #
    #     downloaded_chapter = sum(total_down_file_cnt) - sum_book  # 每个书有个manifest文件
    #     return sum_book, sum_chapter, downloaded_chapter

    @staticmethod
    def get_all_stat_cnt(query_list=[]):
        # todo 使用walk实现
        query_url_set = set([query["book_url"] for query in query_list])

        catalog_path = os.path.join(BookCatalogManager.store_path, "__CATALOG__")
        sum_book_catalog = len(BookCatalogManager.catalog)  # 目录记录&&在本地中的数量 （不在也可能，比输入手动删了catalog）
        sum_book_local = 0
        sum_book_detail = 0  # 含有__MANIFEST_DETAIL__文件
        sum_book_valid = 0  # 即在catalog中，又在本地有且下载的json有效

        sum_book_downloaded = 0
        sum_book_onedrive = 0
        sum_book_done = 0
        sum_chapter = 0
        sum_chapter_downloaded = 0

        for root, dirs, files in os.walk(BookCatalogManager.store_path):
            # for dir in dirs:
            #     path=os.path.join(root,dir)
            level_form_store = root.count(os.sep)
            # 当前都在 ./book_store/类型/作者/书名 目录下（4层）
            if level_form_store == 4:
                sum_book_local += 1

                sum_cur_book_chapter_downloaded = 0
                for file in files:
                    if file == "__MANIFEST_DETAIL__":
                        sum_book_detail += 1
                    elif not (file.endswith(".tmp") or file.startswith("__")):
                        sum_cur_book_chapter_downloaded += 1
                # sum_chapter_downloaded += sum_cur_book_chapter_downloaded
                # if sum_cur_book_chapter_downloaded== sum_cur_book_chapter:
                #     pass
                rel_path = os.path.relpath(root, BookCatalogManager.store_path)
                if rel_path in BookCatalogManager.catalog:
                    if len(query_url_set) == 0 or \
                            BookCatalogManager.catalog[rel_path] in query_url_set:
                        book_detail = BookCatalogManager.load_manifest_detail(root)
                        if book_detail is not None:
                            sum_book_valid += 1
                            sum_cur_book_chapter = len(book_detail["chapter_list"])
                            sum_chapter += sum_cur_book_chapter

                            # 前提条件：done和onedrive的manifest在本地目录中！！！

                            # 完结的书统计
                            if BookCatalogManager.catalog[rel_path] in BookCatalogManager.done:
                                sum_book_done += 1
                            # 已经发送给onedrive，删除了本地文件的情况，可以任务已经下载了所有的
                            if rel_path in BookCatalogManager.onedrive:
                                sum_book_onedrive += 1
                                # 补充一下发送过去的内容
                                sum_book_downloaded += 1
                                sum_chapter_downloaded += sum_cur_book_chapter
                            else:
                                # 一般情况的统计
                                sum_chapter_downloaded += sum_cur_book_chapter_downloaded
                                if sum_cur_book_chapter_downloaded == sum_cur_book_chapter:
                                    sum_book_downloaded += 1
        logging.warning(
            "sum_book_catalog:{} sum_book_local:{} sum_book_detail:{} sum_book_valid:{} sum_book_downloaded:{} sum_book_done:{} sum_book_onedrive:{}".format(
                sum_book_catalog,
                sum_book_local,
                sum_book_detail,
                sum_book_valid,
                sum_book_downloaded,
                sum_book_done,
                sum_book_onedrive
            ))
        logging.warning(
            "sum_chapter:{} sum_chapter_downloaded:{}".format(
                sum_chapter,
                sum_chapter_downloaded
            ))
        return sum_book_valid, sum_chapter, sum_chapter_downloaded



if __name__ == "__main__":
    BookCatalogManager.load_catalog_info("./book_store")
    import time

    started_at = time.monotonic()
    print(BookCatalogManager.get_all_stat_cnt(query_list=[{"book_url": "http://www.aoyuge.com/12/12610/index.html"}]))
    total_slept_for = time.monotonic() - started_at
    print(total_slept_for)
