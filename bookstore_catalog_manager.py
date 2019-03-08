from collections import OrderedDict
from pathlib import Path
import os
import json
import logging

class BookCatalogManager:
    catalog = OrderedDict()
    done = set()
    catalog_file = None
    done_file = None
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
        except Exception as e:
            logging.exception("close file error")


    @staticmethod
    def load_catalog_info(bookstore_dir_path):
        BookCatalogManager.store_path = bookstore_dir_path
        catalog_path = os.path.join(bookstore_dir_path, "__CATALOG__")
        done_path = os.path.join(bookstore_dir_path, "__DONE__")
        # rel_book_path = os.path.relpath(book_dir_path, __BOOK_STORE_PATH__)
        # logging.debug(catalog_path)
        # os.path.exists(catalog_path) or Path(catalog_path).touch()
        # Opens a file for both reading and writing.
        # The file pointer will be at the beginning of the file.
        Path(catalog_path).touch()
        Path(done_path).touch()
        BookCatalogManager.catalog_file = open(catalog_path, "r+", encoding='utf8')
        BookCatalogManager.done_file = open(done_path, "r+", encoding='utf8')
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
    def load_manifest_detail_by_url(book_path):
        BookCatalogManager.load_manifest_detail(
            os.path.join(BookCatalogManager.store_path, BookCatalogManager.urls[book_path]))

    @staticmethod
    def load_manifest_detail(book_path):
        path = os.path.join(book_path, "__MANIFEST_DETAIL__")
        p = Path(path)
        if p.exists():
            try:
                return json.load(open(path, encoding='utf-8'))
            except Exception as e:
                print(e)
                return None
        else:
            return None

    @staticmethod
    def add_done_to_catalog(book_url):
        if BookCatalogManager.done_file is None:
            raise Exception("not init")
        if book_url in BookCatalogManager.done:
            return
        if BookCatalogManager.done_file is not None:
            BookCatalogManager.done_file.write(book_url + "\n")
            BookCatalogManager.done.add(book_url)

    @staticmethod
    def is_status_done(book_url):
        if BookCatalogManager.done is None:
            raise Exception("not init")
        if book_url in BookCatalogManager.done:
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


if __name__ == "__main__":
    BookCatalogManager.load_catalog_info("./book_store")
