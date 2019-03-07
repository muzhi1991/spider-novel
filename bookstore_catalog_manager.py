from collections import OrderedDict
from pathlib import Path
import os


class BookCatalogManager:
    catalog = set()
    done = set()
    catalog_file = None
    done_file = None
    store_path = None

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
                arr = line.split()
                book_rel_path = None
                if len(arr) == 1:
                    book_rel_path = arr[0]
                if book_rel_path:
                    BookCatalogManager.catalog.add(book_rel_path)
        for line in BookCatalogManager.done_file:
            if line:
                arr = line.split()
                book_rel_path = None
                if len(arr) == 1:
                    book_rel_path = arr[0]
                if book_rel_path:
                    BookCatalogManager.done.add(book_rel_path)

    @staticmethod
    def add_book_to_catalog(book_dir_path):
        if BookCatalogManager.catalog_file is None:
            raise Exception("not init")
        rel_book_path = os.path.relpath(book_dir_path, BookCatalogManager.store_path)
        if rel_book_path in BookCatalogManager.catalog:
            return True
        if BookCatalogManager.catalog_file is not None:
            BookCatalogManager.catalog_file.write(rel_book_path + "\n")
            BookCatalogManager.catalog.add(rel_book_path)

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
