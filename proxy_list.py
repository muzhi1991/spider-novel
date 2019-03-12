import json
import logging
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from itertools import islice


def load_proxy_file():
    p = dict(http='http://127.0.0.1:1087',
             https='http://127.0.0.1:1087')
    try:
        if requests.get("http://www.google.com", timeout=3).ok:
            p = {}
    except Exception as ex:
        pass
    proxy_text = requests.get(
        "https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list", proxies=p).text
    proxy_list = [json.loads(line) for line in proxy_text.splitlines()]
    random.shuffle(proxy_list)
    logging.info("刷新代理源 获得代理{}个".format(len(proxy_list)))
    return proxy_list


def convert_to_request_proxy(proxy):
    return {proxy['type']: proxy['host'] + ":" + str(proxy['port'])}


def filter_proxy(proxy):
    if proxy['anonymity'] != 'high_anonymous':
        return None
    p = {
        proxy['type']: proxy['host'] + ":" + str(proxy['port']),
    }
    try:
        res = requests.get("https://www.baidu.com", proxies=p, timeout=2)
        if res.ok:
            return convert_to_request_proxy(proxy)
    except Exception as err:
        logging.debug(str(err))
    return None


executor = ThreadPoolExecutor(max_workers=50)
# result = executor.map(filter_proxy, proxy_list) # iter

# 预处理
proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous', load_proxy_file())


# proxy_list_iter=iter(load_proxy_file())
def get_proxy_pool(num):
    global proxy_list_iter
    res = []
    level = 0
    while num > 1 and level < 3:
        logging.info("parallel search:{}".format(num))
        check_list = list(islice(proxy_list_iter, 0, num))
        pl = [x for x in executor.map(filter_proxy, check_list) if x is not None]
        if len(check_list) < num:
            proxy_list_iter = load_proxy_file()
            level += 1
        res.extend(pl)
        num = num - len(pl)
    # linear search, 只有一个的时候线性搜索
    if num > 0 and level < 3:
        logging.info("linear search:{}".format(num))
        res.extend(map(convert_to_request_proxy,
                       islice(filter(lambda p: filter_proxy(p) is not None, proxy_list_iter), num)))
    return res


def get_proxy_avaliable():
    return get_proxy_pool(1)[0]


def refresh_proxy_pool(pool, index=-1, force=False):
    global proxy_list_iter
    if force:
        proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous', load_proxy_file())

    if index == -1:
        p2 = get_proxy_pool(len(pool))
        pool.clear()
        pool.extend(p2)
    else:
        pool[index] = get_proxy_avaliable()
