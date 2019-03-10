# 最终方案：使用asyncio+requests_futures
# https://stackoverflow.com/questions/34376814/await-future-from-executor-future-cant-be-used-in-await-expression/34376938
# https://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio
# 文档：https://docs.python.org/zh-cn/3/library/asyncio-task.html

import json
import asyncio
import requests
# from requests_futures.sessions import FuturesSession
from itertools import islice
import logging
import random
from functools import partial


# __future_session = FuturesSession(max_workers=50)

def request_url_async(url, proxy, timeout=3):
    get_request = partial(requests.get, proxies=proxy, timeout=timeout)
    f = asyncio.get_event_loop().run_in_executor(None,
                                                 get_request, url)
    return f


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
    if proxy == {}:
        return {}
    return {proxy['type']: proxy['host'] + ":" + str(proxy['port'])}


def filter_proxy(proxy):
    return convert_to_request_proxy(proxy)
    if proxy['anonymity'] != 'high_anonymous':
        return None
    p = {
        proxy['type']: proxy['host'] + ":" + str(proxy['port']),
    }

    try:
        # 这句话也会有异常！！！！，这个奇葩的库，先这么用把
        # f = __future_session.get("https://www.baidu.com", proxies=p, timeout=2)
        # 这块是concurrent.futures.Future转成asyncio.Future,await 求值有抛出可能异常因为concurrent的result()
        resp = request_url_async("https://www.baidu.com", p)
        if resp and resp.ok:
            return convert_to_request_proxy(proxy)
    except Exception as e:
        logging.exception("request error")
        return None
    # logging.warning(resp)

    return None


# 预处理
proxy_list = load_proxy_file()
proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous', proxy_list)


def get_proxy_pool(num):
    global proxy_list_iter
    res = []
    while len(res) < num:
        res.append(get_proxy_avaliable())
    # level = 0
    # while num > 0 and level < 3:
    #     logging.info("proxy search parallel search:{}".format(num))
    #     check_list = list(islice(proxy_list_iter, 0, num))
    #     done, pending = await asyncio.wait(map(filter_proxy, check_list))
    #     pl = [t for t in [x.result() for x in done] if t is not None]
    #     if len(check_list) < num:
    #         proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous',
    #                                  load_proxy_file())
    #         level += 1
    #     res.extend(pl)
    #     num = num - len(pl)
    return res


def get_proxy_avaliable():
    global proxy_list_iter
    res = next(proxy_list_iter, None)
    if res is None:
        proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous',
                                 load_proxy_file())
        res = next(proxy_list_iter, {})  # 默认给个空
    return convert_to_request_proxy(res)
    # return (await get_proxy_pool(1))[0]


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
    return pool


if __name__ == '__main__':
    async def main():
        # t = await get_proxy_pool(100)
        # await refresh_proxy_pool(t, force=True)
        print("proxy list async return: ", t)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
