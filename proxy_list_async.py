# 最终方案：使用asyncio+requests_futures
# https://stackoverflow.com/questions/34376814/await-future-from-executor-future-cant-be-used-in-await-expression/34376938
# https://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio
# 文档：https://docs.python.org/zh-cn/3/library/asyncio-task.html

import json
import asyncio
import requests
from requests_futures.sessions import FuturesSession
from itertools import islice

session = FuturesSession(max_workers=50)


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
    print("get proxy:", len(proxy_list))
    return proxy_list


def convert_to_request_proxy(proxy):
    return {proxy['type']: proxy['host'] + ":" + str(proxy['port'])}


async def filter_proxy(proxy):
    if proxy['anonymity'] != 'high_anonymous':
        return None
    p = {
        proxy['type']: proxy['host'] + ":" + str(proxy['port']),
    }

    f = session.get("https://www.baidu.com", proxies=p, timeout=2)

    try:
        # 这块是concurrent.futures.Future转成asyncio.Future,await 求值有抛出可能异常因为concurrent的result()
        resp = await asyncio.wrap_future(f)
    except Exception as e:
        print("request error", e)
        return None
    if resp.ok:
        return convert_to_request_proxy(proxy)
    return None


# 预处理
proxy_list_iter = filter(lambda p: p['anonymity'] == 'high_anonymous', load_proxy_file())


async def get_proxy_pool(num):
    global proxy_list_iter
    res = []
    level = 0
    while num > 0 and level < 3:
        print("parallel search:", num)
        check_list = list(islice(proxy_list_iter, 0, num))
        done, pending = await asyncio.wait(map(filter_proxy, check_list))
        pl = [t for t in [x.result() for x in done] if t is not None]
        if len(check_list) < num:
            proxy_list_iter = load_proxy_file()
            level += 1
        res.extend(pl)
        num = num - len(pl)
    return res


async def get_proxy_avaliable():
    return (await get_proxy_pool(1))[0]


async def refresh_proxy_pool(pool, index=-1):
    if index == -1:
        p2 = await get_proxy_pool(len(pool))
        pool.clear()
        pool.extend(p2)
    else:
        pool[index] = await get_proxy_avaliable()
    return pool


if __name__ == '__main__':
    async def main():
        t = await get_proxy_pool(100)
        print("proxy list async return: ", t)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
