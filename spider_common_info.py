__ua_list__ = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15",
]

__ua_list_m__ = [
    "Mozilla/5.0 (Linux; Android 7.1.1; OD103) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 MQQBrowser/9.0.3 Mobile/15E148 Safari/604.1 MttCustomUA/2 QBWebViewType/1 WKType/1",
]

__common_headers__ = {'User-Agent': __ua_list__[0],
                      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                      'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
                      }

__common_headers_m__ = {'User-Agent': __ua_list_m__[0],
                      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                      'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
                      }