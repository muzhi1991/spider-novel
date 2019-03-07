import logging
import logging.handlers
import logging.config
import os
import sys

try:
    import colorlog
except ImportError:
    pass


def setup_for_color_console(config):
    """
    安装colorlog(pip install colorlog)，如果要在idea中显示，在Configuration中打开「Emulate terminal in output console」
    :param config:
    :return:
    """
    if 'colorlog' in sys.modules and os.isatty(2):
        format = '%(asctime)s - %(name)s - %(levelname)-7s - %(message)s'
        format = '%(asctime)s - %(name)s - %(levelname)-7s - %(filename)s[line:%(lineno)d] - %(funcName)s() -  %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        cformat = '%(log_color)s' + format
        config['formatters']['colored_console'] = {'()': 'colorlog.ColoredFormatter',
                                                   'format': cformat,
                                                   'datefmt': date_format,
                                                   'log_colors': {'DEBUG': 'reset', 'INFO': 'reset',
                                                                  'WARNING': 'bold_yellow',
                                                                  'ERROR': 'bold_red',
                                                                  'CRITICAL': 'bold_red'}}

        config['handlers']['console']['formatter'] = 'colored_console'
        return config
    else:
        return config


#
# setup_logging()
# log = logging.getLogger(__name__)


def configure_root_logger(log_path):
    """
    配置根logging，如果安装colorlog就有颜色
    建议：为了方便这里配置的是root，建议给每个文件配置logger logger = logging.getLogger(__name__)
    :param log_path: 日志路径，是文件位置，如 logs/app.log 而不是非目录
    :return:
    """
    # check output dir 必须存在
    os.path.exists(os.path.dirname(log_path)) or os.makedirs(os.path.dirname(log_path))
    # 除了dictConfig方式外，还有代码方式和配置文件的方式
    # 此外还可以设置filter
    config = {
        'version': 1,
        'formatters': {
            'simple': {
                'format': '%(asctime)s - %(name)s - %(levelname)-7s - %(filename)s[line:%(lineno)d] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'default': {
                'format': '%(asctime)s - %(name)s - %(levelname)-7s -  %(filename)s[line:%(lineno)d] - %(funcName)s() -  %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',  # 控制台输出debug/info信息
                'class': 'logging.StreamHandler',
                'formatter': 'default',  # 输出详细格式信息
                # 'stream': 'ext://sys.stdout' # 默认输出到stderr
            },
            'file': {  # 保留最近3个天的错误信息到文件
                'level': 'WARN',  # 文件防止太大，输出错误
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'formatter': 'simple',
                'when': 'D',  # 天
                'interval': 1,  # 间隔一个小时生成一个文件
                'backupCount': 3,  # 保留最近3个文件
                'filename': log_path,  # 当前天的文件就是这个名字，后面的会有时间suffix
                "encoding": "utf8"
            }
        },
        'loggers': {
            '': {  # root logger 等价 logging.getLogger()
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': True

            },
            'debug': {  # 这里没有用 logging.getLogger("online") 获得
                'level': 'DEBUG',
                'handlers': ['file', 'console'],
                'propagate': False  # 如果为True（默认），会向上到root的logger都执行
            },
            'info': {  # 这里没有用 logging.getLogger("online") 获得
                'level': 'INFO',
                'handlers': ['file', 'console'],
                'propagate': False  # 如果为True（默认），会向上到root的logger都执行
            },
            'online': {  # 这里没有用 logging.getLogger("online") 获得
                'level': 'WARN',
                'handlers': ['file'],
                'propagate': False  # 如果为True（默认），会向上到root的logger都执行
            }
        },
        'disable_existing_loggers': False
    }

    setup_for_color_console(config)
    logging.config.dictConfig(config)

    logging.debug("logging文件(保留3天的warning日志)位置:%s", log_path)
    # 返回默认的root logger，就是logging.xxx使用的
    return logging.getLogger()
