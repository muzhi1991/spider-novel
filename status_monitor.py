from tqdm import tqdm
import sys
from collections import OrderedDict


class StatusMonitor:
    status = OrderedDict()
    monitor_key_list = []

    @staticmethod
    def set_monitor(key, total, unit, initial_val=0, desc=None):
        t = tqdm(total=total, unit=unit, file=sys.stdout, initial=initial_val)
        t.update(0)
        if desc is None:
            t.set_description(key)
        else:
            t.set_description(desc)
        StatusMonitor.status[key] = {"total": total, "unit": unit, "val": initial_val, "tqdm": t}

    @staticmethod
    def del_monitor(key):
        if key in StatusMonitor.status:
            r = StatusMonitor.status.pop(key)
            r["tqdm"].close()
            if "book" in StatusMonitor.status:
                StatusMonitor.update_monitor("book")

    @staticmethod
    def update_monitor(key, increment=1):
        if key in StatusMonitor.status:
            item = StatusMonitor.status[key]
            item["val"] += increment
            item["tqdm"].update(increment)
            # 100%后自动删除
            if item["val"] == item["total"]:
                StatusMonitor.del_monitor(key)
