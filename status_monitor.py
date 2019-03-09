from tqdm import tqdm
import sys
from collections import OrderedDict
import heapq


class StatusMonitor:
    status = OrderedDict()
    monitor_pos_h = []
    total_pos = 0
    flag = True

    @staticmethod
    def set_monitor(key, total, unit, initial_val=0, desc=None, callback=None, progress=True):
        if not StatusMonitor.flag: return
        pos = 0
        t = None
        if progress and StatusMonitor.flag:
            if len(StatusMonitor.monitor_pos_h) != 0:
                pos = heapq.heappop(StatusMonitor.monitor_pos_h)
            else:
                pos = StatusMonitor.total_pos
            StatusMonitor.total_pos += 1
            t = tqdm(total=total, unit=unit, file=sys.stdout, initial=initial_val,
                     position=pos)
            t.update(0)
            t.set_description(desc)
            if desc is None:
                desc = key

        StatusMonitor.status[key] = {"total": total, "unit": unit, "val": initial_val,
                                     "tqdm": t,
                                     "desc": desc, "pos": pos, "callback": callback,
                                     "progress": progress}

    @staticmethod
    def del_monitor(key):
        if key in StatusMonitor.status:
            r = StatusMonitor.status.pop(key)
            if StatusMonitor.total_pos and r["progress"]:
                r["tqdm"].close()
                heapq.heappush(StatusMonitor.monitor_pos_h, r["pos"])
            if "callback" in r and r["callback"] is not None:
                r["callback"](key)
            if "sum" in StatusMonitor.status:
                StatusMonitor.update_monitor("sum")

    @staticmethod
    def update_monitor(key, increment=1):
        if key in StatusMonitor.status:
            item = StatusMonitor.status[key]
            # 更新位置
            if StatusMonitor.total_pos and item["progress"]:
                if len(StatusMonitor.monitor_pos_h) != 0:
                    pos = item["pos"]
                    if pos > StatusMonitor.monitor_pos_h[0]:
                        pos = heapq.heapreplace(StatusMonitor.monitor_pos_h, pos)
                        item["pos"] = pos
                        item["tqdm"].clear()
                        item["tqdm"].close()
                        item["tqdm"] = tqdm(total=item["total"], unit=item["unit"], file=sys.stdout,
                                            initial=item["val"],
                                            position=item["pos"])
                item["tqdm"].update(increment)
                item["tqdm"].set_description(item["desc"])

            item["val"] += increment
            # 100%后自动删除
            if item["val"] == item["total"]:
                StatusMonitor.del_monitor(key)

