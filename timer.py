import time
import math


def cur_time_ms():
    return math.floor(time.time_ns() / 1000000)
