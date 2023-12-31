from datetime import datetime


def time(time_unit):
    dt = datetime.now()

    if time_unit == 's':
        return dt.second
    elif time_unit == 'ms':
        return dt.microsecond
    else:
        exit(-1)
