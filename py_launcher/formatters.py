import datetime


def to_date(time_ms: int) -> str:
    return datetime.datetime.fromtimestamp(time_ms / 1000).strftime('%H:%M:%S.%f [%Y-%m-%d]')


def to_duration(time_nanos: int) -> str:
    time_micros = time_nanos / 1000

    time_ms = time_micros / 1000
    if time_ms < 1:
        return "{} µs".format(int(time_micros))

    time_sec = time_ms / 1000
    if time_sec < 1:
        return "{} ms".format(int(time_ms))

    return str(datetime.timedelta(microseconds=time_micros))


def format_size(nbytes: int) -> str:
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])