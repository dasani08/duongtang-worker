from datetime import datetime


def parse_cookie(cookie_str):
    cookies = dict()
    for cookie in cookie_str.split(';'):
        cookie = cookie.split('=', maxsplit=1)
        cookies[cookie[0].strip()] = cookie[1].strip() if len(
            cookie) > 1 else None
    return cookies


def serialize_cookie(cookie_dict):
    cookies = []
    for key, value in cookie_dict.items():
        cookies.append(f'{key}={value}')
    return ';'.join(cookies)


def get_unix_time():
    unix_time = datetime.timestamp(datetime.now())
    unix_time = str(unix_time).replace('.', '')
    unix_time = unix_time.ljust(16, '0')
    unix_time = int(unix_time)
    return unix_time
