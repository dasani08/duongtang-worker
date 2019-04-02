import requests
import json
from os import environ as env
from urllib.parse import urlparse, parse_qsl

from core.worker import celery, DbTask
from core.utils import parse_cookie
from models import Source, Config

ITAG_EXTENSION = {
    "5": "flv",
    "6": "flv",
    "13": "3gp",
    "17": "3gp",
    "18": "mp4",
    "22": "mp4",
    "34": "flv",
    "35": "flv",
    "36": "3gp",
    "37": "mp4",
    "38": "mp4",
    "43": "webm",
    "44": "webm",
    "45": "webm",
    "46": "webm",
    "59": "mp4",
}

ITAG_RESOLUTION = {
    "18": "360",
    "59": "480",
    "22": "720",
    "37": "1080",
}

GDRIVE_API_KEY = 'GDRIVE_API_KEY'
GDRIVE_COOKIE_KEY = 'GMAIL_COOKIE'
NUMBER_OF_CLONE = env.get('NUMBER_OF_CLONE', 3)


def get_video_info(drive_id):
    url = f"https://mail.google.com/e/get_video_info?docid={drive_id}"
    r = requests.get(url)

    cookies = parse_cookie(r.headers.get('set-cookie'))
    content = dict(parse_qsl(str(r.content.decode('utf-8'))))

    if "fail" in content.get('status'):
        # Remove drive id from source due to invalid drive id
        raise Exception(content['reason'])

    if ('DRIVE_STREAM' not in cookies):
        # Remove drive id from source due to invalid drive id
        raise Exception("not found drive stream cookie")

    source = {
        'source_id': drive_id,
        'title': content.get('title'),
        'duration': content.get('length_seconds'),
        'cookie': cookies.get('DRIVE_STREAM'),
        'expire': None,
        'source_link': []
    }

    for m in content.get('fmt_stream_map').split(','):
        raw = m.split('|')
        if (len(raw) < 2):
            raise Exception(f'fmt_stream_map: {m}')

        (itag, video_url) = raw

        source_link = {
            'itag': itag,
            'url': video_url,
            'resolution': ITAG_RESOLUTION[itag],
            'extension': ITAG_EXTENSION[itag]
        }

        source['source_link'].append(source_link)

    f = source.get('source_link')[0]
    result = urlparse(f['url'])

    qs = dict(parse_qsl(result.query))
    source['expire'] = qs.get('expire')

    return source


def get_cookie(db_session):
    return Config.get_one_by_key(db_session, GDRIVE_COOKIE_KEY)


def get_api_key(db_session):
    return Config.get_one_by_key(db_session, GDRIVE_API_KEY)


def send_msg_to_uploader(db_session, drive_id):
    api_key = get_api_key(db_session)
    cookie = get_cookie(db_session)

    if api_key is None:
        raise Exception('No api key found, should retry after 60 seconds')
    if cookie is None:
        raise Exception('No cookie found, should retry after 60 seconds')

    content = {
        'cookie': cookie['value'],
        'email': cookie['group'],
        'driveid': drive_id,
        'apikey': api_key['value']
    }

    celery.send_task(
        'duongtang.upload',
        args=[content],
        kwargs={},
        queue='upload', exchange='upload', routing_key='upload')


@celery.task(
    base=DbTask,
    name='duongtang.get_source',
    bind=True,
    acks_late=True)
def get_source(self, drive_id, user_id):
    try:
        source = get_video_info(drive_id)
        source['source_link'] = json.dumps(source['source_link'])
        source['user_id'] = user_id
        # update_source(source)
        self.db_session.query(Source).filter_by(
            source_id=drive_id).update(source)
        self.db_session.commit()
        # TODO: send upload google photo task
        for x in range(NUMBER_OF_CLONE):
            send_msg_to_uploader(self.db_session, drive_id)
    except Exception as exc:
        raise self.retry(exc, countdown=60)
