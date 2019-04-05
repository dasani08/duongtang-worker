import requests
import json
import logging
from os import environ as env
from urllib.parse import urlparse, parse_qsl
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session

from core.utils import parse_cookie
from models import Source, Config
from workers.upload import UploadPublisher

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
AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')

uploader = UploadPublisher(AMQP_BROKER_URL)


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
        raise Exception('No api key found')
    if cookie is None:
        raise Exception('No cookie found')

    content = {
        'cookie': cookie['value'],
        'email': cookie['group'],
        'driveid': drive_id,
        'apikey': api_key['value']
    }

    uploader.publish(json.dumps(content, ensure_ascii=False))


class SourceConsumer(PikaConsumer):
    ROUTING_KEY = 'default.source'
    QUEUE = 'source'
    _session = None

    def on_connection_closed(self, *args, **kwargs):
        super().on_connection_closed(*args, **kwargs)
        if self._session is not None:
            self._session.close()

    @property
    def db_session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        try:
            content = json.loads(body)
            drive_id = content['drive_id']
            user_id = content['user_id']

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
            LOGGER.error('Something went wrong! {}'.format(exc))
        self.acknowledge_message(basic_deliver.delivery_tag)


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    consumer = SourceConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
