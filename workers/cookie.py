import logging
import json
from os import environ as env
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session
from models import Config, UploadQueueLog, GDRIVE_API_KEY, GDRIVE_COOKIE_KEY
from workers.upload import UploadPublisher


AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')


class CookieConsumer(PikaConsumer):
    QUEUE = 'cookie'
    ROUTING_KEY = 'default.cookie'

    _session = None
    _upload_publisher = None

    @property
    def upload_publisher(self):
        if self._upload_publisher is None:
            self._upload_publisher = UploadPublisher(self._url)
        return self._upload_publisher

    def on_connection_closed(self, *args, **kwargs):
        super().on_connection_closed(*args, **kwargs)
        if self._session is not None:
            self._session.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        try:
            body = json.loads(body)
            if 'email' in body:
                Config.disable_cookie(self.db_session, body['email'])
                # Disable all stream links relating to this email
                LOGGER.info(
                    "Cookie of email: {} has been disabled".format(
                        body['email']))
                # Gets all upload_queue_logs
                upload_queue_logs = self.db_session.query(
                    UploadQueueLog).filter_by(
                        email=body['email'],
                        status=UploadQueueLog.STATUS_READY).all()
                # Start db transaction
                # 1. Requeue for uploader
                for log in upload_queue_logs:
                    self._reupload(log.drive_id)
                # 2. Update status for upload queue logs
                self.db_session.query(UploadQueueLog).filter_by(
                    email=body['email']
                ).update({
                    'status': UploadQueueLog.STATUS_CANCEL
                })
                self.db_session.commit()
        except json.JSONDecodeError:
            LOGGER.error('JSONDecodeError')
        except Exception as exc:
            LOGGER.error('Something went wrong! {}'.format(exc))
        self.acknowledge_message(basic_deliver.delivery_tag)

    @property
    def db_session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session

    def _reupload(self, drive_id):
        # Get api key
        api_key = Config.get_one_by_key(
            self.db_session, GDRIVE_API_KEY)
        cookie = Config.get_one_by_key(
            self.db_session, GDRIVE_COOKIE_KEY)
        if api_key is None:
            raise Exception('No api key is available')
        # Get cookie
        if cookie is None:
            raise Exception('No cookie is available')
        msg = {
            'cookie': cookie['value'],
            'apikey': api_key['value'],
            'driveid': drive_id,
            'email': cookie['group']
        }
        self.upload_publisher.publish(
            json.dumps(msg, ensure_ascii=False))


def main():
    logging.basicConfig(level=env.get(
        'LOG_LEVEL', logging.INFO), format=LOG_FORMAT)
    consumer = CookieConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
