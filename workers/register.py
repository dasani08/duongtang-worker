import logging
import json
from os import environ as env
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session
from workers.upload import UploadPublisher
from core.mail import mailer

AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')

APP_NAME = 'Duongtang'
APP_DOMAIN = 'duongtang.clgt.vn'
APP_NOREPLY_EMAIL_ADDRESS = 'noreply@localhost'


class InvalidMessageError(Exception):
    pass


class NoCookieError(Exception):
    pass


class NoApiKeyError(Exception):
    pass


class RegisterConsumer(PikaConsumer):
    ROUTING_KEY = 'default.register'
    QUEUE = 'register'

    _session = None
    _upload_publisher = None

    def on_connection_closed(self, *args, **kwargs):
        super().on_connection_closed(*args, **kwargs)
        if self._session is not None:
            self._session.close()

    @property
    def db_session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session

    @property
    def upload_publisher(self):
        if self._upload_publisher is None:
            self._upload_publisher = UploadPublisher(self._url)
        return self._upload_publisher

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        try:
            body = json.loads(body)
            confirmation_link = (
                "http://{app_domain}/account/verify?"
                "active_code={active_code}&"
                "expired={expired}&"
                "hash={hash}").format(
                app_domain=APP_DOMAIN,
                active_code=body['active_code'],
                expired=body['expired'],
                hash=body['hash'])
            message = """Subject: {app_name} - Please confirm your registration

Please confirm your registration by click this link {link}""".format(
                app_name=APP_NAME, link=confirmation_link)
            mailer.send(body['email'], message)
        except json.JSONDecodeError:
            LOGGER.error('JSONDecodeError')
        except Exception as exc:
            LOGGER.error('Something went wrong! {}'.format(exc))
        self.acknowledge_message(basic_deliver.delivery_tag)


def main():
    logging.basicConfig(level=env.get(
        'LOG_LEVEL', logging.INFO), format=LOG_FORMAT)
    consumer = RegisterConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
