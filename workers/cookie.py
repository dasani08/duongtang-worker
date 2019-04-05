import logging
import json
from os import environ as env
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session
from models import Config


AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')


class CookieConsumer(PikaConsumer):
    QUEUE = 'cookie'
    ROUTING_KEY = 'default.cookie'
    _session = None

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
                LOGGER.info(
                    "Cookie of email: {} has been disabled".format(
                        body['email']))
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


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    consumer = CookieConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
