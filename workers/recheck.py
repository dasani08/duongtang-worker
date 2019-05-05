import logging
import json
import time
from os import environ as env
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session
from models import Config, GDRIVE_API_KEY, GDRIVE_COOKIE_KEY
from workers.upload import UploadPublisher

AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')


class InvalidMessageError(Exception):
    pass


class NoCookieError(Exception):
    pass


class NoApiKeyError(Exception):
    pass


class RecheckConsumer(PikaConsumer):
    ROUTING_KEY = 'default.recheck'
    QUEUE = 'recheck'

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
        # keep up with uploaders by not working so fast
        time.sleep(0.5)

        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        try:
            body = json.loads(body)
            if 'email' not in body or 'driveid' not in body:
                raise InvalidMessageError('email or driveid is missing')

            Config.limit_cookie(self.db_session, body['email'])
            LOGGER.info(
                "The cookie of email: {} has \
                been expired to tomorow".format(
                    body['email']))
            # Get api key
            api_key = Config.get_one_by_key(
                self.db_session, GDRIVE_API_KEY)
            cookie = Config.get_one_by_key(
                self.db_session, GDRIVE_COOKIE_KEY)
            if api_key is None:
                raise NoApiKeyError('No api key is available')
            # Get cookie
            if cookie is None:
                raise NoCookieError('No cookie is available')
            msg = {
                'cookie': cookie['value'],
                'apikey': api_key['value'],
                'driveid': body['driveid'],
                'email': cookie['group']
            }
            self.upload_publisher.publish(msg)
            self.acknowledge_message(basic_deliver.delivery_tag)
        except json.JSONDecodeError:
            LOGGER.info('JSONDecodeError')
            self.acknowledge_message(basic_deliver.delivery_tag)
        except InvalidMessageError:
            self.acknowledge_message(basic_deliver.delivery_tag)
        except (NoCookieError, NoApiKeyError):
            time.sleep(300)
            pass    # Should keep the message on queue
        except Exception as exc:
            LOGGER.info('Something went wrong! {}'.format(exc))
            self.acknowledge_message(basic_deliver.delivery_tag)


def main():
    logging.basicConfig(level=env.get(
        'LOG_LEVEL', logging.INFO), format=LOG_FORMAT)
    consumer = RecheckConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
