import logging
import json
from os import environ as env
from core.pika import PikaConsumer, LOGGER, LOG_FORMAT
from core.db import get_engine_session
from models import UploadQueueLog
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=env.get(
    'LOG_LEVEL', logging.INFO), format=LOG_FORMAT)

AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')


class UploadQueueLogConsumer(PikaConsumer):
    QUEUE = 'upload_logs'
    EXCHANGE = 'default'
    ROUTING_KEY = 'default.upload'

    _session = None

    def on_connection_closed(self, *args, **kwargs):
        super().on_connection_closed(*args, **kwargs)
        if self._session is not None:
            self._session.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from [%s, %s]: %s',
                    basic_deliver.delivery_tag, properties.app_id,
                    properties.message_id, body)
        try:
            body = json.loads(body)
            if 'email' in body and 'driveid' in body:
                self.db_session.add(UploadQueueLog(
                    message_id=properties.message_id,
                    email=body['email'],
                    drive_id=body['driveid']))
                self.db_session.commit()
        except json.JSONDecodeError:
            LOGGER.error('JSONDecodeError')
        except SQLAlchemyError as exc:
            self.db_session.rollback()
            LOGGER.info(exc)
        except Exception as exc:
            LOGGER.error('Something went wrong! {}'.format(exc))
        self.acknowledge_message(basic_deliver.delivery_tag)

    @property
    def db_session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session


def main():
    consumer = UploadQueueLogConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
