import os
from celery import Celery
from db import get_engine_session

env = os.environ
CELERY_BROKER_URL = env.get(
    'CELERY_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')

celery = Celery(__name__,
                broker=CELERY_BROKER_URL)


class DbTask(celery.Task):
    _session = None

    def after_return(self, *args, **kargs):
        if self._session is not None:
            self._session.remove()

    @property
    def session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session
