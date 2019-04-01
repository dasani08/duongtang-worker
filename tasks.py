import os
import time
from celery import Celery
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

env = os.environ
CELERY_BROKER_URL = env.get(
    'CELERY_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')
SQLALCHEMY_DATABASE_URI = env.get(
    'SQLALCHEMY_DATABASE_URI',
    'mysql+pymysql://root:duongtang2019@127.0.0.1/duongtang?charset=utf8')

app = Celery('tasks',
             broker=CELERY_BROKER_URL)


def get_engine_session():
    Session = sessionmaker()
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Session.configure(bind=engine)
    return Session()


class DbTask(app.Task):
    _session = None

    def after_return(self, *args, **kargs):
        if self._session is not None:
            self._session.remove()

    @property
    def session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session


@app.task(base=DbTask, name='duongtang.add', bind=True)
def add(x, y):
    # lets sleep for a while before doing the gigantic addition task!
    time.sleep(5)
    return x + y
