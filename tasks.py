import os
import time
from celery import Celery

env = os.environ
CELERY_BROKER_URL = env.get(
    'CELERY_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')

celery = Celery('tasks',
                broker=CELERY_BROKER_URL)


@celery.task(name='duongtang.add')
def add(x, y):
    # lets sleep for a while before doing the gigantic addition task!
    time.sleep(5)
    return x + y
