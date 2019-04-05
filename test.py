import unittest
from core.pika import PikaPublisher
from os import environ as env


class RecheckPublisher(PikaPublisher):
    QUEUE = 'recheck'
    ROUTING_KEY = 'default.recheck'


class TestRecheck(unittest.TestCase):
    def test_recheck(self):
        CELERY_BROKER_URL = env.get(
            'CELERY_BROKER_URL',
            'amqp://worker:duongtang2019@localhost/duongtang')
        publisher = RecheckPublisher(CELERY_BROKER_URL)
        publisher.publish(
            '{"driveid":"15IZwzTZpf38Tq6-vucxR2lYLBuwC0Yrd","email":"e1@gmail.com"}')


if __name__ == '__main__':
    unittest.main()
