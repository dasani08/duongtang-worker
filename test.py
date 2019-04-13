import unittest
from core.pika import PikaPublisher
from os import environ as env
from workers.upload import UploadPublisher


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


class TestUpload(unittest.TestCase):
    def test_upload(self):
        AMQP_BROKER_URL = 'amqp://huctqdff:U2MPPud4_CfhAYBA1jr4AvOW14VRzslJ@dinosaur.rmq.cloudamqp.com/huctqdff'
        uploader = UploadPublisher(AMQP_BROKER_URL)
        uploader.publish(
            '{"driveid":"15IZwzTZpf38Tq6-vucxR2lYLBuwC0Yrd","email":"email_1@abc.com"}')


if __name__ == '__main__':
    unittest.main()
