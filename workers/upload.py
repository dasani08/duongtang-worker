from core.pika import PikaPublisher


class UploadPublisher(PikaPublisher):
    QUEUE = 'upload'
    EXCHANGE = 'default'
    ROUTING_KEY = 'default'
