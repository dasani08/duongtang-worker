from celery.bootsteps import ConsumerStep
from kombu import Exchange, Queue, Consumer
from core.worker import celery


class DefaultComsumer(ConsumerStep):
    _queue = Queue('default', Exchange('default'),
                   routing_key='duongtang.default')

    def get_consumers(self, channel):
        return [Consumer(channel,
                         queues=[self._queue],
                         callbacks=[self.handle_message],
                         accept=['json'])]

    def handle_message(self, body, message):
        print('Received message: {0!r}'.format(body))
        message.ack()


celery.steps['consumer'].add(DefaultComsumer)
