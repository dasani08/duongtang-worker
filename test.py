import unittest
from kombu import Exchange, Queue
from core.worker import celery

queue = Queue('source', Exchange('source'),
              routing_key='duongtang.source')


class TestConfig(unittest.TestCase):
    def test_get_single_config(self):
        celery.send_task(
            'duongtang.default',
            args=[1, 2], kwargs={}, queue='default')

    def test_get_source(self):

        drive_ids = [
            '1C8xXLZzcbcZ-7_KuwhLFAUwk3xb7avXu',
            '1UA8xm1og6IyCdbMFQ9dTOMJ8KlGqGYB6',
            '1-09_UabdpwDbU4UyBrD-w_E3NEYJCNgS',
            '1MsbTaxlRQE-5in50p-IEkobFQC2iqQMp',
            '1F_7TG-XDuQg7z_g_bz_AVaORResm9yPX'
        ]

        for drive_id in drive_ids:
            celery.send_task(
                'duongtang.get_source',
                kwargs={
                    'drive_id': drive_id, 'user_id': 1},
                queue='source', exchange='source', routing_key='source')


if __name__ == '__main__':
    unittest.main()
