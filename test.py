import unittest
from worker import celery


class TestConfig(unittest.TestCase):
    def test_get_single_config(self):
        celery.send_task('duongtang.add', args=[1, 2], kwargs={})


if __name__ == '__main__':
    unittest.main()
