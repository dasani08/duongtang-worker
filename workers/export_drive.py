import logging
from os import environ as env, mkdir, path, remove
import json
import csv
import concurrent.futures
import boto3
import requests
from urllib.parse import urlencode
from sqlalchemy.exc import SQLAlchemyError
from core.pika import PikaConsumer, LOG_FORMAT
from core.db import get_engine_session
from models import Config, UserDrive, GDRIVE_API_KEY

AMQP_BROKER_URL = env.get(
    'AMQP_BROKER_URL', 'amqp://worker:duongtang2019@localhost/duongtang')
S3_BUCKET = 'duongtang'
AWS_ACCESS_KEY_ID = env.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = env.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = env.get('AWS_REGION')
API_URL = env.get('API_URL')
PAGE_SIZE = 1000
DRIVE_FILE_MIME_TYPES = {
    'g_file': 'application/vnd.google-apps.file',
    'g_folder': 'application/vnd.google-apps.folder'
}
NUM_EXTRACTING_FOLDER = 10
MAX_EXTRACTING_WORKER = 1

LOGGER = logging.getLogger(__name__)

client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


class DriveService:
    @classmethod
    def is_valid_drive_id(cls, drive_id):
        return drive_id and drive_id.strip() != ''

    @classmethod
    def is_drive_file_type(cls, mime):
        return mime == DRIVE_FILE_MIME_TYPES['g_file']

    @classmethod
    def is_drive_folder_type(cls, mime):
        return mime == DRIVE_FILE_MIME_TYPES['g_folder']

    @classmethod
    def get_files(cls, drive_id, api_key, next_page_token=None):
        query = {
            'orderBy': 'folder desc',  # trying to list all files on first
            'pageSize': PAGE_SIZE,
            'key': api_key,
            'q': '"%s" in parents' % (drive_id)
        }

        if next_page_token is not None:
            query['pageToken'] = next_page_token

        api_url = 'https://www.googleapis.com/drive/v3/files?%s' % urlencode(
            query)
        r = requests.get(api_url,
                         headers={"Accept": "application/json"})

        if r.status_code != 200:
            return [], [], None

        content = r.json()
        files = list(
            filter(lambda file: not cls.is_drive_folder_type(file['mimeType']),
                   content.get('files')))
        folders = list(
            filter(lambda file: cls.is_drive_folder_type(file['mimeType']),
                   content.get('files')))
        next_page_token = content.get('nextPageToken', None)
        return files, folders, next_page_token


class DriveExtractorConsumer(PikaConsumer):
    QUEUE = 'export_drive'
    ROUTING_KEY = 'default.export_drive'
    TMP_DIR = './tmp'

    _session = None
    _upload_publisher = None

    def __init__(self, url):
        super().__init__(url)
        if not path.exists(self.TMP_DIR):
            try:
                mkdir(self.TMP_DIR)
            except Exception:
                pass

    def on_connection_closed(self, *args, **kwargs):
        super().on_connection_closed(*args, **kwargs)
        if self._session is not None:
            self._session.close()

    @property
    def db_session(self):
        if self._session is None:
            self._session = get_engine_session()
        return self._session

    def upload_to_s3(self, filename):
        with open("{}/{}".format(self.TMP_DIR, filename), "rb") as f:
            client.upload_fileobj(f, S3_BUCKET, filename)
            LOGGER.info('Upload file {} to s3 completed'.format(filename))

    def remove_tmp_file(self, filename):
        filepath = '{}/{}'.format(self.TMP_DIR, filename)
        if path.exists(filepath):
            remove(filepath)
            LOGGER.info('Delete tmp file {} completed'.format(filename))

    def extract_drive(self, drive_id):
        LOGGER.info('extracting drive {}'.format(drive_id))
        folder_ids = [{
            'drive_id': drive_id,
            'next_page_token': None
        }]
        api_key = Config.get_one_by_key(self.db_session, GDRIVE_API_KEY)
        while len(folder_ids) > 0:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=MAX_EXTRACTING_WORKER) as worker:
                futures = {
                    worker.submit(
                        DriveService.get_files,
                        item['drive_id'],
                        api_key['value'],
                        item['next_page_token']): item for item in
                    folder_ids[:NUM_EXTRACTING_FOLDER]}
                for future in concurrent.futures.as_completed(futures):
                    item = futures[future]
                    (files, folders, page_token) = future.result()
                    if len(files) > 0:
                        yield [file['id'] for file in files]
                    if page_token:
                        item['next_page_token'] = page_token
                    else:
                        folder_ids.remove(item)

                    for folder in folders:
                        folder_ids.append({
                            'drive_id': folder['id'],
                            'next_page_token': None
                        })
        LOGGER.info('extracting completed')

    def prepare_upload(self, filename, api_key):
        try:
            with open("{}/{}".format(self.TMP_DIR, filename)) as f:
                reader = csv.reader(f)
                for row in reader:
                    drive_id = row[0]
                    url = "{}/api/get?url={}&api_key={}".format(
                        API_URL, drive_id, api_key)
                    LOGGER.info('prepare upload for {}'.format(drive_id))
                    requests.get(url)
        except Exception as exc:
            print(exc)

    def update_db(self, req_id, **data):
        try:
            self.db_session.query(UserDrive).filter_by(id=req_id).update(data)
            self.db_session.commit()
        except SQLAlchemyError as exc:
            self.db_session.rollback()
            raise exc

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        body = json.loads(body)
        drive_id = body['drive_id']
        req_id = body['id']
        api_key = body['api_key']
        upload_flag = body['upload_flag']
        LOGGER.info('received a message req_id={}\
            drive_id={}'.format(req_id, drive_id))
        try:
            if drive_id is None:
                raise Exception('drive id is missing')
            if req_id is None:
                raise Exception('req_id is missing')
            filename = '{}_{}.csv'.format(drive_id, req_id)
            total_file = 0
            self.update_db(req_id, status=UserDrive.EXTRACTING_STATUS)
            with open('{}/{}'.format(self.TMP_DIR, filename), mode='w+') as f:
                for drives in self.extract_drive(drive_id):
                    total_file += len(drives)
                    f.write('\n'.join(drives) + '\n')
            self.update_db(req_id, status=UserDrive.FINISHED_STATUS,
                           total_file=total_file)
            LOGGER.info('total drive id found: {}'.format(total_file))
            self.upload_to_s3(filename)
            if upload_flag:
                self.prepare_upload(filename, api_key)
            self.remove_tmp_file(filename)
            self.acknowledge_message(basic_deliver.delivery_tag)
        except Exception as exc:
            LOGGER.info('extracting error: {}'.format(exc))
            self.update_db(req_id, status=UserDrive.ERROR_STATUS)


def main():
    logging.basicConfig(level=env.get(
        'LOG_LEVEL', logging.INFO), format=LOG_FORMAT)
    logging.getLogger('pika').setLevel(logging.WARNING)
    consumer = DriveExtractorConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == '__main__':
    main()
