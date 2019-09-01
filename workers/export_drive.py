import logging
from os import environ as env, mkdir, path, remove
import json
import csv
import concurrent.futures
import boto3
import requests
from urllib.parse import urlencode
from sqlalchemy.exc import SQLAlchemyError
from core.pika import PikaConsumer
from core.db import get_engine_session
from core.utils import get_unix_time
from models import Config, UserDrive, BalanceLog, GDRIVE_API_KEY


LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)s] [%(module)s] %(funcName)s: %(message)s"
)
AMQP_BROKER_URL = env.get(
    "AMQP_BROKER_URL", "amqp://worker:duongtang2019@localhost/duongtang"
)
S3_BUCKET = "duongtang"
AWS_ACCESS_KEY_ID = env.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = env.get("AWS_REGION")
API_URL = env.get("API_URL", "http://localhost:5000")
PAGE_SIZE = 1000
DRIVE_FILE_MIME_TYPES = {
    "g_file": "application/vnd.google-apps.file",
    "g_folder": "application/vnd.google-apps.folder",
}
SUPPORTED_VIDEO_MIME_TYPES = [
    # WebM files (Vp8 video codec; Vorbis Audio codec)
    "video/webm",
    # MPEG4, 3GPP, and MOV files(h264 and MPEG4 video codecs AAC audio codec)
    "video/mp4",
    "video/3gpp",
    "video/quicktime",
    "application/x-mpegURL",
    # AVI (MJPEG video codec; PCM audio)
    "video/x-msvideo",
    # WMV
    "video/x-ms-wmv",
    # FLV (Adobe - FLV1 video codec, MP3 audio)
    "video/x-flv",
    # MTS
    "application/metastream",
    "video/avchd-stream",
    "video/mts",
    "video/vnd.dlna.mpeg-tts",
    # OGG
    "video/ogg",
    "application/ogg",
]
CONCURRENT_EXTRACTING_FOLDER = 10
MAX_EXTRACTING_WORKER = 1

logger = logging.getLogger(__name__)
client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


class WorkerException(Exception):
    pass


class DriveService:
    @classmethod
    def is_valid_drive_id(cls, drive_id):
        return drive_id and drive_id.strip() != ""

    @classmethod
    def is_drive_file_type(cls, mime):
        return mime == DRIVE_FILE_MIME_TYPES["g_file"]

    @classmethod
    def is_drive_folder_type(cls, mime):
        return mime == DRIVE_FILE_MIME_TYPES["g_folder"]

    @classmethod
    def get_files(cls, drive_id, api_key, next_page_token=None):
        supported_mime_types = SUPPORTED_VIDEO_MIME_TYPES + [
            DRIVE_FILE_MIME_TYPES["g_folder"]
        ]
        mime_types_query = [
            'mimeType = "%s"' % mime for mime in supported_mime_types
        ]
        q = '"%s" in parents and (%s)' % (
            drive_id,
            " or ".join(mime_types_query),
        )
        query = {
            "orderBy": "folder desc",  # trying to list all files on first
            "pageSize": PAGE_SIZE,
            "key": api_key,
            "q": q,
        }

        if next_page_token is not None:
            query["pageToken"] = next_page_token

        api_url = "https://www.googleapis.com/drive/v3/files?%s" % urlencode(
            query
        )
        r = requests.get(api_url, headers={"Accept": "application/json"})

        if r.status_code != 200:
            logger.info("can not get files")
            return [], [], None

        content = r.json()
        files = list(
            filter(
                lambda file: not cls.is_drive_folder_type(file["mimeType"]),
                content.get("files"),
            )
        )
        folders = list(
            filter(
                lambda file: cls.is_drive_folder_type(file["mimeType"]),
                content.get("files"),
            )
        )
        next_page_token = content.get("nextPageToken", None)
        return files, folders, next_page_token


class DriveExtractorConsumer(PikaConsumer):
    QUEUE = "export_drive"
    ROUTING_KEY = "default.export_drive"
    TMP_DIR = "./tmp"

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
            logger.info("Upload file {} to s3 completed".format(filename))

    def remove_tmp_file(self, filename):
        filepath = "{}/{}".format(self.TMP_DIR, filename)
        if path.exists(filepath):
            remove(filepath)
            logger.info("Delete tmp file {} completed".format(filename))

    def get_drive_url(self, drive_id):
        return "https://drive.google.com/open?id={}".format(drive_id)

    def extract_drive(self, drive_id):
        logger.info("extracting drive {}".format(drive_id))
        folder_ids = [{"drive_id": drive_id, "next_page_token": None}]
        api_key = Config.get_one_by_key(self.db_session, GDRIVE_API_KEY)
        while len(folder_ids) > 0:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_EXTRACTING_WORKER
            ) as worker:
                futures = {
                    worker.submit(
                        DriveService.get_files,
                        item["drive_id"],
                        api_key["value"],
                        item["next_page_token"],
                    ): item
                    for item in folder_ids[:CONCURRENT_EXTRACTING_FOLDER]
                }
                for future in concurrent.futures.as_completed(futures):
                    item = futures[future]
                    (files, folders, page_token) = future.result()
                    if len(files) > 0:
                        yield [self.get_drive_url(file["id"]) for file in files]
                    if page_token:
                        item["next_page_token"] = page_token
                    else:
                        folder_ids.remove(item)

                    for folder in folders:
                        folder_ids.append(
                            {"drive_id": folder["id"], "next_page_token": None}
                        )
        logger.info("extracting completed")

    def prepare_upload(self, filename, api_key):
        try:
            with open("{}/{}".format(self.TMP_DIR, filename)) as f:
                reader = csv.reader(f)
                for row in reader:
                    drive_id = row[0]
                    url = "{}/api/get?url={}&api_key={}".format(
                        API_URL, drive_id, api_key
                    )
                    logger.info("prepare upload for {}".format(drive_id))
                    requests.get(url)
        except requests.exceptions.RequestException as exc:
            logger.error("prepare upload with errors: {}".format(exc))

    def get_user_drive(self, req_id):
        return self.db_session.query(UserDrive).filter_by(id=req_id).first()

    def update_db(self, req_id, **data):
        try:
            self.db_session.query(UserDrive).filter_by(id=req_id).update(data)
            self.db_session.commit()
        except SQLAlchemyError as exc:
            self.db_session.rollback()
            print(exc)
            logger.error("update db with errors: {}".format(exc))

    def add_balance_log(self, data):
        try:
            self.db_session.add(BalanceLog(**data))
            self.db_session.commit()
        except SQLAlchemyError as exc:
            self.db_session.rollback()
            logger.error("add balance error {}".format(exc))

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        body = json.loads(body)
        drive_id = body["drive_id"]
        req_id = body["id"]
        api_key = body["api_key"]
        upload_flag = False
        logger.info(
            "received a message req_id={}\
            drive_id={}".format(
                req_id, drive_id
            )
        )
        try:
            if drive_id is None:
                raise WorkerException("drive id is missing")
            if req_id is None:
                raise WorkerException("req_id is missing")

            user_drive = self.get_user_drive(req_id)
            if user_drive is None:
                raise WorkerException("user_drive not found")

            filename = "{}_{}.csv".format(drive_id, req_id)
            total_file = 0
            self.update_db(req_id, status=UserDrive.EXTRACTING_STATUS)
            with open("{}/{}".format(self.TMP_DIR, filename), mode="w+") as f:
                for drives in self.extract_drive(drive_id):
                    total_file += len(drives)
                    f.write("\n".join(drives) + "\n")
            self.update_db(
                req_id, status=UserDrive.FINISHED_STATUS, total_file=total_file
            )
            logger.info("total drive id found: {}".format(total_file))
            if total_file > 0:
                self.upload_to_s3(filename)
                if upload_flag:
                    self.prepare_upload(filename, api_key)
                else:
                    self.add_balance_log(
                        {
                            "user_id": user_drive.user_id,
                            "balance": (total_file * -1),
                            "transaction_timestamp": get_unix_time(),
                            "transaction_type": "EXPORT_DRIVE",
                            "source_id": req_id,
                        }
                    )
                self.remove_tmp_file(filename)
        except (Exception, WorkerException) as exc:
            logger.error("export has error: {}".format(exc))
            self.update_db(req_id, status=UserDrive.ERROR_STATUS)
        finally:
            self.acknowledge_message(basic_deliver.delivery_tag)


def main():
    logging.basicConfig(
        level=env.get("LOG_LEVEL", logging.INFO), format=LOG_FORMAT
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    consumer = DriveExtractorConsumer(AMQP_BROKER_URL)
    consumer.run()


if __name__ == "__main__":
    main()
