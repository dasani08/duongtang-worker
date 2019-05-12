from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Text
from sqlalchemy.orm import load_only
from sqlalchemy.exc import SQLAlchemyError

from core.db import Base
from core.utils import get_unix_time


GDRIVE_API_KEY = 'GDRIVE_API_KEY'
GDRIVE_COOKIE_KEY = 'GMAIL_COOKIE'


class BaseModelMixin(object):
    created_date = Column(
        DateTime, nullable=True, default=datetime.now())
    updated_date = Column(DateTime, onupdate=datetime.utcnow)
    deleted_date = Column(DateTime, nullable=True, default=None)

    def update(self, b):
        for key, value in b.items():
            setattr(self, key, value)


class Config(Base, BaseModelMixin):
    __tablename__ = 'configs'

    ACTIVE_STATUS = 1
    INACTIVE_STATUS = 0

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(255), nullable=False)
    value = Column(Text, nullable=False, default='')
    group = Column(String(128), nullable=False)
    expired_to = Column(Integer, nullable=False)
    status = Column(Integer, default=ACTIVE_STATUS)
    updated_timestamp = Column(BigInteger, nullable=True)

    @classmethod
    def get_one_by_key(cls, session, key):
        now = int(datetime.timestamp(datetime.now()))
        config = session.query(cls).filter_by(
            key=key,
            status=cls.ACTIVE_STATUS).filter(
            (cls.expired_to == None) | (
                cls.expired_to < now)
        ).order_by(
            cls.updated_timestamp.asc()
        ).options(load_only('id', 'value', 'group')).first()

        if config is not None:
            # Update config timestamp
            config.updated_timestamp = get_unix_time()
            config_dict = {'group': config.group, 'value': config.value}
            session.add(config)
            # session.query(Config).filter_by(id=config.id).update(
            #     {'updated_timestamp': get_unix_time()})
            session.commit()
            """
            Should return new dictionay of config object instead of itself
            Accessing any props of config object will cause new query to db to
            ensure that new changes has updated to database
            """
            return config_dict
        return None

    @classmethod
    def disable_cookie(cls, session, email):
        # Gets cookie
        session.query(cls).filter_by(
            key=GDRIVE_COOKIE_KEY, group=email).update({
                'status': Config.INACTIVE_STATUS
            })
        session.commit()

    @classmethod
    def limit_cookie(cls, session, email):
        next_day = int(datetime.timestamp(
            datetime.now() + timedelta(days=1)))
        try:
            session.query(cls).filter_by(
                key=GDRIVE_COOKIE_KEY, group=email).update({
                    'expired_to': next_day
                })
            session.commit()
        except SQLAlchemyError:
            session.rollback()


class Source(Base, BaseModelMixin):
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(128), nullable=False, default='drive')
    source_id = Column(String(128), nullable=False)
    user_id = Column(Integer, nullable=True)
    source_link = Column(Text, nullable=True)
    cookie = Column(String(255), nullable=True)
    expire = Column(Integer, nullable=True)
    duration = Column(Integer, nullable=True)
    title = Column(String(255), nullable=True)
    clone = Column(Integer, default=3)


class Stream(Base):
    __tablename__ = 'streams'

    STATUS_CODE_ACTIVE = 200
    STATUS_CODE_DIE = 404
    STATUS_CODE_NOT_PERM = 401

    id = Column(Integer, primary_key=True, autoincrement=True)
    drive_id = Column(String(128), nullable=False)
    gphoto_url = Column(Text, nullable=False)
    email = Column(String(128), nullable=False)
    expired = Column(Integer, nullable=True)
    duration = Column(Integer, nullable=True)
    title = Column(String(255), nullable=True)
    status_code = Column(Integer, nullable=True)


class UploadQueueLog(Base):
    __tablename__ = 'upload_queue_logs'

    STATUS_READY = 'ready'
    STATUS_COMPLETE = 'complete'
    STATUS_CANCEL = 'cancel'

    # uuid
    message_id = Column(String(36), primary_key=True)
    drive_id = Column(String(128), nullable=True)
    email = Column(String(128), nullable=True, index=True)
    status = Column(String(16), default=STATUS_READY)
    created_date = Column(
        DateTime, nullable=True, default=datetime.utcnow)
