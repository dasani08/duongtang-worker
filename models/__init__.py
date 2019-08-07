from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Text, \
    Index
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


class Stream(BaseModelMixin):
    __tablename__ = 'streams'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(256), nullable=False)
    user_id = Column(Integer, nullable=False)
    source_type = Column(String(128), nullable=False)
    type = Column(String(128), nullable=True, default="photo")
    result = Column(Text, nullable=True)
    email = Column(String(128), nullable=True)
    expired = Column(Integer, nullable=True)
    duration = Column(Integer, nullable=True)
    title = Column(String(255), nullable=True)
    status_code = Column(Integer, nullable=True, default=200)
    updated_timestamp = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index('idx_source_id', source_id),
    )


class UserDrive(Base):
    __tablename__ = 'user_drives'

    PENDING_STATUS = 'pending'
    EXTRACTING_STATUS = 'extracting'
    FINISHED_STATUS = 'finished'
    ERROR_STATUS = 'error'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    drive_id = Column(String(255), nullable=False)
    total_file = Column(Integer, nullable=True, default=0)
    status = Column(String(128), nullable=True, default=PENDING_STATUS)
    api_key = Column(String(128), nullable=True)
    created_date = Column(DateTime, nullable=True,
                          default=datetime.utcnow)
    updated_date = Column(DateTime, onupdate=datetime.utcnow)


class UserApp(Base):
    __tablename__ = 'user_apps'

    ACTIVE_STATUS = 1
    INACTIVE_STATUS = 0

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    api_key = Column(String(128), nullable=False)
    stream_type = Column(
        String(128), nullable=False, default='photo, proxy')
    status = Column(Integer, default=0)
    created_date = Column(
        DateTime, nullable=True, default=datetime.utcnow)
    domain = Column(String(256), nullable=True)
    short_domain = Column(String(256), nullable=True,
                          default='https://go.clgt.vn')


class BalanceLog(Base):
    __tablename__ = 'balance_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    transaction_timestamp = Column(BigInteger, nullable=False)
    balance = Column(Integer, nullable=True, default=0)
    transaction_type = Column(String(32), nullable=True, default='VIEW')
    source_id = Column(String(255), nullable=True)
