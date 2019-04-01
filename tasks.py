from datetime import datetime
from worker import celery, DbTask
from db import Base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Text


class Config(Base):
    __tablename__ = 'configs'

    ACTIVE_STATUS = 1
    INACTIVE_STATUS = 0

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(255), nullable=False)
    value = Column(Text, nullable=False, default='')
    group = Column(String(128), nullable=False)
    expired_to = Column(BigInteger, nullable=False)
    status = Column(Integer, default=ACTIVE_STATUS)
    created_date = Column(
        DateTime, nullable=True, default=datetime.utcnow)
    updated_date = Column(DateTime, onupdate=datetime.utcnow)
    deleted_date = Column(DateTime, nullable=True, default=None)
    updated_timestamp = Column(BigInteger, nullable=True)


@celery.task(base=DbTask, name='duongtang.add', bind=True)
def add(self, x, y):
    # lets sleep for a while before doing the gigantic addition task!
    configs = self.session.query(Config).all()
    [print(config.__dict__) for config in configs]
