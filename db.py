import os
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

env = os.environ
SQLALCHEMY_DATABASE_URI = env.get(
    'SQLALCHEMY_DATABASE_URI',
    'mysql+pymysql://root:duongtang2019@127.0.0.1/duongtang?charset=utf8')


def get_engine_session():
    Session = sessionmaker()
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Session.configure(bind=engine)
    return scoped_session(Session)


Base = declarative_base()
