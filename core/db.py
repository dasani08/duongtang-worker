import os
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

env = os.environ
SQLALCHEMY_DATABASE_URI = env.get(
    'SQLALCHEMY_DATABASE_URI',
    'mysql+pymysql://root:duongtang2019@127.0.0.1/duongtang?charset=utf8')
SQLALCHEMY_POOL_RECYCLE = env.get('SQLALCHEMY_POOL_RECYCLE', 500)


# Factory method returning a db session scoped
def get_engine_session():
    Session = sessionmaker()
    engine = create_engine(SQLALCHEMY_DATABASE_URI,
                           pool_recycle=SQLALCHEMY_POOL_RECYCLE)
    Session.configure(bind=engine)
    return scoped_session(Session)


Base = declarative_base()
