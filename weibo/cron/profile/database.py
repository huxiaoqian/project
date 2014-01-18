# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

host = '192.168.2.11'
user = 'root'
database = 'weibo'
#engine = create_engine('mysql+mysqldb://%s:@%s/%s?charset=utf8' % (user, host, database), \
#                       pool_size=20, max_overflow=0)
engine = create_engine('mysql+mysqldb://cobar:@192.168.2.11:8066/cobar_db_weibo?charset=utf8', \
                       pool_size=20, max_overflow=0)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
print db_session
Base = declarative_base()
#Base.query = db_session.query_property()

def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    import models
    Base.metadata.create_all(bind=engine)

def remove_db_session(db_session):
    # remove database sessions
    db_session.remove()

def create_session(url, pool_size=20, max_overflow=0):
    engine = create_engine(url, pool_size=pool_size, max_overflow=max_overflow)
    SessionMaker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionMaker()#scoped_session(SessionMaker())
    session._model_changes = {}
    return session 

if __name__ == '__main__':
    init_db()
    
