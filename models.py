import os
import sys
from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, DateTime, Date, Text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError, IntegrityError
from scrapers import raw_config

Base = declarative_base()

SCHEMA = 'xkcd'
# Used when schema cannot be used
table_prefix = ''

if not raw_config.get('database', 'uri').startswith('postgres'):
    SCHEMA = None
    table_prefix = 'xkcd_'


class Comic(Base):
    __tablename__ = table_prefix + 'comic'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True)
    posted_at = Column(Date)
    time_collected = Column(DateTime)
    comic_id = Column(Integer)
    raw_json = Column(Text)
    title = Column(String(1024))
    alt = Column(Text)
    transcript = Column(Text)
    image_path = Column(String(512))

class Setting(Base):
    __tablename__ = table_prefix + 'setting'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True)
    comic_last_ran = Column(DateTime)
    comic_last_id = Column(Integer)
    what_if_ran = Column(DateTime)
    what_if_id = Column(Integer)
    bit = Column(Integer, unique=True)


engine = create_engine(raw_config.get('database', 'uri'))

if raw_config.get('database', 'uri').startswith('postgres'):
    try:
        engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError:
        # Schema already exists
        pass

Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)

db_session = DBSession()

try:
    new_setting = Setting()
    new_setting.bit=0
    db_session.add(new_setting)
    db_session.commit()
except IntegrityError:
    # Settings row has already been created
    db_session.rollback()






