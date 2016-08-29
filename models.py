import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scrapers import raw_config

Base = declarative_base()

SCHEMA = 'xkcd'
# Used when schema cannot be used
table_prefix  = ''

if not raw_config.get('database', 'uri').startswith('postgres'):
    SCHEMA = None
    table_prefix = 'xkcd.'

class Comics(Base):
    __tablename__ = table_prefix+'comic_test'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)

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
