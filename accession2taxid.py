# -*- coding: utf-8 -*-
from os import makedirs
from contextlib import contextmanager
from os.path import expanduser, exists, join

import redis
import click
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, create_engine


DBFILE = 'accession2taxid.db'
REDIS_KEY = 'accession2taxid'
ACCESSION2TAXID_DIR = '~/.accession2taxid'


class Accession2TaxidError(Exception):
    pass


def get_accession2taxid_dir():
    accession2taxid = expanduser(ACCESSION2TAXID_DIR)
    if not exists(accession2taxid):
        makedirs(accession2taxid)
    return accession2taxid


def get_dbfile():
    return join(get_accession2taxid_dir(), DBFILE)


def get_sqlite_engine():
    return create_engine(
        'sqlite:///%s' % get_dbfile()
    )


def get_sqlite_session():
    return sessionmaker(bind=get_sqlite_engine())()


@contextmanager
def get_sqlite_scoped_session():
    session = get_sqlite_session()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def get_redis_conn():
    conn = redis.StrictRedis.from_url(
        'redis://localhost:6379',
        decode_responses=True
    )
    try:
        conn.ping()
    except redis.ConnectionError:
        raise Accession2TaxidError('Please start Redis server first.')

    try:
        yield conn
    finally:
        conn.close()


Base = declarative_base()


class Accession2Taxid(Base):

    __tablename__ = 'accession2taxid'
    id = Column(Integer, primary_key=True)
    accession = Column(String(64), unique=True)
    accession_version = Column(String(64), unique=True)
    taxid = Column(String(64), index=True)
    gi = Column(String(64), unique=True)

    def __repr__(self):
        return f'<Accession2TaxId({self.accession})>'


@click.group()
def accession2taxid():
    """Saving accession2taxid file to SQLite or Redis for querying"""


@accession2taxid.command('sqlite')
@click.option(
    '-i', '--infile',
    type=click.Path(exists=True),
    help='the accession2taxid file downloaded from NCBI FTP site.'
)
def to_sqlite(infile):
    """Saving to SQLite"""
    Base.metadata.create_all(get_sqlite_engine())
    with open(infile) as fp, get_sqlite_scoped_session() as session:
        fp.readline()  # omit header

        id = 0
        for line in fp:
            id += 1
            accession, accession_version, taxid, gi = line.rstrip().split('\t')
            session.add(Accession2Taxid(
                id=id,
                accession=accession,
                accession_version=accession_version,
                taxid=taxid,
                gi=gi
            ))
            if id % 100000 == 0:
                session.commit()
        session.commit()
        print(f'{id} accession2taxid saved.')


@accession2taxid.command('redis')
@click.option(
    '-i', '--infile',
    type=click.Path(exists=True),
    help='the accession2taxid file downloaded from NCBI FTP site.'
)
def to_redis(infile):
    """Saving to Redis"""
    with open(infile) as fp, get_redis_conn() as conn:
        fp.readline()  # omit header

        id = 0
        for line in fp:
            id += 1
            accession, accession_version, taxid, gi = line.rstrip().split('\t')
            conn.hset(REDIS_KEY, accession, taxid)
        print(f'{id} accession2taxid saved.')


if __name__ == '__main__':
    accession2taxid()
