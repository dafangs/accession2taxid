# -*- coding: utf-8 -*-
from os import makedirs
from contextlib import contextmanager
from os.path import expanduser, exists, join

import click
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, create_engine


DBFILE = 'accession2taxid.db'
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


def get_engine():
    return create_engine(
        'sqlite:///%s' % get_dbfile()
    )


def get_session():
    return sessionmaker(bind=get_engine())()


@contextmanager
def get_scoped_session():
    session = get_session()
    try:
        yield session
    finally:
        session.close()


Base = declarative_base()


class Accession2Taxid(Base):

    __tablename__ = 'accession2taxid'
    id = Column(Integer, primary_key=True)
    accession = Column(String(64), unique=True)
    taxid = Column(String(64), index=True)

    def __repr__(self):
        return f'<Accession2TaxId({self.accession})>'


@click.command()
@click.option(
    '-i', '--infile',
    type=click.Path(exists=True),
    help='the accession2taxid file downloaded from NCBI FTP site.'
)
def accession2taxid(infile):
    """Saving to SQLite"""
    Base.metadata.create_all(get_engine())
    with open(infile) as fp, get_scoped_session() as session:
        fp.readline()  # omit header

        id = 0
        for line in fp:
            id += 1
            accession, taxid = line.rstrip().split('\t')
            session.add(Accession2Taxid(
                id=id,
                accession=accession,
                taxid=taxid,
            ))
            if id % 100000 == 0:
                session.commit()
        session.commit()
        print(f'{id} accession2taxid saved.')


if __name__ == '__main__':
    accession2taxid()
