# accession2taxid: saving NCBI accession2taxid file to database for querying

## installation

```
pip install accession2taxid
```

## usage

### SQLite

#### saving

```
accession2taxid sqlite -i prot.accession2taxid
```

#### querying

```python
from accession2taxid import get_sqlite_scoped_session, Accession2Taxid

with get_sqlite_scoped_session() as session:
    session.query(Accession2Taxid).filter_by(
        accession='A0A0A0MTA4'
    )
    session.query(Accession2Taxid).filter_by(
        accession_version='A0A0A0MTA4.2'
    )
    session.query(Accession2Taxid).filter_by(
        gi='1679377559'
    )
```

### Redis

#### saving

```
accession2taxid redis -i prot.accession2taxid
```

#### querying

```python
from accession2taxid import REDIS_KEY, get_redis_conn


with get_redis_conn() as conn:
    conn.hget(REDIS_KEY, 'xxx')
```