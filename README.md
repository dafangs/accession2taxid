# accession2taxid: saving NCBI accession2taxid file to database for querying

## installation

```
pip install accession2taxid
```

## usage

### saving

```
accession2taxid -i prot.accession2taxid.FULL
```

### querying

```python
from accession2taxid import get_scoped_session, Accession2Taxid

with get_scoped_session() as session:
    session.query(Accession2Taxid).filter_by(
        accession='A0A0A0MTA4'
    )
```