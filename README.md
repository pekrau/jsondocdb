# YasonDB

Yet another JSON document database, with indexes and transactions.
Built on Sqlite3 and JSONPath in Python.

```python
import yasondb

# To create a new file use 'create=True'.
# To use an existing file, 'create=False', which is the default

db = yasondb.Database("demo.db", create=True)

# Database modifications must be done within a transaction,
# which are created using a 'with' context manager.

with db:
     # Add a document with a specified key.
    db["id1"] = {"key": "k1", "n": 3}
    doc = {"key": "k2", "n": 5}
    # Add a document, letting YasonDB set a UUID4-based key, which is returned.
    autoid = db.add(doc)

if db[autoid] == doc:
    print("Fetched doc is equal to previously input.")

# Not allowed to add if the key is already in use.

with db:
    try:
        db.add({"key": "x"}, autoid)
    except KeyError as error:
        print(error)

# Search all documents for those matching a JSON path.
# Note that this does not test for the value of the JSON path,
# just that it exists in a document.
# The id, value and optionally doc are returned for each match.

found = db.search("$.key")
print(len(found), "documents in search for those with item 'key'.")

# A named index using JSONPath: documents giving one or more matches
# with the path will be present in the index.

with db:
    db.create_index("key_index", "$.key")
    db["in_index"] = {"key": "k3"}
    db["not_in_index"] = {"key2": "k4"}

# 'lookup' returns a list of id's for matching documents from the named index.
# Note that this operation does not require a transaction.

found = db.lookup("key_index", "k2")
if len(found) == 1 and db[found[0]] == doc:
    print("Found doc is equal to previously input.")

if not db.in_index("key_index", "k4"):
    print("Document having 'key2' but not 'key' item is not in this index.")

# 'range' returns a generator of identifiers of documents  matching
# the inclusive interval ["k1", "k2"].

ids = list(db.range("key_index", "k1", "k2"))
if len(ids) == 2:
    print("'range' return ids within low and high inclusive.")
```

## `class Database(dbfilepath, create=False)`

- `dbfilepath`: The filepath for the YasonDB database file.
  The special value `:memory:` indicates an in-memory database.
- `create`:
  - `False`: The database file must exist, and must be a YasonDB database.
  - `True`: Create and initialize the file. It must not exist.

Raises:
- `IOError`: The file exists when it shouldn't, and vice versa,
  depending on `create`.
- `ValueError`: Could not initialize the YasonDB database.
- `YasonDB.InvalidDatabaseError`: The database file is not a YasonDB file.

### `str(db)`

Return a string with info on number of documents and indexes.

### `iter(db)`

Return an iterator over ids for all documents in the database.

### `len(db)`

Return the number of documents in the database.

### `db[id]`

Return the document with the given id.

Raises:
- `KeyError`: No such document id in the database.

### `db[id] = doc`

Add or update the document with the given id in the database.

Raises:
- `YasonDb.NotInTransaction`

### `del db[id]`

Delete the document with the given id from the database.
No error if the document with the given key does not exist.

Raises:
- `YasonDb.NotInTransaction`

### `id in db`

Return `True` if the given id is in the database, else `False`.

### `with db: ...`

A context manager for a transaction. All operations that modify the data
must occur within a transaction.

If all goes well, the transaction is committed.
If an error occurs within the block, the transaction is rolled back.

Raises:
- `YasonDB.AlreadyInTransactionError`

### `db.in_transaction`

A property returning whether we are within a transaction.

### `db.begin()`

Start a transaction. Use the context manager instead.

Raises:
- `YasonDB.AlreadyInTransactionError`

### `db.commit()`

End the transaction, storing the modifications. Use the context
manager instead.

Raises:
- `YasonDb.NotInTransaction`

### `db.rollback()`

End the transaction, discaring the modifications. Use the context
manager instead.

Raises:
- `YasonDb.NotInTransaction`

### `db.get(id, default=None)`

Retrieve the document given its id, else the default.

### `db.add(doc, id=None)`

Add the document to the database. If `id` is not provided, create a UUID4 id.
Return the id actually used.

Raises:
- `ValueError`: If the document is not a dictionary.
- `KeyError`: If the id already exists in the database.
- `YasonDB.NotInTransaction`

### `db.update(id, doc, add=False)`

Update the document with the given id.

Raises:
- `ValueError`: If the document is not a dictionary.
- `KeyError`: If no such id in the database and 'add' is False.
- `YasonDB.NotInTransaction`

### `db.delete(id)`

Delete the document with the given id from the database.

Raises:
- `KeyError`: No such document id.
- `YasonDB.NotInTransaction`

### `db.search(jsonpath, include_docs=False)`

Search all documents and return those that match the given JSON path.
The result is a list of dict(key, id[, doc]).

Raises:
- ValueError: Invalid JSON path.

### `db.index_exists(indexname)

Does the named index exist?
        
### `db.create_index(indexname, jsonpath)`

Create an index for a given JSON path.

Raises:
- `ValueError`: The indexname is invalid or already in use, or the given
  JSON path is invalid.
- `YasonDB.NotInTransaction`

### `db.get_indexes()`

Return the list names for the current indexes.

### `db.get_index(indexname)`

Return definition and statistics for the named index.

Raises:
- `KeyError`: If there is no such index.

### `db.get_index_keys(indexname)`

Return a generator to provide all tuples `(id, key)` in the index.

Raises:
- `KeyError`: If there is no such index.

### `db.in_index(indexname, id)`

Is the given id in the named index?

### `db.delete_index(indexname)`

Delete the named index.

Raises:
- `KeyError`: If there is no such index.
- `YasonDB.NotInTransaction`

### `db.lookup(indexname, key)`

Return a list of all ids for the documents having
the given key in the named index.

Raises:
- `KeyError`: If there is no such index.

### `db.range(indexname, lowkey, highkey, limit=None, offset=None)`

Return a generator over all ids for the documents having 
a key in the named index within the given inclusive range.

- `limit`: Limit the number of ids returned.
- `offset`: Skip the first number of ids found.

Raises:
- `KeyError`: If there is no such index.

### `db.backup(dbfilepath)`

Backup this database safely into a new file at the given path.

Raises:
- `IOError`: If a file already exists at the new path.
- `YasonDB.InTransactionError`

### `db.close()`

Close the connection to the Sqlite3 database.

## `class BaseError(Exception)`

Base class for YasonDB-specific errors.

## `class InvalidDatabaseError(BaseError)`

The file is not a valid YasonDB database.

## `class AlreadyInTransactionError(BaseError)`

Attempt to begin a transaction when already within one.

## `class NotInTransactionError(BaseError)`

Attempted operation requires being in a transaction.


