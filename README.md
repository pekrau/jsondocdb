# YasonDB

Yet another JSON document database, with indexes and transactions.
Built on Sqlite3 in Python.

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

# A named index using JSONPath: documents giving one or more matches
# with the path will be present in the index.

with db:
    db.create_index("key_index", "$.key")
    db["in_index"] = {"key": "k3"}
    db["not_in_index"] = {"key2": "k4"}

# 'find' returns a list of id's for matching documents from the named index.
# Note that this operation does not require a transaction.

found = db.find("key_index", "k2")
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

- **dbfilepath**: The filepath for the YasonDB database file.
  The special value `:memory:` indicates an in-memory database.
- **create**:
  - `False`: The database file must exist, and must be a YasonDB database.
  - `True`: Create and initialize the file. It must not exist.

Raises:
- **IOError**: The file exists when it shouldn't, and vice versa,
  depending on `create`.
- **ValueError**: Could not initialize the YasonDB database.
- **YasonDB.InvalidDatabaseError**: The database file is not a YasonDB file.

### `str(db)`

Return a string with info on number of documents and indexes.

### `iter(db)`

Return an iterator over id's for all documents in the database.

### `len(db)`

Return the number of documents in the database.

### `db[id]`

Return the document with the given id.

Raises:
- **KeyError**: No such document id in the database.

### `db[id] = doc`

Add or update the document with the given id in the database.

Raises:
- **YasonDb.NotInTransaction**

### `del db[id]`

Delete the document with the given id from the database.
No error if the document with the given key does not exist.

Raises:
- **YasonDb.NotInTransaction**

### `id in db`

Return `True` if the given id is in the database, else `False`.

### `with db: ...`

A context manager for a transaction. All operations that modify the data
must occur within a transaction.

If all goes well, the transaction is committed.
If an error occurs within the block, the transaction is rolled back.

Raises:
- **YasonDB.AlreadyInTransactionError**

## `db.in_transaction`

A property returning whether we are within a transaction.

## `db.begin()`

Start a transaction. Use the context manager instead.

Raises:
- **YasonDB.AlreadyInTransactionError**

## `db.commit()`

End the transaction, storing the modifications. Use the context
manager instead.

Raises:
- **YasonDb.NotInTransaction**

## `db.rollback()`

End the transaction, discaring the modifications. Use the context
manager instead.

Raises:
- **YasonDb.NotInTransaction**

## `db.get(id, default=None)`

Retrieve the document given its id, else the default.

## `db.add(doc, id=None)`

Add the document to the database. If 'id' is not provided, create a UUID4 id.
Return the id.

Raises:
- **ValueError**: If the document is not a dictionary.
- **KeyError**: If the id already exists in the database.
- **YasonDB.NotInTransaction**

## `db.update(id, doc, add=False)`

## `db.delete(id)`

## `db.create_index(indexname, jsonpath)`

## `db.index_exists(indexname)

## `db.create_index(indexname, jsonpath)`

## `db.get_indexes()`

## `db.get_index(indexname)`

## `db.get_index_keys(indexname)`

## `db.in_index(indexname, id)`

## `db.delete_index(indexname)`

## `db.find(indexname, key, limit=None, offset=None)`

## `db.range(indexname, lowkey, highkey, limit=None, offset=None)`

## `db.backup(dbfilepath)`

## `db.close()`

