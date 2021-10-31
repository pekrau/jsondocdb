# jsondblite

JSON document database in a file, with indexes and transactions.
Built on Sqlite3 and JSONPath in Python.

```python
import jsondblite

db = jsondblite.Database("demo.db", create=True) # Explicitly create new db.

with db:                 # Database transaction; required for modifications.
    db["id1"] = {"key": "k1", "n": 3}   # Specify identifier explicitly.

    doc = {"key": "k2", "n": 5}  # Let the system set and return a UUID4 id.
    autoid = db.add(doc)

if db[autoid] == doc:
    print("Fetched doc is equal to previously input.")

try:
    with db:
        db.add({"key2": "x"}, autoid)  # Won't work; id already in use.
except KeyError as error:
    print(error)

with db:
    db.update(autoid, {"key2": "x"})  # Update existing entry.

found = db.search("$.key", "k1")  # Find all docs value at JSON path.
                                  # Tuples (id, doc) are returned.
print(len(found), "documents having the value 'k1' for item 'key'.")

with db:
    db.create_index("key_index", "$.key") # Named index using JSON path.
    doc = {"key": "k3"}
    db["in_index"] = doc
    db["not_in_index"] = {"key2": "k4"}

found = db.lookup("key_index", "k3")  # Return list of ids of matching docs.
if len(found) == 1 and db[found[0]] == doc:
    print("Found doc is equal to previously input.")

if not db.in_index("key_index", "k4"):
    print("Document is not in this index.")

ids = list(db.range("key_index", "k1", "k3"))  # Return generator of ids of docs
                                               # matching inclusive interval.
print(f"'range' returned {len(ids)} ids within low and high inclusive.")

# Measure CPU time to add 100000 documents.
import time
N = 100000
start = time.process_time()
# Perform as many operations as possible within a transaction; much faster.
with db:
    for n in range(100000):
        doc = {"key": n, "some_other_key": str(2*n)}
        db.add(doc)
delta = time.process_time() - start
print(f"Added {N:,} documents in {delta:.3g} seconds, {1000*delta/N:.3g} ms per document.")
# output> Added 100,000 documents in 3.01 seconds, 0.0301 ms per document.
```

## `class Database(dbfilepath, create=False, index_functions=None)`

- `dbfilepath`: The filepath for the jsondblite database file.
  The special value `:memory:` indicates an in-memory database.
- `create`:
  - `False`: The database file must exist, and must be a jsondblite database.
  - `True`: Create and initialize the file. It must not exist.

'index_functions': Dictionary with the index name as key and
a callable as value. Required if the database has indexes
created with callables. See `[create_function_index](#create_function_indexindexname-function)`.

Raises:
- `IOError`: The file exists when it shouldn't, and vice versa,
  depending on `create`.
- `ValueError`: Could not initialize the jsondblite database.
- `KeyError`: The callable is missing for a function index.
- `jsondblite.InvalidDatabaseError`: The file is not a jsondblite file.

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
- `jsondblite.NotInTransaction`

### `del db[id]`

Delete the document with the given id from the database.
No error if the document with the given key does not exist.

Raises:
- `jsondblite.NotInTransaction`

### `id in db`

Return `True` if the given id is in the database, else `False`.

### `with db: ...`

A context manager for a transaction. All operations that modify the data
must occur within a transaction.

If all goes well, the transaction is committed.
If an error occurs within the block, the transaction is rolled back.

Raises:
- `jsondblite.AlreadyInTransactionError`

### `db.in_transaction`

A property returning whether we are within a transaction.

### `db.begin()`

Start a transaction. Use the context manager instead.

Raises:
- `jsondblite.AlreadyInTransactionError`

### `db.commit()`

End the transaction, storing the modifications. Use the context
manager instead.

Raises:
- `jsondblite.NotInTransaction`

### `db.rollback()`

End the transaction, discaring the modifications. Use the context
manager instead.

Raises:
- `jsondblite.NotInTransaction`

### `db.get(id, default=None)`

Retrieve the document given its id, else the default.

### `db.add(doc, id=None)`

Add the document to the database. If `id` is not provided, create a UUID4 id.
Return the id actually used.

Raises:
- `ValueError`: If the document is not a dictionary.
- `KeyError`: If the id already exists in the database.
- `jsondblite.NotInTransaction`

### `db.update(id, doc, add=False)`

Update the document with the given id.

Raises:
- `ValueError`: If the document is not a dictionary.
- `KeyError`: If no such id in the database and 'add' is False.
- `jsondblite.NotInTransaction`

### `db.delete(id)`

Delete the document with the given id from the database.

Raises:
- `KeyError`: No such document id.
- `jsondblite.NotInTransaction`

### `db.have_jsonpath(jsonpath)`

Return a generator providing ids of all documents matching the given JSON path.

Raises:
- ValueError: Invalid JSON path.

### `db.lack_jsonpath(jsonpath)`

Return a generator providing ids of all documents not matching
the given JSON path.

Raises:
- ValueError: Invalid JSON path.

### `db.search(jsonpath, value)`

Return a list of tuple(id, doc) for all documents that have
the given value at the given JSON path.

Raises:
- ValueError: Invalid JSON path.

### `db.index_exists(indexname)

Does the named index exist?
        
### `db.create_index(indexname, jsonpath)`

Create an index for a given JSON path. The JSON path is applied to
each document 'dict' and must produce (possibly empty) list containing
'str' or 'int' values. Other value types are ignored.

Raises:
- `KeyError`: The indexname is invalid or already in use.
- `ValueError`: The JSON path is invalid.
- `jsondblite.NotInTransaction`

### `create_function_index(indexname, function)`

Create an index that uses the given callable 'function' to compute the
index table entries for a document. The callable takes a document
'dict' and must produce a (possibly empty) list containing 'str' or
'int' values.  Other value types in the list are ignored.

Since the callable is not stored in the database, it will have to be
provided each time the database is opened subsequently.

Raises:
- KeyError: The indexname is invalid or already in use.
- ValueError: 'function' is not a callable, or it did not return a list.
- jsondblite.NotInTransaction

### `db.get_indexes()`

Return the list names for the current indexes.

### `db.get_index(indexname)`

Return definition and statistics for the named index.

Raises:
- `KeyError`: If there is no such index.

### `db.get_index_values(indexname)`

Return a generator to provide all tuples `(id, value)` in the index.

Raises:
- `KeyError`: If there is no such index.

### `db.in_index(indexname, id)`

Is the given id in the named index?

### `db.delete_index(indexname)`

Delete the named index.

Raises:
- `KeyError`: If there is no such index.
- `jsondblite.NotInTransaction`

### `db.lookup(indexname, value)`

Return a list of all ids for the documents having
the given value in the named index.

Raises:
- `ValueError`: The value cannot be None, since not in the index.
- `KeyError`: If there is no such index.

### `db.range(indexname, low, high, limit=None, offset=None)`

Return a generator over all ids for the documents having 
a value in the named index within the given inclusive range.

- `limit`: Limit the number of ids returned.
- `offset`: Skip the first number of ids found.

Raises:
- `KeyError`: If there is no such index.

### `db.backup(dbfilepath)`

Backup this database safely into a new file at the given path.

Raises:
- `IOError`: If a file already exists at the new path.
- `jsondblite.InTransactionError`

### `db.close()`

Close the connection to the Sqlite3 database.

## `class BaseError(Exception)`

Base class for jsondblite-specific errors.

## `class InvalidDatabaseError(BaseError)`

The file is not a valid jsondblite database.

## `class InTransactionError(BaseError)`

Attempt to begin a transaction when already within one.

## `class NotInTransactionError(BaseError)`

Attempted operation requires being in a transaction.


