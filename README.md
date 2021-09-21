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

