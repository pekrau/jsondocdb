"Demo code for the README."

import jsondblite

# To create a new file use 'create=True'.
# To use an existing file, 'create=False', which is the default.

db = jsondblite.Database("demo.db", create=True)

# Database modifications must be done within a transaction,
# which are created using a 'with' context manager.

with db:
     # Add a document with a specified key.
    db["id1"] = {"key": "k1", "n": 3}

    # Add a document, letting YasonDB set a UUID4-based key, which is returned.
    doc = {"key": "k2", "n": 5}
    autoid = db.add(doc)

if db[autoid] == doc:
    print("Fetched doc is equal to previously input.")

# Not allowed to add if the key is already in use.

with db:
    try:
        db.add({"key": "x"}, autoid)
    except KeyError as error:
        print(error)

# Find all documents having a given value at the given JSON path.
# Tuples (id, doc) are returned.
# No transaction is required since nothing is modified.

found = db.search("$.key", "k1")
print(len(found), "documents having the value 'k1' for item 'key'.")

# Create a named index using JSONPath: documents giving one or more
# matches with the path will be present in the index.

with db:
    db.create_index("key_index", "$.key")
    db["in_index"] = {"key": "k3"}
    db["not_in_index"] = {"key2": "k4"}

# 'lookup' returns a list of ids for matching documents from the named index.
# No transaction is required since nothing is modified.

found = db.lookup("key_index", "k2")
if len(found) == 1 and db[found[0]] == doc:
    print("Found doc is equal to previously input.")

if not db.in_index("key_index", "k4"):
    print("Document having 'key2' but not 'key' item is not in this index.")

# 'range' returns a generator of identifiers of documents  matching
# the inclusive interval ["k1", "k2"].

ids = list(db.range("key_index", "k1", "k2"))
print(f"'range' return {len(ids)} ids within low and high inclusive.")
