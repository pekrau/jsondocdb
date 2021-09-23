"Demo code for the README."

try:
    import os
    os.remove("demo.db")
except OSError:
    pass

import jsondblite

# To create a new file specify 'create=True' explicitly.
db = jsondblite.Database("demo.db", create=True)

# Database modifications must be done within a transaction,
# which are created using a 'with' context manager.
with db:
     # Add a document with a specified identifier.
    db["id1"] = {"key": "k1", "n": 3}

    # Add a document, letting the system set a UUID4-based identifier,
    # which is returned.
    doc = {"key": "k2", "n": 5}
    autoid = db.add(doc)

if db[autoid] == doc:
    print("Fetched doc is equal to previously input.")
# output> Fetched doc is equal to previously input.

# Not allowed to add if the identifier is already in use.
try:
    with db:
        db.add({"key2": "x"}, autoid)
except KeyError as error:
    print(error)
# output> "The id 'a06bd6326dbb4a80af301e628e781207' already exists."

# But update is allowed.
with db:
    db.update(autoid, {"key2": "x"})

# Find all documents having a given value at the given JSON path.
# Tuples (id, doc) are returned.
# No transaction is required since nothing is modified.
found = db.search("$.key", "k1")
print(len(found), "documents having the value 'k1' for item 'key'.")
# output> 1 documents having the value 'k1' for item 'key'.

# Create a named index using JSONPath: documents giving one or more
# matches with the path will be present in the index.
with db:
    db.create_index("key_index", "$.key")
    doc = {"key": "k3"}
    db["in_index"] = doc
    db["not_in_index"] = {"key2": "k4"}

# 'lookup' returns a list of ids for matching documents from the named index.
# No transaction is required since nothing is modified.
found = db.lookup("key_index", "k3")
if len(found) == 1 and db[found[0]] == doc:
    print("Found doc is equal to previously input.")
# output> Found doc is equal to previously input.

if not db.in_index("key_index", "k4"):
    print("Document is not in this index.")
# output> Document is not in this index.

# 'range' returns a generator of identifiers of documents  matching
# the inclusive interval ["k1", "k2"].

ids = list(db.range("key_index", "k1", "k3"))
print(f"'range' returned {len(ids)} ids within low and high inclusive.")
# output> 'range' returned 2 ids within low and high inclusive.

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
