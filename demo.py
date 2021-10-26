"Demo code for the README."

try:
    import os
    os.remove("demo.db")
except OSError:
    pass

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
