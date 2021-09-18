"Demo code for the README."

import yasondb

# Argument 'create' is by default False, requiring an existing file.
db = yasondb.YasonDB("demo.db", create=True)

# Database modifications must be done within a transaction for persistence,
# which is done by performing the modifying actions in a 'with' context.
with db:
    db["id1"] = {"key": "k1", "n": 3}
    doc = {"key": "k2", "n": 5}
    autoid = db.add(doc)

if db[autoid] == doc:
    print("Fetched doc is equal to previously input.")

with db:
    try:
        db.add({"key": "x"}, autoid)
    except KeyError as error:
        print(error)

with db:
    db.create_index("key_index", "$.key")
    db["in_index"] = {"key": "k3"}
    db["not_in_index"] = {"key2": "k4"}

found = db.find("key_index", "k2")
if len(found) == 1 and found[0] == doc:
    print("Found doc is equal to previously input.")

if not db.in_index("key_index", "k4"):
    print("Document having 'key2' but not 'key' item is not in this index.")

# Range returns a generator.
ids = list(db.range("key_index", "k1", "k2"))
if len(ids) == 2:
    print("'range' return ids within low and high inclusive.")
