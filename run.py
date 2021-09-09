import json

import yasondb

# db = yasondb.YasonDB("test.yasondb", create=True)
db = yasondb.YasonDB("test.yasondb")
# db.create_index("i1", "$..key1", doctype="test")
iuid = db.put({"key1": "string value", "key2": 123456, "key3": False})
iuid = db.put({"key1": "string value", "key2": 1, "key3": True}, "test")
print(json.dumps(db.get(iuid), indent=2))
print(len(list(yasondb.DocIterator(db))))
print(len(list(yasondb.DocIterator(db, doctype='test'))))
print(len(db))
print(db.count("test"))
print(db.get_indexes())
db.delete_index("i1")
