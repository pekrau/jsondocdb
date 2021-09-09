import json

import idmon

db = idmon.Idmon("test.idmon", initialize=True)
# db.create_index("$..key1", doctype="test")
iuid = db.put({"key1": "string value", "key2": 123456, "key3": False})
iuid = db.put({"key1": "string value", "key2": 1, "key3": True}, "test")
print(json.dumps(db.get(iuid), indent=2))
print(len(list(idmon.DocIterator(db))))
print(len(list(idmon.DocIterator(db, doctype='test'))))
print(len(db))
print(db.count('test'))
db.delete(iuid)
print(len(db))
print(db.count('test'))
