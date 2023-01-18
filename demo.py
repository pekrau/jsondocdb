from jsondocdb import Database, IndexSpecificationError

db = Database("test.db")
with db:
    db["b"] = {"num": 2, "c": 3, "d": [1, 2, 3]}
    db["x"] = {"erty": "apa"}

try:
    db.create_index("some", "num")
    print("created index 'some'")
    db.create_index("content", "content")
    print("created index 'content'")
    print(db.lookup_count("content", "blopp"), "key 'blopp' in index 'content'")
except IndexSpecificationError:
    db.delete_index("some")
    print("deleted index 'some'")
    db.delete_index("content")
    print("deleted index 'content'")

with db:
    for i in range(100):
        db[f"d{i}"] = {"num": i, "content": f"Some string with a number {2*i}."}

for doc in db.range_documents("some", 2.1, 5.1):
    print(doc)
print(db.range_count("some", 2.1, 5.1))
print(list(db.lookup_documents("some", 2)))
