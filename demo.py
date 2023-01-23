from jsondocdb import Database, IndexSpecificationError

db = Database("test.db")
with db:
    db["b"] = {"num": 2, "c": 3, "d": [1, 2, 3]}
    db["x"] = {"erty": "apa"}
    db["y"] = {"content": "blopp"}

try:
    db.delete_index("some")
    print("deleted index 'some'")
except NoSuchIndexError:
    pass
try:
    db.delete_index("content")
    print("deleted index 'content'")
except NoSuchIndexError:
    pass

db.create_index("some", "num")
print("created index 'some'")
db.create_index("content", "content")
print("created index 'content'")
print(db.lookup_count("content", "blopp"), "key 'blopp' in index 'content'")


with db:
    for i in range(100):
        db[f"d{i}"] = {"num": i, "content": f"Some string with a number {2*i}."}

print(type(db.items()))

for result in db.range_documents("some", 2.1, 5.1):
    print(result, type(result))
    print(result)

print(db.range_count("some", 2.1, 5.1))
print(list(db.lookup_documents("some", 2)))
