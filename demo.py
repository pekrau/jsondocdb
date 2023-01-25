from jsondocdb import Database, IndexSpecificationError, NoSuchIndexError

db = Database(":memory:")
with db:
    db["b"] = {"num": 2, "c": 3, "d": [1, 2, 3]}
    db["x"] = {"erty": "apa"}
    db["y"] = {"content": "blopp"}

try:
    db.index("some").delete()
    print("deleted index 'some'")
except NoSuchIndexError:
    pass
try:
    db.index("content").delete()
    print("deleted index 'content'")
except NoSuchIndexError:
    pass

x = db.index("some", "num")
print(x)

y = db.index("content", "content")
print(y)
print(len(list(y.get("blopp"))), "key 'blopp' in index 'content'")


with db:
    for i in range(100):
        db[f"d{i}"] = {"num": i, "content": f"Some string with a number {2*i}."}

for result in x.range_documents(2.1, 5.1):
    print(result, type(result))

a = db.attachments("x")
filepath = "demo.py"
with open(filepath, "rb") as infile:
    content = infile.read()
with db:
    a.put(filepath, content)
print(db)

print(len(a))
for name in a:
    attachment = a.get(name)
    print(attachment.name, attachment.content_type, len(attachment))
