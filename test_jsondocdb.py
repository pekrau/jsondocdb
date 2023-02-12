"Pytest functions for the module jsondocdb."

import os
import sqlite3
import uuid

import pytest

import jsondocdb


def get_filepath():
    base_filepath = "/tmp/test_jsondocdb"
    count = 0
    filepath = base_filepath + str(count)
    while os.path.exists(filepath):
        count += 1
        filepath = base_filepath + str(count)
    return filepath

@pytest.fixture
def db():
    "Get a newly created database."
    filepath = get_filepath()
    db = jsondocdb.Database(filepath)
    yield db
    db.close()
    os.remove(filepath)

def add_some_documents(db):
    with db:
        db["first document"] = dict(a=1, b="two", c="III")
        db["second"] = dict(a=2, text="Some text.")
        db["third"] = dict(a=3, text="Another text.", d=True)
        db["fourth"] = dict(a=4, text="Some text.", d=False, x=[3, 2, "mix"])
        db[uuid.uuid4().hex] = dict(a=19, text="Further along.",
                                    x={"lkla": 234,"q": [1,2]})

def test_create_db_file():
    filepath = get_filepath()
    db = jsondocdb.Database() 
    db.create(filepath)
    assert len(db) == 0, "The database should be empty."
    os.remove(filepath)

def test_create_close_reopen_db_file():
    filepath = get_filepath()
    db = jsondocdb.Database(filepath) 
    assert len(db) == 0, "The database should be empty."
    db.close()
    db2 = jsondocdb.Database(filepath)
    assert len(db2) == 0, "The database should be empty."
    with pytest.raises(OSError):
        db3 = jsondocdb.Database()
        db3.create(filepath)
    os.remove(filepath)

def test_open_open_close_close():
    filepath = get_filepath()
    with pytest.raises(OSError):
        db = jsondocdb.Database()
        db.open(filepath)
    db = jsondocdb.Database(filepath) 
    assert len(db) == 0, "The database should be empty."
    with pytest.raises(jsondocdb.ConnectionError):
        db.open(filepath)
    db.close()
    with pytest.raises(jsondocdb.ConnectionError):
        db.close()
    os.remove(filepath)

def test_create_close_reopen_readonly_db_file():
    filepath = get_filepath()
    with pytest.raises(OSError):
        db = jsondocdb.Database(filepath, readonly=True)
        db.open(filepath)
    db = jsondocdb.Database(filepath)
    assert len(db) == 0, "The database should be empty."
    db.close()
    db2 = jsondocdb.Database(filepath, readonly=True)
    assert len(db2) == 0, "The database should be empty."
    os.remove(filepath)

def test_not_a_sqlite_file():
    with pytest.raises(jsondocdb.InvalidFileError):
        db = jsondocdb.Database("test_jsondocdb.py")

def test_sqlite_file_but_not_jsondocdb_file():
    filepath = get_filepath()
    cnx = sqlite3.connect(filepath)
    cnx.execute("CREATE TABLE stuff (i INT PRIMARY KEY)")
    cnx.close()
    with pytest.raises(jsondocdb.InvalidFileError):
        db = jsondocdb.Database(filepath)
    os.remove(filepath)

def test_add_doc_retrieve(db):
    assert len(db) == 0, "Empty database."
    docid = "a document"
    doc = {"this": "is",
           "a": "document",
           "integer": 1,
           "alist": [1, 2, 3.14159],
           "adict": dict(a=1, b=2, c=3)}
    with db:
        db[docid] = doc
    assert len(db) == 1, "One document in the database."
    assert docid in db, "The identifier should be in the database."
    assert db[docid] == doc, "The identifier fetches its document."
    assert list(db.keys()) == [docid], "The list of identifiers in the database."
    assert list(db.values()) == [doc], "The list of documents in the database."

def test_no_such_document(db):
    docid = "a document"
    doc = {"this": "is", "a": "document"}
    with db:
        db[docid] = doc
    assert docid in db, "The identifier is in the database."
    assert "nonexistent" not in db, "The identifier is not in the database."
    with pytest.raises(jsondocdb.NoSuchDocumentError):
        doc2 = db["nonexistent"]

def test_transactions(db):
    with pytest.raises(jsondocdb.NotInTransactionError):
        db["a document"] = {"this": "is", "a": "document"}
    with db:
        with pytest.raises(jsondocdb.InTransactionError):
            with db:
                pass

def test_add_doc_same_id(db):
    assert len(db) == 0, "Empty database."
    docid = "a document"
    with db:
        db[docid] = {"some": "content"}
    m = len(db)
    assert m == 1, "One document in the database."
    assert docid in db, "The identifier is in the database."
    with db:
        db[docid] = {"different": "content"}
    assert m == len(db), "Same number of documents in the database; overwritten."
    assert docid in db, "The identifier is in the database."
    assert list(db.keys()) == [docid], "The list of identifiers in the database."

def test_several_docs(db):
    assert len(db) == 0, "Empty database."
    with db:
        for i in range(10):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == 10, "Ten documents in the database."
    with db:
        for i in range(5, 16):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == 16, "Net sixteen documents in the database."
    for i in range(2,12):
        docid = f"myname{i}"
        assert docid in db, "The identifier should be in the database."
    for i in range(16, 18):
        docid = f"myname{i}"
        assert docid not in db, "The identifier should not be in the database."

def test_create_delete_index(db):
    add_some_documents(db)
    x = db.index("my_index", "a", unique=True)
    assert len(db.indexes()) == 1, "The number of indexes in the database."
    with pytest.raises(jsondocdb.IndexExistsError):
        y = db.index("my_index", "text")
    with pytest.raises(jsondocdb.NoSuchIndexError):
        y = db.index("no_such_index", None)
    db.index("my_index").delete()
    assert len(db.indexes()) == 0
    with pytest.raises(jsondocdb.NoSuchIndexError):
        y = db.index("my_index")
    cursor = db.cnx.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert set([n[0] for n in cursor.fetchall()]) == set(["documents", "indexes", "attachments"]), "All index tables must have been deleted."


def test_index_get(db):
    add_some_documents(db)
    x = db.index("my_index", "a")
    assert len(db.indexes()) == 1, "The number of indexes in the database."
    assert len(x) == len(db), "The number of entries in the index equals the number of items."
    assert 2 in x, "The key should be in the index."
    assert 1000 not in x, "The key should not be in the index."
    assert [1,2,3] not in x, "Garbage key should not be in the index without raising an error."
    key = 2
    result = list(db.index("my_index").get(key))
    assert len(result) == 1, "One identifier fetched from index."
    id = result[0]
    doc = db[id]
    assert doc["a"] == key, "Correctly indexed document."
    result = list(db.index("my_index").get_documents(key))
    assert len(result) == 1, "One (identifier, document) fetched from index."
    identifier, document = result[0]
    assert document["a"] == key, "Correctly indexed document."
    assert id == identifier, "Same identifier fetched."
    assert doc == document, "Same document fetched."
    with db:
        del db[identifier]
    assert len(x) == len(db), "Removing item from database should also remove entry from index."
    
def test_index_get_unique(db):
    add_some_documents(db)
    x = db.index("index", "a", unique=True)
    assert len(db.indexes()) == 1, "The number of indexes in the database."
    assert len(x) == len(db), "The number of entries in the index equals the number of items."
    with pytest.raises(jsondocdb.NotUniqueError):
        y = db.index("unique_index", "text", unique=True)

def test_index_range(db):
    add_some_documents(db)
    x = db.index("my_index", "a")
    low = 1
    high = 3
    result = list(x.range(low, high))
    assert len(result) == 2, "Two items in index."
    assert result[0][0] in db, "Identifier in database."
    assert result[0][1] >= low, "Lower key bound."
    assert result[-1][1] < high, "Upper key bound."
    assert not list(x.range(-2, -1)), "Empty result."

def test_index_range_documents(db):
    add_some_documents(db)
    x = db.index("my_index", "a")
    low = 1
    high = 3
    result = list(x.range_documents(low, high))
    assert len(result) == 2, "Two items in index."
    docid = result[0][0]
    doc = result[0][2]
    assert docid in db, "Identifier in database."
    assert db[docid] == doc, "Same document."
    result = list(x.range_documents(low, high, reverse=True))
    assert len(result) == 2, "Two items in index."
    docid2 = result[0][0]
    doc2 = result[0][2]
    assert docid2 in db, "Identifier in database."
    assert db[docid2] == doc2, "Same document."
    assert docid == result[1][0], "Reversed order."

def test_attachment(db):
    add_some_documents(db)
    docid = "first document"
    assert docid in db, "Document in database."
    a = db.attachments(docid)
    assert len(a) == 0, "Initially no attachments for the document."
    filepath = "test_jsondocdb.py"
    with open(filepath, "rb") as infile:
        content = infile.read()
    length = len(content)
    with db:
        a.put(filepath, content)
    assert len(a) == 1, "One attachment for the document."
    b = db.attachments(docid)
    assert len(b) == 1, "One attachment for the document."
    att = b.get(filepath)
    assert att.name == filepath, "Attachment name is the filepath."
    assert att.content_type == "text/x-python", "Python content type."
    assert len(att) == length, "Correct length."
    faked_filepath = "tmp.txt"
    with db:
        b.put(faked_filepath, content=b"some text")
    assert len(b) == 2, "Two attachments for the document."
    assert set(b.keys()) == set([filepath, faked_filepath])
    with db:
        b.get(faked_filepath).delete()
    assert len(b) == 1, "One attachment for the document."
    with db:
        del b[filepath]
    assert len(b) == 0, "No attachments for the document."
