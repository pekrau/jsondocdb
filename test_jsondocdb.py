"Pytest functions for the module jsondocdb."

import os
import sqlite3
import tempfile
import uuid

import pytest

import jsondocdb


@pytest.fixture
def db():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    yield db
    db.close()
    os.remove(dbfile.name)

@pytest.fixture
def db_with_docs():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    with db:
        db["first document"] = dict(a=1, b="two", c="III")
        db["second"] = dict(a=2, text="Some text.")
        db["third"] = dict(a=3, text="Another text.", d=True)
        db["fourth"] = dict(a=4, text="Some text.", d=False, x=[3, 2, "mix"])
        db[uuid.uuid4().hex] = dict(a=19, text="Further along.",
                                    x={"lkla": 234,"q": [1,2]})
    yield db
    db.close()
    os.remove(dbfile.name)

def test_create_db_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name) 
    assert len(db) == 0, "The database should be empty."
    os.remove(dbfile.name)

def test_create_close_reopen_db_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0, "The database should be empty."
    db.close()
    db2 = jsondocdb.Database(dbfile.name)
    assert len(db2) == 0, "The database should be empty."
    os.remove(dbfile.name)

def test_open_open_close_close():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0, "The database should be empty."
    with pytest.raises(jsondocdb.ConnectionError):
        db.open(dbfile.name)
    db.close()
    with pytest.raises(jsondocdb.ConnectionError):
        db.close()
    os.remove(dbfile.name)

def test_create_close_reopen_readonly_db_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0, "The database should be empty."
    db.close()
    db2 = jsondocdb.Database(dbfile.name, readonly=True)
    assert len(db2) == 0, "The database should be empty."
    os.remove(dbfile.name)

def test_not_a_sqlite_file():
    with pytest.raises(jsondocdb.InvalidFileError):
        db = jsondocdb.Database("test_jsondocdb.py")

def test_sqlite_file_but_not_jsondocdb_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    cnx = sqlite3.connect(dbfile.name)
    cnx.execute("CREATE TABLE stuff (i INT PRIMARY KEY)")
    cnx.close()
    with pytest.raises(jsondocdb.InvalidFileError):
        db = jsondocdb.Database(dbfile.name)
    os.remove(dbfile.name)

def test_add_doc_retrieve(db):
    n = len(db)
    docid = "a document"
    doc = {"this": "is",
           "a": "document",
           "integer": 1,
           "alist": [1, 2, 3.14159],
           "adict": dict(a=1, b=2, c=3)}
    with db:
        db[docid] = doc
    assert len(db) == n + 1, "One more document in the database."
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
    n = len(db)
    docid = "a document"
    with db:
        db[docid] = {"some": "content"}
    m = len(db)
    assert m == n + 1, "One more document in the database."
    assert docid in db, "The identifier is in the database."
    with db:
        db[docid] = {"different": "content"}
    assert m == len(db), "Same number of documents in the database; overwritten."
    assert docid in db, "The identifier is in the database."
    assert list(db.keys()) == [docid], "The list of identifiers in the database."

def test_several_docs(db):
    n = len(db)
    with db:
        for i in range(10):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == n + 10, "The number of documents in the database."
    with db:
        for i in range(5, 16):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == n + 16, "The net number of documents in the database."
    for i in range(2,12):
        docid = f"myname{i}"
        assert docid in db, "The identifier should be in the database."
    for i in range(16, 18):
        docid = f"myname{i}"
        assert docid not in db, "The identifier should not be in the database."

def test_create_delete_index(db_with_docs):
    x = db_with_docs.index("first_index", "a", unique=True)
    assert len(db_with_docs.indexes()) == 1, "The number of indexes in the database."
    with pytest.raises(jsondocdb.IndexExistsError):
        y = db_with_docs.index("first_index", "text")
    with pytest.raises(jsondocdb.NoSuchIndexError):
        y = db_with_docs.index("no_such_index", None)
    db_with_docs.index("first_index").delete()
    assert len(db_with_docs.indexes()) == 0
    with pytest.raises(jsondocdb.NoSuchIndexError):
        y = db_with_docs.index("first_index")

def test_index_get(db_with_docs):
    x = db_with_docs.index("index_name", "a")
    assert len(db_with_docs.indexes()) == 1, "The number of indexes in the database."
    assert len(x) == len(db_with_docs), "The number of entries in the index equals the number of items."
    assert 2 in x, "The key should be in the index."
    assert 1000 not in x, "The key should not be in the index."
    assert [1,2,3] not in x, "Garbage key should not be in the index without raising an error."
    key = 2
    result = list(db_with_docs.index("index_name").get(key))
    assert len(result) == 1, "One identifier fetched from index."
    id = result[0]
    doc = db_with_docs[id]
    assert doc["a"] == key, "Correctly indexed document."
    result = list(db_with_docs.index("first_index").get_documents(key))
    assert len(result) == 1, "One (identifier, document) fetched from index."
    identifier, document = result[0]
    assert document["a"] == key, "Correctly indexed document."
    assert id == identifier, "Same identifier fetched."
    assert doc == document, "Same document fetched."
    with db_with_docs:
        del db_with_docs[identifier]
    assert len(x) == 3, "Removing item from database should also remove entry from index."
    

def test_index_get(db_with_docs):
    x = db_with_docs.index("index", "a", unique=True)
    assert len(db_with_docs.indexes()) == 1, "The number of indexes in the database."
    assert len(x) == len(db_with_docs), "The number of entries in the index equals the number of items."
    with pytest.raises(jsondocdb.NotUniqueError):
        y = db_with_docs.index("unique_index", "text", unique=True)

def test_index_range(db_with_docs):
    x = db_with_docs.index("my_index", "a")
