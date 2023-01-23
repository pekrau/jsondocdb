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

def test_create_db_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0
    os.remove(dbfile.name)

def test_create_close_reopen_db_file():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0
    db.close()
    db2 = jsondocdb.Database(dbfile.name)
    assert len(db2) == 0
    os.remove(dbfile.name)

def test_open_open_close_close():
    dbfile = tempfile.NamedTemporaryFile(delete=False)
    dbfile.close()
    db = jsondocdb.Database(dbfile.name)
    assert len(db) == 0
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
    assert len(db) == 0
    db.close()
    db2 = jsondocdb.Database(dbfile.name, readonly=True)
    assert len(db2) == 0
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
    assert len(db) == n + 1
    assert docid in db
    assert db[docid] == doc
    assert set(db.keys()) == set([docid])
    assert list(db.values()) == [doc]

def test_no_such_document(db):
    docid = "a document"
    doc = {"this": "is", "a": "document"}
    with db:
        db[docid] = doc
    assert docid in db
    assert "nonexistent" not in db
    with pytest.raises(jsondocdb.NoSuchDocumentError):
        doc2 = db["nonexistent"]

def test_add_doc_outside_transaction(db):
    with pytest.raises(jsondocdb.TransactionError):
        db["a document"] = {"this": "is", "a": "document"}

def test_add_doc_same_id(db):
    n = len(db)
    docid = "a document"
    with db:
        db[docid] = {"some": "content"}
    m = len(db)
    assert m == n + 1
    assert docid in db
    with db:
        db[docid] = {"different": "content"}
    assert m == len(db)
    assert docid in db
    assert set(db.keys()) == set([docid])

def test_several_docs(db):
    n = len(db)
    with db:
        for i in range(10):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == n + 10
    with db:
        for i in range(5, 16):
            docid = f"myname{i}"
            doc = dict(num=i, data="a string" * i)
            db[docid] = doc
    assert len(db) == n + 16
    for i in range(2,12):
        docid = f"myname{i}"
        assert docid in db
    for i in range(16, 18):
        docid = f"myname{i}"
        assert docid not in db
