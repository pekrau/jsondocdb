"""jsondblite

Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic.

The Logic class was adapted from https://github.com/nadirizr/json-logic-py
"""

__version__ = "0.9.0"


import json
import re
import sqlite3


_INDEXNAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    return json.loads(data)

def _jsondoc_adapter(jsondoc):
    return json.dumps(jsondoc, ensure_ascii=False)

sqlite3.register_converter("JSONDOC", _jsondoc_converter)
sqlite3.register_adapter(dict, _jsondoc_adapter)


class jsondbliteException(Exception):
    "Base class for jsondblite errors."
    pass

class InvalidFileError(jsondbliteException):
    "The SQLite3 file is not a jsondblite file."
    pass

class NoSuchDocumentError(jsondbliteException):
    "The document was not found in the jsondblite database."
    pass

class InTransactionError(jsondbliteException):
    "Operation is invalid while in a transaction."

class NotInTransactionError(jsondbliteException):
    "Operation is invalid when not in a transaction."


class Jsondblite:
    "Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic."

    def __init__(self, filepath, **kwargs):
        """Open or create the database file.

        If the file exists, checks that it has the tables appropriate for jsondblite.

        If the file is created, creates the required tables.

        The filepath and any additional keyword arguments are passed  to
        sqlite3.connect, except for 'detect_types', which is hard-wired
        to sqlite3.PARSE_DECLTYPES.
        """
        kwargs["detect_types"] = sqlite3.PARSE_DECLTYPES  # Handle JSONDOC.
        self.cnx = sqlite3.connect(filepath, **kwargs)

        cursor = self.cnx.cursor()
        cursor = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = [n[0] for n in cursor.fetchall()]

        if names:     # Check that this is a jsondblite database file.
            if set(names) != set(["docs", "indexes", "attachments"]):
                raise InvalidFileError

        else:   # Empty; initialize as a jsondblite database.
            cursor.execute(
                "CREATE TABLE docs"
                "(id TEXT PRIMARY KEY,"
                " doc JSONDOC NOT NULL)")
            cursor.execute(
                "CREATE TABLE indexes"
                "(name TEXT PRIMARY KEY,"
                " path JSONDOC NOT NULL,"
                " require JSONDOC)"
            )
            cursor.execute(
                "CREATE TABLE attachments"
                "(docid TEXT NOT NULL,"  # Foreign key to docs.id
                " name TEXT NOT NULL,"
                " mimetype TEXT NOT NULL,"
                " size INT NOT NULL,"
                " data BLOB NOT NULL)"
            )

    def __str__(self):
        "Return a string with info on number of documents and indexes."
        return f"jsondblite {__version__}: {len(self)} documents, {self.count_indexes()} indexes, {self.count_attachments()} attachments."

    def __iter__(self):
        "Return an iterator over ids for all documents in the database."
        return (row[0] for row in self.cnx.execute("SELECT id FROM docs ORDER BY id"))

    def __len__(self):
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __getitem__(self, id):
        "Return the document with the given id."
        row = self.cnx.execute("SELECT doc FROM docs WHERE id=?", (id,)).fetchone()
        if not row:
            raise NoSuchDocumentError(f"No such document '{id}'.")
        return row[0]

    def __setitem__(self, id, doc):
        """Add or update the document in the database with the given id.

        Raises NotInTransactionError
        """
        if not self.in_transaction:
            raise NotInTransactionError("Cannot set item when not in transaction.")
        if not isinstance(id, str):
            raise ValueError("'id' must be an instance of 'str'.")
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        cursor = self.cnx.cursor()
        try:
            cursor.execute("INSERT INTO docs (id, doc) VALUES (?, ?)", (id, doc))
        except sqlite3.IntegrityError:
            cursor.execute("UPDATE docs SET doc=? where id=?", (doc, id))

    def __delitem__(self, id):
        """Delete the document with the given id from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        self.delete(id)

    def __contains__(self, id):
        "Return `True` if the given id is in the database, else `False`."
        return bool(self.cnx.execute("SELECT COUNT(*) FROM docs WHERE id=?", (id,)).fetchone()[0])

    def __enter__(self):
        """A context manager for a transaction. All operations that modify
        the data must occur within a transaction.
        If all goes well, the transaction is committed.
        If an error occurs within the context block, the transaction is rolled back.

        Raises InTransactionError, if already within a transaction.
        """
        if self.in_transaction:
            raise InTransactionError("Already within a transaction.")
        self.cnx.execute("BEGIN")

    def __exit__(self, type, value, tb):
        """End a transaction; commit if OK, rollback if not.
        No effect if not within a transaction.
        """
        if type is None:
            if self.in_transaction:
                self.cnx.execute("COMMIT")
        else:
            if self.in_transaction:
                self.cnx.execute("ROLLBACK")
        return False

    @property
    def in_transaction(self):
        "Are we within a transaction?"
        return self.cnx.in_transaction

    def __del__(self):
        """Close the database connection.

        Raises InTransactionError if within a transaction.
        """
        self.close()

    def get(self, id, default=None):
        "Return the document with the given id. If not found, return the 'default'."
        try:
            return self[id]
        except NoSuchDocumentError:
            return default

    def keys(self):
        "Return an iterator over ids for all documents in the database."
        return iter(self)

    def values(self):
        "Return an iterator over all documents in the database."
        return (row[0] for row in self.cnx.execute("SELECT doc FROM docs ORDER BY id"))

    def items(self):
        "Return an iterator over all tuples (id, document)n in the database."
        return ((row[0], row[1]) for row in self.cnx.execute("SELECT id, doc FROM docs ORDER BY id"))

    def delete(self, id):
        """Delete the document with the given id from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        if not self.in_transaction:
            raise NotInTransactionError("Cannot delete an item when not in a transaction.")
        cursor = self.cnx.cursor()
        cursor.execute("DELETE FROM docs WHERE id=?", (id,))
        if cursor.rowcount != 1:
            raise NoSuchDocumentError
        cursor.execute("DELETE FROM attachments WHERE docid=?", (id,))

    def get_indexes(self):
        "Return the list of names for the current indexes."
        return [indexname for (indexname,) in self.cnx.execute("SELECT name FROM indexes")]

    def count_documents(self):
        "Return the number of documents in the database."
        return len(self)

    def count_indexes(self):
        "Return the number of indexes in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM indexes").fetchone()[0]

    def count_attachments(self):
        "Return the number of attachments in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM attachments").fetchone()[0]

    def close(self):
        """Close the database connection.

        Raises InTransactionError if within a transaction.
        """
        if self.in_transaction:
            raise InTransactionError("Cannot close while in a transaction.")
        try:
            self.cnx.close()
            del self.cnx
        except AttributeError:
            pass


if __name__ == "__main__":
    db = Jsondblite("test.db")
    print(db, list(db))
    with db:
        db["b"] = {"b": 2, "c":3, "d": [1,2,3]}
    with db:
        db["x"] = {"erty": "apa"}
    print(db, list(db))
    db.close()
