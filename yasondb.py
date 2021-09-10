"Yet another JSON document database. Built on Sqlite3 in Python."

import json
import os.path
import re
import sqlite3
import sys
import uuid

from jsonpath_ng import JSONPathError
from jsonpath_ng.ext import parse as pathparse

__version__ = "0.1.3"

NAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


class YasonDB:
    "Yet another JSON document database."

    def __init__(self, path, create=False):
        """Connect to the Sqlite3 database file given by the path.
        The special path ':memory' indicates a RAM database.
        'create':
          - False: The database file must exist, and be an YasonDB database.
          - True: The database file must not exist; created and initialized.
        """
        if create:
            if os.path.exists(path):
                raise IOError(f"File '{path}' already exists.")
            self.cnx = sqlite3.connect(path)
            self.initialize()
        else:
            if not os.path.exists(path):
                raise IOError(f"File '{path}' does not exist.")
            self.cnx = sqlite3.connect(path)
            self.check_valid()
        self._index_cache = {}  # key: path; value: expression (parsed path)

    def __str__(self):
        return f"YasonDb: {len(self)} documents, {len(self.get_indexes())} indexes"

    def __iter__(self):
        "Return an iterator over all document iuid's."
        return IuidIterator(self)

    def __len__(self):
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __del__(self):
        self.close()

    def __getitem__(self, iuid):
        cursor = self.cnx.execute("SELECT doc FROM docs WHERE iuid=?", (iuid,))
        doc = cursor.fetchone()
        if not doc:
            raise KeyError(f"No such document '{iuid}'.")
        return json.loads(doc[0])

    def __setitem__(self, iuid, doc):
        """If the document with the given iuid exists, update it.
        If no document with the given iuid exists, add it,
        setting the doctype to 'default'.
        To set the doctype to some other value, use 'put'.
        """
        try:
            self.update(iuid, doc)
        except KeyError:
            self.put(doc, iuid=iuid)

    def __delitem__(self, iuid):
        if iuid in self:
            self.delete(iuid)
        else:
            raise KeyError(f"No such document '{iuid}'.")

    def __contains__(self, iuid):
        sql = "SELECT COUNT(*) FROM docs WHERE iuid=?"
        cursor = self.cnx.execute(sql, (iuid,))
        return bool(cursor.fetchone()[0])

    def initialize(self):
        "Set up the tables to hold documents and index definitions."
        try:
            self.cnx.execute("CREATE TABLE docs"
                             " (iuid TEXT PRIMARY KEY,"
                             "  doctype TEXT NOT NULL,"
                             "  doc TEXT NOT NULL)")
            self.cnx.execute("CREATE INDEX docs_doctype_ix"
                             " ON docs (doctype)")
            self.cnx.execute("CREATE TABLE indexes"
                             " (name TEXT PRIMARY KEY,"
                             "  path TEXT NOT NULL,"
                             "  doctype TEXT NOT NULL)")
        except sqlite3.Error:
            raise ValueError("Could not initialize the YasonDB database.")

    def is_valid(self):
        "Is the database a valid YasonDB one?"
        try:
            self.cnx.execute("SELECT COUNT(*) FROM docs")
            self.cnx.execute("SELECT COUNT(*) FROM indexes")
        except sqlite3.Error:
            return False
        return True

    def check_valid(self):
        "Check that the database is a valid YasonDB one."
        if not self.is_valid():
            raise ValueError("Could not read the database; not a YasonDB file?")

    def get(self, iuid, default=None):
        "Retrieve the document given its iuid, else the default."
        try:
            return self[iuid]
        except KeyError:
            return default

    def put(self, doc, doctype='default', iuid=None):
        """Store the document.
        If 'iuid' is not given, create a UUID4 iuid.
        Raise KeyError if the iuid already exists in the database.
        Return the iuid.
        """
        if not iuid:
            iuid = uuid.uuid4().hex
        with self.cnx:
            try:
                sql = "INSERT INTO docs (iuid, doctype, doc) VALUES (?, ?, ?)"
                self.cnx.execute(sql, (iuid, doctype, json.dumps(doc)))
            except sqlite3.DatabaseError:
                raise KeyError(f"The iuid '{iuid}' already exists.")
            self._add_to_indexes(iuid, doc, doctype)
        return iuid

    def update(self, iuid, doc):
        """Update the document with the given iuid.
        The doctype cannot be changed.
        Raise KeyError if no such iuid in the database.
        """
        sql = "UPDATE docs SET doc=? WHERE iuid=?"
        cursor = self.cnx.execute(sql, (json.dumps(doc), iuid))
        if cursor.rowcount != 1:
            raise KeyError(f"No such document '{iuid}' to update.")
        with self.cnx:
            self._remove_from_indexes(iuid)
            sql = "SELECT doctype FROM docs WHERE iuid=?"
            cursor = self.cnx.execute(sql, (iuid,))
            self._add_to_indexes(iuid, doc, cursor.fetchone()[0])

    def delete(self, iuid):
        """Delete the document with the given iuid from the database.
        No error if the document with the given key does not exist.
        """
        with self.cnx:
            self._remove_from_indexes(iuid)
            self.cnx.execute("DELETE FROM docs WHERE iuid=?", (iuid,))

    def docs(self):
        "Return an iterator over all documents in the database."
        return DocIterator(self)

    def count(self, doctype):
        "Return the number of documents of the given doctype."
        sql = "SELECT COUNT(*) FROM docs WHERE doctype=?"
        cursor = self.cnx.execute(sql, (doctype,))
        return cursor.fetchone()[0]

    def create_index(self, name, path, doctype="default"):
        "Create an index given a JSON path and a doctype."
        if not NAME_RX.match(name):
            raise ValueError(f"Invalid index name '{name}'.")
        if self.index_exists(name):
            raise ValueError(f"Index '{name}' is already defined.")
        try:
            expression = pathparse(path)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        try:
            with self.cnx:
                sql = "INSERT INTO indexes (name, path, doctype) VALUES (?,?,?)"
                self.cnx.execute(sql, (name, path, doctype))
                sql = f"CREATE TABLE index_{name}" \
                    " (iuid TEXT PRIMARY KEY, ikey NOT NULL)"
                self.cnx.execute(sql)
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{name}': {error}")
        self._index_cache[name] = expression
        with self.cnx:
            sql = "SELECT iuid, doc FROM docs WHERE doctype=?"
            cursor = self.cnx.execute(sql, (doctype,))
            sql = f"INSERT INTO index_{name} (iuid, ikey) VALUES(?, ?)"
            for iuid, doc in cursor:
                for match in expression.find(json.loads(doc)):
                    self.cnx.execute(sql, (iuid, match.value))

    def index_exists(self, name):
        "Does an index with the given name exist?"
        sql = "SELECT COUNT(*) FROM indexes WHERE name=?"
        cursor = self.cnx.execute(sql, (name,))
        return bool(cursor.fetchone()[0])

    def get_indexes(self):
        "Return the list names for the current indexes."
        sql = "SELECT name FROM indexes"
        return [name for (name,) in self.cnx.execute(sql)]

    def get_index(self, name):
        "Return definition and statistics for the named index."
        try:
            sql = "SELECT path, doctype FROM indexes WHERE name=?"
            cursor = self.cnx.execute(sql, (name,))
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise ValueError
            (path, doctype) = rows[0]
            result = {"path": path, "doctype": doctype}
            cursor = self.cnx.execute(f"SELECT COUNT(*) FROM index_{name}")
            result["count"] = cursor.fetchone()[0]
        except (ValueError, sqlite3.Error):
            raise KeyError(f"No such index '{name}'.")
        if result["count"] > 0:
            cursor = self.cnx.execute(f"SELECT MIN(ikey) FROM index_{name}")
            result["min"] = cursor.fetchone()[0]
            cursor = self.cnx.execute(f"SELECT MAX(ikey) FROM index_{name}")
            result["max"] = cursor.fetchone()[0]
        return result

    def in_index(self, name, iuid):
        "Is the given iuid in the named index?"
        try:
            sql = f"SELECT COUNT(*) FROM index_{name} WHERE iuid=?"
            cursor = self.cnx.execute(sql, (iuid,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return bool(cursor.fetchone()[0])

    def delete_index(self, name):
        "Delete the index with the given name."
        if not self.index_exists(name):
            raise ValueError(f"No index '{name}' exists.")
        with self.cnx:
            self.cnx.execute("DELETE FROM indexes WHERE name=?", (name,))
            self.cnx.execute(f"DROP TABLE index_{name}")
            self._index_cache.pop(name, None)

    def find(self, name, key):
        """Return a generator of tuples containing (iuid, document) for
        all documents having the given key in the named index.
        """
        try:
            sql = f"SELECT index_{name}.iuid, docs.doc FROM index_{name}, docs"\
                f" WHERE index_{name}.ikey=? AND docs.iuid=index_{name}.iuid"
            cursor = self.cnx.execute(sql, (key,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return ((name, json.loads(doc)) for name, doc in cursor)

    def range(self, name, lowkey, highkey):
        """Return a generator of tuples containing (iuid, document) for
        all documents having a key in the named index within the given
        inclusive range.
        """
        try:
            sql = f"SELECT index_{name}.iuid, docs.doc FROM index_{name}, docs"\
                f" WHERE index_{name}.ikey>=? AND index_{name}.ikey<=?" \
                f" AND docs.iuid=index_{name}.iuid ORDER BY index_{name}.ikey"
            cursor = self.cnx.execute(sql, (lowkey, highkey))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return ((name, json.loads(doc)) for name, doc in cursor)

    def close(self):
        "Close the connection."
        try:
            self.cnx.close()
            del self.cnx
        except AttributeError:
            pass

    def _add_to_indexes(self, iuid, doc, doctype):
        """Add the document with the given iuid to the applicable indexes.
        This operation must be performed within a transaction.
        """
        sql = "SELECT name, path FROM indexes WHERE doctype=?"
        cursor = self.cnx.execute(sql, (doctype,))
        for name, path in cursor:
            try:
                expression = self._index_cache[name]
            except KeyError:
                expression = pathparse(path)
                self._index_cache[name] = expression
            sql = f"INSERT INTO index_{name} (iuid, ikey) VALUES(?, ?)"
            for match in expression.find(doc):
                self.cnx.execute(sql, (iuid, match.value))

    def _remove_from_indexes(self, iuid):
        """Remove the document with the given iuid from the indexes.
        This operation must be performed within a transaction.
        """
        sql = "SELECT indexes.name FROM indexes, docs" \
            " WHERE indexes.doctype=docs.doctype AND docs.iuid=?"
        cursor = self.cnx.execute(sql, (iuid,))
        for (name,) in cursor:
            self.cnx.execute(f"DELETE FROM index_{name} WHERE iuid=?", (iuid,))


class IuidIterator:
    "Iterate over document identifiers in the database; all or given doctype."

    CHUNK_SIZE = 100

    def __init__(self, db, doctype='default'):
        self.doctype = doctype
        self.cursor = db.cnx.cursor()
        self.chunk = []
        self.offset = None
        self.pos = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.chunk[self.pos][0]
        except IndexError:
            sql = f"SELECT iuid FROM docs WHERE doctype=? LIMIT {self.CHUNK_SIZE}"
            if self.offset is not None:
                sql += f" OFFSET {self.offset}"
            self.cursor.execute(sql, (self.doctype,))
            self.chunk = self.cursor.fetchall()
            if len(self.chunk) == 0:
                raise StopIteration
            elif self.offset is None:
                self.offset = self.CHUNK_SIZE
            else:
                self.offset += self.CHUNK_SIZE
            self.pos = 0
            return self.chunk[self.pos][0]
        finally:
            self.pos += 1


class DocIterator:
    "Iterate over documents in the database; all or given doctype."

    def __init__(self, db, doctype='default'):
        self.db = db
        self.iuiditerator = IuidIterator(self.db, doctype=doctype)

    def __iter__(self):
        return self

    def __next__(self):
        return self.db.get(next(self.iuiditerator))


if __name__ == "__main__":
    main()
