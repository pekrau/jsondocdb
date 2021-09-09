"Yet another JSON document database. Built on Sqlite3 in Python."

import argparse
import json
import os.path
import re
import sqlite3
import sys
import uuid

from jsonpath_ng import JSONPathError
from jsonpath_ng.ext import parse as pathparse

__version__ = "0.1.0"

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
        cursor = self.cnx.execute("SELECT COUNT(*) FROM docs WHERE iuid=?",
                                  (iuid,))
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

    def put(self, doc, iuid=None, doctype='default'):
        """Store the document.
        If 'iuid' is not given, create a UUID4 iuid.
        Raise KeyError if the iuid already exists in the database.
        Return the iuid.
        """
        if not iuid:
            iuid = uuid.uuid4().hex
        with self.cnx:
            try:
                self.cnx.execute("INSERT INTO docs (iuid, doctype, doc)"
                                 " VALUES (?, ?, ?)",
                                 (iuid, doctype, json.dumps(doc)))
            except sqlite3.DatabaseError:
                raise KeyError(f"The iuid '{iuid}' already exists.")
            self._add_to_indexes(iuid, doc, doctype)
        return iuid

    def update(self, iuid, doc):
        """Update the document with the given iuid.
        The doctype cannot be changed.
        Raise KeyError if no such iuid in the database.
        """
        cursor = self.cnx.execute("UPDATE docs SET doc=? WHERE iuid=?",
                                  (json.dumps(doc), iuid))
        if cursor.rowcount != 1:
            raise KeyError(f"No such document '{iuid}' to update.")
        self._remove_from_indexes(iuid)
        cursor = self.cnx.execute("SELECT doctype FROM docs WHERE iuid=?",
                                  (iuid,))
        self._add_to_indexes(iuid, doc, cursor.fetchone()[0])

    def delete(self, iuid):
        """Delete the document with the given iuid from the database.
        No error if the document with the given key does not exist.
        """
        with self.cnx:
            self._remove_from_indexes(iuid)
            self.cnx.execute("DELETE FROM docs WHERE iuid=?", (iuid,))

    def count(self, doctype):
        "Return the number of documents of the given doctype."
        cursor = self.cnx.execute("SELECT COUNT(*) FROM docs"
                                  " WHERE doctype=?", (doctype,))
        return cursor.fetchone()[0]

    def create_index(self, name, path, doctype="default"):
        "Create an index given a JSON path and a doctype."
        if not NAME_RX.match(name):
            raise ValueError("Invalid index name '{name}'.")
        if self.index_exists(name):
            raise ValueError("Index '{name}' is already defined.")
        try:
            expression = pathparse(path)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        try:
            with self.cnx:
                self.cnx.execute("INSERT INTO indexes"
                                 " (name, path, doctype) VALUES (?, ?, ?)",
                                 (name, path, doctype))
                self.cnx.execute(f"CREATE TABLE index_{name}"
                                 " (iuid TEXT PRIMARY KEY,"
                                 "  value NOT NULL)")
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{name}': {error}")
        self._index_cache[name] = expression
        cursor = self.cnx.execute("SELECT iuid, doc FROM docs WHERE doctype=?",
                                  (doctype,))
        for iuid, doc in cursor:
            self._add_to_indexes(iuid, json.loads(doc), doctype)

    def index_exists(self, name):
        "Does an index with the given name exist?"
        cursor = self.cnx.execute("SELECT COUNT(*) FROM indexes WHERE name=?",
                                  (name,))
        return bool(cursor.fetchone()[0])

    def get_indexes(self):
        "Return the current list of indexes"
        result = {}
        for row in self.cnx.execute("SELECT name, path, doctype FROM indexes"):
            result[row[0]] = {"path": row[1],
                              "doctype": row[2]}
        return result

    def count_index(self, name):
        "Return the number of items in the named index."
        try:
            cursor = self.cnx.execute(f"SELECT COUNT(*) FROM index_{name}")
        except sqlite3.Error as error:
            raise KeyError(f"No such index '{name}'.")
        return cursor.fetchone()[0]

    def in_index(self, name, iuid):
        "Is the given iuid in the named index?"
        try:
            cursor = self.cnx.execute(f"SELECT COUNT(*) FROM index_{name}"
                                      " WHERE iuid=?",
                                      (iuid,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return bool(cursor.fetchone()[0])

    def find(self, name, value):
        """Return a list of tuples containing (iuid, document) for all
        documents  having the given value in the named index.
        """
        try:
            cursor = self.cnx.execute(f"SELECT index_{name}.iuid, docs.doc"
                                      " FROM index_{name}, docs"
                                      " WHERE index_{name}.value=?"
                                      " AND docs.iuid=index_{name}.iuid",
                                      (value,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return [(row[0], json.loads(row[1])) for row in cursor]

    def delete_index(self, name):
        "Delete the index with the given JSON path and optional doctype."
        if not self.index_exists(name):
            raise ValueError(f"No index '{name}' exists.")
        with self.cnx:
            self.cnx.execute("DELETE FROM indexes WHERE name=?", (name,))
            self.cnx.execute(f"DROP TABLE index_{name}")
            self._index_cache.pop(name, None)

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
        cursor = self.cnx.execute("SELECT name, path FROM indexes"
                                  " WHERE doctype=?",
                                  (doctype,))
        for name, path in cursor:
            try:
                expression = self._index_cache[name]
            except KeyError:
                expression = pathparse(path)
                self._index_cache[name] = expression
            for match in expression.find(doc):
                self.cnx.execute(f"INSERT INTO index_{name}"
                                 " (iuid, value) VALUES(?, ?)",
                                 (iuid, match.value))

    def _remove_from_indexes(self, iuid):
        """Remove the document with the given iuid from the indexes.
        This operation must be performed within a transaction.
        """
        cursor = self.cnx.execute("SELECT indexes.name FROM indexes, docs"
                                  " WHERE indexes.doctype=docs.doctype"
                                  " AND docs.iuid=?",
                                  (iuid,))
        for name in [row[0] for row in cursor]:
            self.cnx.execute(f"DELETE FROM index_{name} WHERE iuid=?", (iuid,))


class IuidIterator:
    "Iterate over document identifiers in the database; all or given doctype."

    CHUNK_SIZE = 100

    def __init__(self, db, doctype='default'):
        self.doctype = doctype
        self.cursor = db.cnx.cursor()
        self.chunk = []
        self.last = None
        self.pos = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.chunk[self.pos][0]
        except IndexError:
            where = ["doctype=?"]
            args = [self.doctype]
            if self.last is not None:
                where.append("iuid>?")
                args.append(self.last)
            if where:
                where = "WHERE " + " AND ".join(where)
            else:
                where = ""
            sql = "SELECT iuid FROM docs" \
                f" {where} ORDER BY iuid LIMIT {self.CHUNK_SIZE}"
            self.cursor.execute(sql, args)
            self.chunk = self.cursor.fetchall()
            try:
                self.last = self.chunk[-1][0]
            except IndexError:
                raise StopIteration
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


def _get_parser():
    "Get the parser for the command-line tool."
    p = argparse.ArgumentParser(prog="yasondb",
                                usage="%(prog)s dbfilepath [options]",
                                description="YasonDB command line tool.")
    p.add_argument("dbfilepath", metavar="DBFILEPATH",
                   help="Path to the Sqlite3 YasonDB database file.")
    x01 = p.add_mutually_exclusive_group()
    x01.add_argument("-c", "--create", action="store_true",
                     help="Create the database file.")
    return p

def _execute(args):
    try:
        if args.create:
            db = YasonDB(args.dbfilepath, create=True)
        else:
            db = YasonDB(args.dbfilepath, create=False)
    except IOError as error:
        sys.exit(str(error))

def main():
    "Command-line tool."
    parser = _get_parser()
    args = parser.parse_args()
    _execute(args)

if __name__ == "__main__":
    main()
