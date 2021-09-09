"JSON document database built on Sqlite3 using Python."

import argparse
import json
import os.path
import sqlite3
import sys
import uuid

from jsonpath_ng import JSONPathError
from jsonpath_ng.ext import parse as pathparse

__version__ = "0.1.0"


class Idmon:
    "JSON document database built on Sqlite3 using Python."

    def __init__(self, path, create=False):
        """Connect to the Sqlite3 database file given by the path.
        The special path ':memory' indicates a RAM database.
        'create':
          - False: The database file must exist, and be an Idmon database.
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
            try:
                cursor = self.cnx.execute("SELECT COUNT(*) FROM docs")
            except sqlite3.Error:
                raise ValueError("Could not open the database; not Idmon file?")
        self._index_cache = {}  # key: path; value: expression (parsed path)

    def __iter__(self):
        return IteratorIuid(self)

    def initialize(self):
        "Set up the tables to hold documents and path indexes."
        try:
            self.cnx.execute("CREATE TABLE docs"
                             " (iuid TEXT PRIMARY KEY,"
                             "  doctype TEXT,"
                             "  doc TEXT)")
            self.cnx.execute("CREATE INDEX docs_doctype_ix"
                             " ON docs (doctype)")
            self.cnx.execute("CREATE TABLE index_defs"
                             " (ixid INTEGER PRIMARY KEY AUTOINCREMENT,"
                             "  path TEXT NOT NULL,"
                             "  doctype TEXT,"
                             "  UNIQUE (path, doctype))")
            self.cnx.execute("CREATE TABLE indexes"
                             " (iuid TEXT NOT NULL,"
                             "  ixid INTEGER NOT NULL,"
                             "  value NOT NULL)")
        except sqlite3.Error:
            raise ValueError("Could not initialize the Idmon database.")

    def put(self, doc, doctype=None, iuid=None):
        "Store the document"
        if not iuid:
            iuid = uuid.uuid4().hex
        with self.cnx:
            # XXX check does not already exist
            self.cnx.execute("INSERT INTO docs (iuid, doctype, doc)"
                             " VALUES (?, ?, ?)",
                             (iuid, doctype, json.dumps(doc)))
            self._index_add(iuid, doc, doctype=doctype)
        return iuid

    def update(self, doc, iuid):
        "Update the document with the given iuid."
        raise NotImplementedError

    def get(self, iuid):
        "Retrieve the document given its iuid."
        cursor = self.cnx.execute("SELECT doc FROM docs WHERE iuid=?",
                                  (iuid,))
        doc = cursor.fetchone()
        if not doc:
            raise KeyError(f"No such document '{iuid}'.")
        return json.loads(doc[0])

    def delete(self, iuid):
        "Delete the document with the given iuid from the database."
        with self.cnx:
            self._index_remove(iuid)
            self.cnx.execute("DELETE FROM docs WHERE iuid=?", (iuid,))

    def __len__(self):
        cursor = self.cnx.execute("SELECT COUNT(*) FROM docs")
        return cursor.fetchone()[0]

    def count(self, doctype):
        "Return the number of documents of the given doctype."
        cursor = self.cnx.execute("SELECT COUNT(*) FROM docs"
                                  " WHERE doctype=?", (doctype,))
        return cursor.fetchone()[0]

    def create_index(self, path, doctype=None):
        "Create an index given a JSON path and an optional doctype."
        try:
            expression = pathparse(path)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        try:
            # Unique does not work for NULL as needed here.
            if doctype is None:
                cursor = self.cnx.execute("SELECT COUNT(*) FROM index_defs"
                                          " WHERE path=?",
                                          (path,))
                if cursor.fetchone()[0] > 0:
                    raise ValueError
                                          
            with self.cnx:
                self.cnx.execute("INSERT INTO index_defs"
                                 " (path, doctype) VALUES (?, ?)",
                                 (path, doctype))
        except (sqlite3.Error, ValueError):
            raise ValueError(f"Index already exists with path '{path}'"
                             f" and doctype '{doctype}'.")
        self._index_cache[path] = expression
        cursor = self.cnx.execute("SELECT ixid FROM index_defs"
                                  " WHERE path=? AND doctype=?",
                                  (path, doctype))
        ixid = cursor.fetchone()[0]
        # XXX Go through docs, add to this index.

    def delete_index(self, path, doctype=None):
        "Delete the index with the given JSON path and optional doctype."
        raise NotImplementedError

    def get_indexes(self):
        "Return the current list of indexes."
        raise NotImplementedError

    def _index_add(self, iuid, doc, doctype=None):
        """Add the document with the given iuid to the applicable indexes.
        This operation must be performed within a transaction.
        """
        cursor = self.cnx.execute("SELECT ixid, path FROM index_defs"
                                  " WHERE doctype=?",
                                  (doctype,))
        paths = cursor.fetchall()
        for ixid, path in paths:
            try:
                expression = self._index_cache[path]
            except KeyError:
                expression = pathparse(path)
                self._index_cache[path] = expression
            for match in expression.find(doc):
                self.cnx.execute("INSERT INTO indexes"
                                 " (iuid, ixid, value) VALUES(?, ?, ?)",
                                 (iuid, ixid, match.value))

    def _index_remove(self, iuid):
        """Remove the document with the given iuid from the indexes.
        This operation must be performed within a transaction.
        """
        self.cnx.execute("DELETE FROM indexes WHERE iuid=?", (iuid,))


class IuidIterator:
    "Iterate over document identifiers in the database; all or given doctype."

    CHUNK_SIZE = 100

    def __init__(self, db, doctype=None):
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
            where = []
            args = []
            if self.doctype:
                where.append("doctype=?")
                args.append(self.doctype)
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

    def __init__(self, db, doctype=None):
        self.db = db
        self.iuiditerator = IuidIterator(self.db, doctype=doctype)

    def __iter__(self):
        return self

    def __next__(self):
        return self.db.get(next(self.iuiditerator))


def _get_parser():
    "Get the parser for the command-line tool."
    p = argparse.ArgumentParser(prog="idmon",
                                usage="%(prog)s dbfilepath [options]",
                                description="Idmon command line tool.")
    p.add_argument("dbfilepath", metavar="DBFILEPATH",
                   help="Path to the Sqlite3 Idmon database file.")
    x01 = p.add_mutually_exclusive_group()
    x01.add_argument("-c", "--create", action="store_true",
                     help="Create the database file.")
    return p

def _execute(args):
    try:
        if args.create:
            db = Idmon(args.dbfilepath, create=True)
        else:
            db = Idmon(args.dbfilepath, create=False)
    except IOError as error:
        sys.exit(str(error))

def main():
    "Command-line tool."
    parser = _get_parser()
    args = parser.parse_args()
    _execute(args)

if __name__ == "__main__":
    main()
