"Yet another JSON document database. Built on Sqlite3 in Python."

import json
import os.path
import re
import sqlite3
import sys
import uuid

import click
from jsonpath_ng import JSONPathError
from jsonpath_ng.ext import parse as pathparse

__version__ = "0.3.2"

NAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    return json.loads(data)

def _jsondoc_adapter(jsondoc):
    return json.dumps(jsondoc, ensure_ascii=False)

def _json_str(doc, indent):
    if isinstance(indent, int) and indent <= 0: indent = None
    return json.dumps(doc, indent=indent, ensure_ascii=False)


class YasonDB:
    "Yet another JSON document database."

    def __init__(self, path, create=False):
        """Connect to the Sqlite3 database file given by the path.
        The special path ':memory' indicates a RAM database.
        'create':
          - False: The database file must exist, and be a YasonDB database.
          - True: The database file must not exist; created and initialized.
        """
        if create:
            if os.path.exists(path):
                raise IOError(f"File '{path}' already exists.")
            self._connect(path)
            self.initialize()
        else:
            if not os.path.exists(path):
                raise IOError(f"File '{path}' does not exist.")
            self._connect(path)
            self.check_valid()
        self._index_cache = {}  # key: path; value: expression (parsed path)

    def _connect(self, path):
        "Return the Sqlite3 connection."
        sqlite3.register_converter("JSONDOC", _jsondoc_converter)
        sqlite3.register_adapter(dict, _jsondoc_adapter)
        self.cnx = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)

    def __str__(self):
        return f"YasonDB: {len(self)} documents, {len(self.get_indexes())} indexes."

    def __iter__(self):
        "Return an iterator over all document iuid's."
        return IuidIterator(self)

    def __len__(self):
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __del__(self):
        self.close()

    def __getitem__(self, iuid):
        cursor = self.cnx.execute("SELECT doc FROM docs WHERE iuid=?", (iuid,))
        row = cursor.fetchone()
        if not row:
            raise KeyError(f"No such document '{iuid}'.")
        return row[0]

    def __setitem__(self, iuid, doc):
        """If the document with the given iuid exists, update it.
        If no document with the given iuid exists, add it.
        """
        self.update(iuid, doc, add=True)

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
                             "  doc JSONDOC NOT NULL)")
            self.cnx.execute("CREATE TABLE indexes"
                             " (name TEXT PRIMARY KEY,"
                             "  path TEXT NOT NULL)")
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

    def add(self, doc, iuid=None):
        """Add the document to the database.
        If 'iuid' is not provided, create a UUID4 iuid.
        Raise ValueError if the document is not a dictionary.
        Raise KeyError if the iuid already exists in the database.
        Return the iuid.
        """
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        if not iuid:
            iuid = uuid.uuid4().hex
        with self.cnx:
            try:
                sql = "INSERT INTO docs (iuid, doc) VALUES (?, ?)"
                self.cnx.execute(sql, (iuid, doc))
            except sqlite3.DatabaseError:
                raise KeyError(f"The iuid '{iuid}' already exists.")
            self._add_to_indexes(iuid, doc)
        return iuid

    def update(self, iuid, doc, add=False):
        """Update the document with the given iuid.
        Raise ValueError if the document is not a dictionary.
        Raise KeyError if no such iuid in the database and 'add' is False.
        """
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        with self.cnx:
            sql = "UPDATE docs SET doc=? WHERE iuid=?"
            cursor = self.cnx.execute(sql, (doc, iuid))
            if cursor.rowcount == 1: # Actually updated.
                self._remove_from_indexes(iuid)
                self._add_to_indexes(iuid, doc)
            elif add:
                self.add(doc, iuid=iuid)
            else:
                raise KeyError(f"No such document '{iuid}' to update.")

    def delete(self, iuid):
        """Delete the document with the given iuid from the database.
        No error if the document with the given key does not exist.
        """
        with self.cnx:
            self._remove_from_indexes(iuid)
            cursor = self.cnx.execute("DELETE FROM docs WHERE iuid=?", (iuid,))
            if cursor.rowcount == 0:
                raise KeyError(f"No such document '{iuid}' to delete.")

    def docs(self):
        "Return an iterator over all documents in the database."
        return DocIterator(self)

    def create_index(self, name, path):
        "Create an index for a given JSON path."
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
                sql = "INSERT INTO indexes (name, path) VALUES (?, ?)"
                self.cnx.execute(sql, (name, path))
                sql = f"CREATE TABLE index_{name}" \
                    " (iuid TEXT PRIMARY KEY, ikey NOT NULL)"
                self.cnx.execute(sql)
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{name}': {error}")
        self._index_cache[name] = expression
        with self.cnx:
            sql = "SELECT iuid, doc FROM docs"
            cursor = self.cnx.execute(sql)
            sql = f"INSERT INTO index_{name} (iuid, ikey) VALUES(?, ?)"
            for iuid, doc in cursor:
                for match in expression.find(doc):
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
            sql = "SELECT path FROM indexes WHERE name=?"
            cursor = self.cnx.execute(sql, (name,))
            row = cursor.fetchone()
            if not row:
                raise ValueError
            result = {"path": row[0]}
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

    def find(self, name, key, limit=None, offset=None):
        """Return a list of iuids for all documents having
        the given key in the named index.
        """
        sql = f"SELECT iuid FROM index_{name} WHERE ikey=?"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            cursor = self.cnx.execute(sql, (key,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return [row[0] for row in cursor]

    def find_docs(self, name, key, limit=None, offset=None):
        "Return a list of documents having the given key in the named index."
        sql = f"SELECT docs.doc FROM index_{name}, docs"\
            f" WHERE ikey=? AND docs.iuid=index_{name}.iuid"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            cursor = self.cnx.execute(sql, (key,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return [row[0] for row in cursor]

    def range(self, name, lowkey, highkey, limit=None, offset=None):
        """Return a generator of iuds for all documents having
        a key in the named index within the given inclusive range.
        """
        sql = f"SELECT iuid FROM index_{name}"\
            f" WHERE ?<=ikey AND ikey<=? ORDER BY ikey"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            cursor = self.cnx.execute(sql, (lowkey, highkey))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return (row[0] for row in cursor)

    def range_docs(self, name, lowkey, highkey, limit=None, offset=None):
        """Return a generator of all documents having a key
        in the named index within the given inclusive range.
        """
        sql = f"SELECT docs.doc FROM index_{name}, docs"\
            f" WHERE ?<=ikey AND ikey<=? AND docs.iuid=index_{name}.iuid" \
            f" ORDER BY index_{name}.ikey"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            cursor = self.cnx.execute(sql, (lowkey, highkey))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")
        return (row[0] for row in cursor)

    def close(self):
        "Close the connection."
        try:
            self.cnx.close()
            del self.cnx
        except AttributeError:
            pass

    def _add_to_indexes(self, iuid, doc):
        """Add the document with the given iuid to the indexes.
        This operation must be performed within a transaction.
        """
        sql = "SELECT name, path FROM indexes"
        cursor = self.cnx.execute(sql)
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
        sql = "SELECT indexes.name FROM indexes, docs WHERE docs.iuid=?"
        cursor = self.cnx.execute(sql, (iuid,))
        for (name,) in cursor:
            self.cnx.execute(f"DELETE FROM index_{name} WHERE iuid=?", (iuid,))


class IuidIterator:
    "Iterate over all iuids in the database."

    CHUNK_SIZE = 100

    def __init__(self, db):
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
            sql = f"SELECT iuid FROM docs LIMIT {self.CHUNK_SIZE}"
            if self.offset is not None:
                sql += f" OFFSET {self.offset}"
            self.cursor.execute(sql)
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
    "Iterate over documents in the database."

    def __init__(self, db):
        self.db = db
        self.iuiditerator = IuidIterator(self.db)

    def __iter__(self):
        return self

    def __next__(self):
        return self.db.get(next(self.iuiditerator))


@click.group()
@click.argument("dbpath")
@click.option("-C", "--create", is_flag=True, help="Create the database file.")
@click.pass_context
def cli(ctx, dbpath, create):
    """YasonDB command-line interface. DBPATH is the path to the database file
    (required).
    """
    if create:
        try:
            ctx.obj = YasonDB(dbpath, create=True)
        except IOError as error:
            raise click.ClickException("file already exists")
    else:
        try:
            ctx.obj = YasonDB(dbpath, create=False)
        except IOError as error:
            raise click.ClickException("file does not exist")
        except ValueError:
            raise click.ClickException("file is not a valid YasonDB database")
        
@cli.command()
@click.option("-i", "--indexes", is_flag=True, help="Output index definitions.")
@click.option("-a", "--all", is_flag=True,
              help="Output all data, including the JSON documents.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.pass_obj
def dump(db, indexes, all, indent):
    "Write out data from the database; summary, indexes or all data."
    result = {"n_documents": len(db),
              "n_indexes": len(db.get_indexes())}
    if indexes or all:
        result["indexes"] = {}
        names = db.get_indexes()
        for name in names:
            result["indexes"][name] = db.get_index(name)
    if all:
        result["docs"] = docs = {}
        for iuid in db:
            docs[iuid] = db[iuid]
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("dumpfile", type=click.File("r"))
@click.option("--handle",
              type=click.Choice(["add", "check", "update", "skip"]),
              default="add",
              help="Handle conflicts (i.e. iuid already in database):"
              " 'add': Add documents, after checking for conflicts."
              " 'check': Check for conflicts, do not actually add anything."
              " 'update': Update documents with existing iuids, add all others."
              " 'skip': Skip any documents with same iuid, add all others.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.pass_obj
def load(db, dumpfile, handle, indent):
    """Load the documents from a dump file, allowing different handling
    of conflicts with existing iuid's in the database.
    """
    docs = json.load(dumpfile).get("docs") or {}
    result = {"documents": len(docs)}
    if handle == "add":
        try:
            for iuid, doc in docs.items():
                db.add(doc, iuid=iuid)
        except KeyError as error:
            raise click.ClickException("Conflict(s) between the dump file"
                                       " and the database.")
        else:
            result["added"] = len(docs)
    elif handle == "check":
        conflicts = {}
        for iuid, doc in docs.items():
            if iuid in db:
                conflicts[iuid] = doc
        result["conflicts"] = len(conflicts)
        result["docs"] = conflicts
    elif handle == "update":
        for iuid, doc in docs.items():
            db.update(iuid, doc, add=True)
        result["updated"] = len(docs)
    elif handle == "skip":
        skipped = {}
        for iuid, doc in docs.items():
            try:
                db.add(doc, iuid=iuid)
            except KeyError:
                skipped[iuid] = doc
        result["skipped"] = len(skipped)
        result["docs"] = docs
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("iuid")
@click.argument("doc", type=click.File("r"))
@click.pass_obj
def add(db, iuid, doc):
    "Add the given JSON document into the database with the given iuid."
    try:
        db.add(json.loads(doc.read()), iuid=iuid)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("iuid")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.pass_obj
def get(db, iuid, indent):
    "Print the JSON document given its iuid."
    try:
        click.echo(_json_str(db[iuid], indent=indent))
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("iuid")
@click.argument("doc", type=click.File("r"))
@click.option("-a", "--add", is_flag=True,
              help="Add the document if the iuid does not already exist.")
@click.pass_obj
def update(db, iuid, doc, add):
    "Update the given JSON document in the database with the given iuid."
    try:
        db.update(iuid, json.loads(doc.read()), add=add)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("iuid")
@click.pass_obj
def delete(db, iuid):
    "Delete the JSON document given its iuid."
    try:
        db.delete(iuid)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("name")
@click.option("-c", "--create", is_flag=True, help="Create the named index.")
@click.option("-p", "--path", help="Path for the index to create.")
@click.option("-D", "--delete", is_flag=True, help="Delete the named index.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.pass_obj
def index(db, name, indent, create, path, delete):
    "Show, create or delete index definition."
    if create:
        db.create_index(name, path)
    elif delete:
        db.delete_index(name)
    else:
        try:
            click.echo(_json_str(db.get_index(name), indent))
        except KeyError as error:
            raise click.ClickException(error)

@cli.command()
@click.argument("name")
@click.argument("key")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.option("--docs", is_flag=True, 
              help="Return the list of docs rather than iuds.")
@click.pass_obj
def find(db, name, key, limit, offset, docs, indent):
    "Find the iuids or documents in the given index with the given key."
    try:
        key = int(key)
    except ValueError:
        pass
    result = {"index": name,
              "key": key}
    try:
        iuids = db.find(name, key, limit=limit, offset=offset)
        result["count"] = len(iuids)
        if docs:
            docs = db.find_docs(name, key, limit=limit, offset=offset)
            result["docs"] = dict(zip(iuids, docs))
        else:
            result["iuids"] = iuids
    except KeyError as error:
        raise click.ClickException(error)
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("name")
@click.argument("lowkey")
@click.argument("highkey")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
@click.option("--docs", is_flag=True, 
              help="Return the list of docs rather than iuds.")
@click.pass_obj
def range(db, name, lowkey, highkey, limit, offset, docs, indent):
    """Find the iuids or documents in the given index within
    the given inclusive range.
    """
    try:
        lowkey = int(lowkey)
    except ValueError:
        pass
    try:
        highkey = int(highkey)
    except ValueError:
        pass
    result = {"index": name,
              "lowkey": lowkey,
              "highkey": highkey}
    try:
        iuids = list(db.range(name, lowkey, highkey,
                              limit=limit, offset=offset))
        result["count"] = len(iuids)
        if docs:
            docs = list(db.range_docs(name, lowkey, highkey,
                                      limit=limit, offset=offset))
            result["docs"] = dict(zip(iuids, docs))
        else:
            result["iuids"] = iuids
    except KeyError as error:
        raise click.ClickException(error)
    click.echo(_json_str(result, indent))

if __name__ == "__main__":
    cli()
