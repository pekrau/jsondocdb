"""Yet another JSON document database, with indexes and transactions.
Python using Sqlite3 and JSONPath.
"""

import json
import os.path
import re
import sqlite3
import uuid
from typing import Any, Optional, List, Union

import click
from jsonpath_ng import JSONPathError
from jsonpath_ng.ext import parse as jsonpathparse

__version__ = "0.6.3"

_INDEXNAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    return json.loads(data)

def _jsondoc_adapter(jsondoc):
    return json.dumps(jsondoc, ensure_ascii=False)

sqlite3.register_converter("JSONDOC", _jsondoc_converter)
sqlite3.register_adapter(dict, _jsondoc_adapter)

def _json_str(doc, indent):
    if isinstance(indent, int) and indent <= 0: indent = None
    return json.dumps(doc, indent=indent, ensure_ascii=False)


class Database:
    "Yet another JSON document database, with indexes and transactions."

    def __init__(self, dbfilepath: str, create: bool=False):
        """Connect to the YasonDB database file given by the dbfilepath.
        The special dbfilepath ':memory' indicates an in-memory database.

        'create':
          - False: The database file must exist, and must be a YasonDB database.
          - True: Create and initialize the file. It must not exist.

        Raises:
        - IOError: The file exists when it shouldn't, and vice versa,
          depending on `create`.
        - ValueError: Could not initialize the YasonDB database.
        - YasonDB.InvalidDatabaseError: The database file is not a YasonDB file.
        """
        if create:
            if os.path.exists(dbfilepath):
                raise IOError(f"File '{dbfilepath}' already exists.")
            self._connect(dbfilepath)
            try:
                self.cnx.execute("CREATE TABLE docs"
                                 " (id TEXT PRIMARY KEY,"
                                 "  doc JSONDOC NOT NULL)")
                self.cnx.execute("CREATE TABLE indexes"
                                 " (indexname TEXT PRIMARY KEY,"
                                 "  jsonpath TEXT NOT NULL)")
            except sqlite3.Error:
                raise ValueError("Could not initialize the YasonDB database.")
        else:
            if not os.path.exists(dbfilepath):
                raise IOError(f"File '{dbfilepath}' does not exist.")
            self._connect(dbfilepath)
            try:
                self.cnx.execute("SELECT COUNT(*) FROM docs")
                self.cnx.execute("SELECT COUNT(*) FROM indexes")
            except sqlite3.Error:
                raise InvalidDatabaseError("The database file is not a YasonDB file.")
        self._index_cache = {}  # key: jsonpath; value: parsed jsonpath

    def _connect(self, dbfilepath: str) -> Any:
        "Open the Sqlite3 connection."
        self.cnx = sqlite3.connect(dbfilepath,
                                   detect_types=sqlite3.PARSE_DECLTYPES,
                                   isolation_level="DEFERRED")

    def __str__(self) -> str:
        "Return a string with info on number of documents and indexes."
        return f"Database has {len(self)} documents, {len(self.get_indexes())} indexes."

    def __iter__(self):
        "Return a generator over id's for all documents in the database."
        sql = "SELECT id FROM docs ORDER BY id"
        return (row[0] for row in self.cnx.execute(sql))

    def __len__(self) -> int:
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __del__(self):
        "Close the database connection."
        self.close()

    def __getitem__(self, id) -> dict:
        """Return the document with the given id.
        Raises:
        - YasonDb.NotInTransaction
        """
        cursor = self.cnx.execute("SELECT doc FROM docs WHERE id=?", (id,))
        row = cursor.fetchone()
        if not row:
            raise KeyError(f"No such document '{id}'.")
        return row[0]

    def __setitem__(self, id: str, doc: dict):
        "Add or update the document in the database with the given id."
        self.update(id, doc, add=True)

    def __delitem__(self, id: str):
        """Delete the document with the given id from the database.

        Raises:
        - KeyError: No such document id.
        - YasonDb.NotInTransaction
        """
        self.delete(id)

    def __contains__(self, id: str):
        "Return `True` if the given id is in the database, else `False`."
        sql = "SELECT COUNT(*) FROM docs WHERE id=?"
        cursor = self.cnx.execute(sql, (id,))
        return bool(cursor.fetchone()[0])

    def __enter__(self):
        """A context manager for a transaction. All operations that modify
        the data must occur within a transaction.
        If all goes well, the transaction is committed.
        If an error occurs within the block, the transaction is rolled back.
        """
        self.begin()

    def __exit__(self, type, value, tb):
        "End a transaction; commit if OK, rollback if not."
        if type is None:
            self.commit()
        else:
            self.rollback()
        return False

    @property
    def in_transaction(self):
        "Are we within a transaction?"
        return self.cnx.in_transaction

    def begin(self):
        "Start a transaction. Use the context manager instead."
        if self.in_transaction:
            raise InTransactionError
        self.cnx.execute("BEGIN")

    def commit(self):
        """End the transaction, storing the modifications.
        Use the context manager instead.
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self.cnx.execute("COMMIT")

    def rollback(self):
        """End the transaction, discaring the modifications.
        Use the context manager instead.
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self.cnx.execute("ROLLBACK")

    def get(self, id: str, default: Optional[dict]=None):
        "Retrieve the document given its id, else the default."
        try:
            return self[id]
        except KeyError:
            return default

    def add(self, doc: dict, id: Optional[str]=None) -> str:
        """Add the document to the database.
        If 'id' is not provided, create a UUID4 id.
        Return the id.

        Raises:
        - ValueError: If doc is not a dictionary.
        - KeyError: If the id already exists in the database.
        - NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        if not id:
            id = uuid.uuid4().hex
        try:
            sql = "INSERT INTO docs (id, doc) VALUES (?, ?)"
            self.cnx.execute(sql, (id, doc))
        except sqlite3.DatabaseError:
            raise KeyError(f"The id '{id}' already exists.")
        self._add_to_indexes(id, doc)
        return id

    def update(self, id: str, doc: dict, add: bool=False):
        """Update the document with the given id.

        Raises:
        - ValueError: If the document is not a dictionary.
        - KeyError: If no such id in the database and 'add' is False.
        - NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        sql = "UPDATE docs SET doc=? WHERE id=?"
        cursor = self.cnx.execute(sql, (doc, id))
        if cursor.rowcount == 1: # Actually updated.
            self._remove_from_indexes(id)
            self._add_to_indexes(id, doc)
        elif add:
            self.add(doc, id=id)
        else:
            raise KeyError(f"No such document '{id}' to update.")

    def delete(self, id: str):
        """Delete the document with the given id from the database.

        Raises:
        - KeyError: No such document id.
        - YasonDB.NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self._remove_from_indexes(id)
        cursor = self.cnx.execute("DELETE FROM docs WHERE id=?", (id,))
        if cursor.rowcount == 0:
            raise KeyError(f"No such document '{id}' to delete.")

    def index_exists(self, indexname: str) -> bool:
        "Does the named index exist?"
        try:
            self.get_index(indexname)
            return True
        except KeyError:
            return False

    def create_index(self, indexname: str, jsonpath: str):
        """Create an index for a given JSON path.

        Raises:
        - ValueError: If the indexname is invalid or already in use.
        - NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not _INDEXNAME_RX.match(indexname):
            raise ValueError(f"Invalid index name '{indexname}'.")
        if self.index_exists(indexname):
            raise ValueError(f"Index '{indexname}' is already defined.")
        try:
            expression = jsonpathparse(jsonpath)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        try:
            sql = "INSERT INTO indexes (indexname, jsonpath) VALUES (?, ?)"
            self.cnx.execute(sql, (indexname, jsonpath))
            sql = f"CREATE TABLE index_{indexname}" \
                " (id TEXT PRIMARY KEY, key NOT NULL)"
            self.cnx.execute(sql)
            sql = f"CREATE INDEX index_{indexname}_ix ON index_{indexname} (key)"
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{indexname}': {error}")
        self._index_cache[indexname] = expression
        sql = "SELECT id, doc FROM docs"
        cursor = self.cnx.execute(sql)
        sql = f"INSERT INTO index_{indexname} (id, key) VALUES(?, ?)"
        for id, doc in cursor:
            for match in expression.find(doc):
                self.cnx.execute(sql, (id, match.value))

    def get_indexes(self):
        "Return the list names for the current indexes."
        sql = "SELECT indexname FROM indexes"
        return [indexname for (indexname,) in self.cnx.execute(sql)]

    def get_index(self, indexname: str) -> dict:
        """Return definition and statistics for the named index.

        Raises:
        - KeyError: If there is no such index.
        """
        try:
            sql = "SELECT jsonpath FROM indexes WHERE indexname=?"
            cursor = self.cnx.execute(sql, (indexname,))
            row = cursor.fetchone()
            if not row:
                raise KeyError(f"No such index '{indexname}'.")
            result = {"jsonpath": row[0]}
            cursor = self.cnx.execute(f"SELECT COUNT(*) FROM index_{indexname}")
            result["count"] = cursor.fetchone()[0]
        except (ValueError, sqlite3.Error):
            raise KeyError(f"No such index '{indexname}'.")
        if result["count"] > 0:
            cursor = self.cnx.execute(f"SELECT MIN(key) FROM index_{indexname}")
            result["min"] = cursor.fetchone()[0]
            cursor = self.cnx.execute(f"SELECT MAX(key) FROM index_{indexname}")
            result["max"] = cursor.fetchone()[0]
        return result

    def get_index_keys(self, indexname: str):
        """Return a generator to provide all tuples (id, key) in the index.

        Raises:
        - KeyError: If there is no such index.
        """
        try:
            cursor = self.cnx.execute(f"SELECT id, key FROM index_{indexname}")
            return (row for row in cursor)
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")

    def in_index(self, indexname: str, id: str) -> bool:
        "Is the given id in the named index?"
        try:
            sql = f"SELECT COUNT(*) FROM index_{indexname} WHERE id=?"
            cursor = self.cnx.execute(sql, (id,))
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")
        return bool(cursor.fetchone()[0])

    def delete_index(self, indexname: str):
        """Delete the named index.

        Raises:
        - KeyError: If there is no such index.
        - NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not self.get_index(indexname):
            raise ValueError(f"No such index '{indexname}'.")
        self.cnx.execute("DELETE FROM indexes WHERE indexname=?", (indexname,))
        self.cnx.execute(f"DROP TABLE index_{indexname}")
        self._index_cache.pop(indexname, None)

    def find(self, indexname: str, key:str,
             limit: Optional[int]=None,
             offset: Optional[int]=None) -> List[str]:
        """Return a list of all ids for the documents having
        the given key in the named index.

        Raises:
        - KeyError: If there is no such index.
        """
        sql = f"SELECT docs.id FROM index_{indexname}, docs" \
            f" WHERE key=? AND docs.id=index_{indexname}.id"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            return [row[0] for row in self.cnx.execute(sql, (key,))]
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")

    def range(self, indexname: str, lowkey: str, highkey: str, 
              limit: Optional[int]=None, offset: Optional[int]=None) -> Any:
        """Return a generator over all ids for the documents having 
        a key in the named index within the given inclusive range.

        Raises:
        - KeyError: If there is no such index.
        """
        sql = f"SELECT docs.id, docs.doc FROM index_{indexname}, docs"\
            f" WHERE ?<=key AND key<=? AND docs.id=index_{indexname}.id" \
            f" ORDER BY index_{indexname}.key"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            return (row[0] for row in self.cnx.execute(sql, (lowkey, highkey)))
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")

    def backup(self, dbfilepath):
        """Backup this database safely into a new file at the given path.

        Raises:
        - IOError: If a file already exists at the new path.
        - YasonDB.InTransactionError
        """
        if self.in_transaction:
            raise InTransactionError
        if os.path.exists(dbfilepath):
            raise IOError(f"File '{dbfilepath}' already exists.")
        bck = sqlite3.connect(dbfilepath,
                              detect_types=sqlite3.PARSE_DECLTYPES)
        with bck:
            self.cnx.backup(bck)
        bck.close()

    def close(self):
        "Close the connection to the Sqlite3 database."
        try:
            self.cnx.close()
            del self.cnx
        except AttributeError:
            pass

    def _add_to_indexes(self, id: str, doc: dict):
        "Add the document with the given id to the indexes."
        sql = "SELECT indexname, jsonpath FROM indexes"
        cursor = self.cnx.execute(sql)
        for indexname, jsonpath in cursor:
            try:
                expression = self._index_cache[indexname]
            except KeyError:
                expression = jsonpathparse(jsonpath)
                self._index_cache[indexname] = expression
            sql = f"INSERT INTO index_{indexname} (id, key) VALUES(?, ?)"
            for match in expression.find(doc):
                self.cnx.execute(sql, (id, match.value))

    def _remove_from_indexes(self, id: str):
        "Remove the document with the given id from the indexes."
        sql = "SELECT indexes.indexname FROM indexes, docs WHERE docs.id=?"
        cursor = self.cnx.execute(sql, (id,))
        for (indexname,) in cursor:
            self.cnx.execute(f"DELETE FROM index_{indexname} WHERE id=?", (id,))


class BaseError(Exception):
    "Base class for YasonDB-specific errors."
    pass

class InvalidDatabaseError(BaseError):
    "The file is not a valid YasonDB database."
    pass

class InTransactionError(BaseError):
    "Attempt to begin a transaction when already within one."
    pass

class NotInTransactionError(BaseError):
    "Attempted operation requires being in a transaction."
    pass


@click.group()
def cli():
    "YasonDB command-line interface."
    pass

@cli.command()
@click.argument("dbfilepath", type=click.Path(writable=True, dir_okay=False))
def create(dbfilepath):
    "Create a YasonDB database at DBFILEPATH."
    if os.path.exists(dbfilepath):
        raise click.BadParameter(f"File {dbfilepath} already exists.")
    try:
        Database(dbfilepath, create=True)
    except IOError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(writable=True, dir_okay=False))
def check(dbfilepath):
    "Check that DBFILEPATH refers to a readable YasonDB file."
    try:
        db = Database(dbfilepath)
    except (IOError, InvalidDatabaseError) as error:
        raise click.ClickException(error)
    click.echo(str(db))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def dump(dbfilepath, indent):
    "Write out all JSON documents from the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    result = {"n_documents": len(db),
              "docs": dict([(id, db[id]) for id in db])}
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("dumpfile", type=click.File("r"))
@click.option("--handle",
              type=click.Choice(["add", "check", "update", "skip"]),
              default="add",
              help="Handle conflicts (i.e. id already in database):"
              " 'add': Add documents, after checking for no conflicts."
              " 'check': Check for conflicts, do not actually add anything."
              " 'update': Update documents with existing ids, add all others."
              " 'skip': Skip any documents with same id, add all others.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def load(dbfilepath, dumpfile, handle, indent):
    """Load the documents (not the indexes) from a file produced by 'dump',
    allowing handling of conflicts with existing id's in the database.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    docs = json.load(dumpfile).get("docs") or {}
    result = {"documents": len(docs)}
    if handle == "add":
        with db:
            try:
                for id, doc in docs.items():
                    db.add(doc, id=id)
            except KeyError as error:
                raise click.ClickException("Conflict(s) between the dump file"
                                           " and the database.")
            else:
                result["added"] = len(docs)
    elif handle == "check":
        conflicts = {}
        with db:
            for id, doc in docs.items():
                if id in db:
                    conflicts[id] = doc
        result["conflicts"] = len(conflicts)
        result["docs"] = conflicts
    elif handle == "update":
        with db:
            for id, doc in docs.items():
                db.update(id, doc, add=True)
        result["updated"] = len(docs)
    elif handle == "skip":
        skipped = {}
        with db:
            for id, doc in docs.items():
                try:
                    db.add(doc, id=id)
                except KeyError:
                    skipped[id] = doc
        result["skipped"] = len(skipped)
        result["docs"] = docs
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("id")
@click.argument("docfile", type=click.File("r"))
def add(dbfilepath, id, docfile):
    "Add the given JSON document with the given id into the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.add(json.loads(docfile.read()), id=id)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("id")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def get(dbfilepath, id, indent):
    "Print the JSON document given its id in the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        click.ClickException(error)
    try:
        click.echo(_json_str(db[id], indent=indent))
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("id")
@click.argument("docfile", type=click.File("r"))
@click.option("-a", "--add", is_flag=True,
              help="Add the document if the id does not already exist.")
def update(dbfilepath, id, docfile, add):
    """Update the given JSON document in the database at DBFILEPATH by
    the JSON document at DOCFILE.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.update(id, json.loads(docfile.read()), add=add)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("id")
def delete(dbfilepath, id):
    "Delete the JSON document given by its id from the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.delete(id)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
@click.option("--keys", is_flag=True, 
              help="List the contents of the named index.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def index(dbfilepath, indexname, keys, indent):
    "Show the index definition and keys in the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        doc = db.get_index(indexname)
        if keys:
            doc["keys"] = list(db.get_index_keys(indexname))
        click.echo(_json_str(doc, indent))
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def indexes(dbfilepath, indent):
    "List the current indexes in the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    result = {"indexes": {}}
    for indexname in db.get_indexes():
        result["indexes"][indexname] = db.get_index(indexname)
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
@click.argument("jsonpath")
def index_create(dbfilepath, indexname, jsonpath):
    """Create an index INDEXNAME with the given JSON path
    in the database at DBFILEPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    with db:
        try:
            db.create_index(indexname, jsonpath)
        except (KeyError, ValueError) as error:
            raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
def index_delete(dbfilepath, indexname):
    "Delete the index INDEXNAME in the database at DBFILEPATH."
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    with db:
        try:
            db.delete_index(indexname)
        except KeyError as error:
            raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
@click.argument("key")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def find(dbfilepath, indexname, key, limit, offset, indent):
    """Print the ids and documents in the index INDEXNAME with the given KEY
    in the database at DBFILEPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        key = int(key)
    except ValueError:
        pass
    try:
        ids = db.find(indexname, key, limit=limit, offset=offset)
    except KeyError as error:
        raise click.ClickException(error)
    result = {"index": indexname,
              "key": key,
              "count": len(ids),
              "docs": dict([(id, db[id]) for id in ids])}
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
@click.argument("lowkey")
@click.argument("highkey")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def range(dbfilepath, indexname, lowkey, highkey, limit, offset, indent):
    """Print the ids and documents in the index INDEXNAME within
    the given inclusive range in the database at DBFILEPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        lowkey = int(lowkey)
    except ValueError:
        pass
    try:
        highkey = int(highkey)
    except ValueError:
        pass
    try:
        ids = list(db.range(indexname, lowkey, highkey, limit=limit, offset=offset))
    except KeyError as error:
        raise click.ClickException(error)
    result = {"index": indexname,
              "lowkey": lowkey,
              "highkey": highkey,
              "count": len(ids),
              "docs": dict([(id, db[id]) for id in ids])}
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("backupfilepath", type=click.Path(writable=True, dir_okay=False))
def backup(dbfilepath, backupfilepath):
    "Backup safely the current database into a new file at BACKUPFILEPATH"
    try:
        db = Database(dbfilepath)
        db.backup(backupfilepath)
    except IOError as error:
        raise click.ClickException(error)


if __name__ == "__main__":
    cli()
