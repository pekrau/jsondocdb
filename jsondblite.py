"""JSON document database in a file, with indexes and transactions.
Built on Sqlite3 and JSONPath in Python.
"""

import json
import os.path
import re
import sqlite3
import uuid
from typing import Any, Optional, List, Dict, Tuple, Union, Iterable

import click
from jsonpath_ng import JSONPathError # type: ignore
from jsonpath_ng.ext import parse as jsonpathparse # type: ignore

__version__ = "0.7.6"

_INDEXNAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    return json.loads(data)

def _jsondoc_adapter(jsondoc):
    return json.dumps(jsondoc, ensure_ascii=False)

sqlite3.register_converter("JSONDOC", _jsondoc_converter)
sqlite3.register_adapter(dict, _jsondoc_adapter)

def _json_str(doc: dict, indent: Optional[int]=None):
    if isinstance(indent, int) and indent <= 0: indent = None
    return json.dumps(doc, indent=indent, ensure_ascii=False)


class Database:
    "JSON document database, with indexes and transactions."

    def __init__(self, dbfilepath: str, create: bool=False):
        """Connect to the jsondblite database file given by the dbfilepath.
        The special dbfilepath ':memory' indicates an in-memory database.

        'create':
          - False: The database file must exist, and must be a jsondblite database.
          - True: Create and initialize the file. It must not exist.

        Raises:
        - IOError: The file exists when it shouldn't, and vice versa,
          depending on `create`.
        - ValueError: Could not initialize the jsondblite database.
        - jsondblite.InvalidDatabaseError: The file is not a jsondblite file.
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
                raise ValueError("Could not initialize the jsondblite database.")
        else:
            if not os.path.exists(dbfilepath):
                raise IOError(f"File '{dbfilepath}' does not exist.")
            self._connect(dbfilepath)
            try:
                self.cnx.execute("SELECT COUNT(*) FROM docs")
                self.cnx.execute("SELECT COUNT(*) FROM indexes")
            except sqlite3.Error:
                raise InvalidDatabaseError("The database file is not a jsondblite file.")
        # key: jsonpath; value: parsed jsonpath
        self._index_cache = {} # type: Dict[str, Any]

    def _connect(self, dbfilepath: str):
        "Open the Sqlite3 connection."
        self.cnx = sqlite3.connect(dbfilepath,
                                   detect_types=sqlite3.PARSE_DECLTYPES,
                                   isolation_level="DEFERRED")

    def __str__(self) -> str:
        "Return a string with info on number of documents and indexes."
        return f"Database has {len(self)} documents, {len(self.get_indexes())} indexes."

    def __iter__(self) -> Iterable[dict]:
        "Return an iterator over ids for all documents in the database."
        sql = "SELECT id FROM docs ORDER BY id"
        return (row[0] for row in self.cnx.execute(sql))

    def __len__(self) -> int:
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __del__(self):
        "Close the database connection."
        self.close()

    def __getitem__(self, id: Union[str, int]) -> dict:
        """Return the document with the given id.

        Raises:
        - jsondblite.NotInTransaction
        """
        cursor = self.cnx.execute("SELECT doc FROM docs WHERE id=?", (id,))
        row = cursor.fetchone()
        if not row:
            raise KeyError(f"No such document '{id}'.")
        return row[0]

    def __setitem__(self, id: str, doc: dict):
        "Add or update the document in the database with the given id."
        self.update(id, doc)

    def __delitem__(self, id: str):
        """Delete the document with the given id from the database.

        Raises:
        - KeyError: No such document id.
        - jsondblite.NotInTransaction
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
    def in_transaction(self) -> bool:
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

        Raises:
        - jsondblite.NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self.cnx.execute("COMMIT")

    def rollback(self):
        """End the transaction, discaring the modifications.
        Use the context manager instead.

        Raises:
        - jsondblite.NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self.cnx.execute("ROLLBACK")

    def get(self, id: str, default: Optional[dict]=None) -> Optional[dict]:
        "Retrieve the document given its id, else the default."
        try:
            return self[id]
        except KeyError:
            return default

    def add(self, doc: dict, id: Optional[str]=None) -> str:
        """Add the document to the database.
        If id is not provided, create a UUID4 id.
        Return the id actually used.

        Raises:
        - ValueError: If doc is not a dictionary.
        - KeyError: If the id already exists in the database.
        - jsondblite.NotInTransaction
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

    def update(self, id: str, doc: dict):
        """Update the document with the given id.
        If the id is not in the database, then add the doc.

        Raises:
        - ValueError: If the document is not a dictionary.
        - jsondblite.NotInTransaction
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
        else:                   # Actually add.
            self.add(doc, id=id)

    def delete(self, id: str):
        """Delete the document with the given id from the database.

        Raises:
        - KeyError: No such document id.
        - jsondblite.NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        self._remove_from_indexes(id)
        cursor = self.cnx.execute("DELETE FROM docs WHERE id=?", (id,))
        if cursor.rowcount == 0:
            raise KeyError(f"No such document '{id}' to delete.")

    def have_jsonpath(self, jsonpath: str) -> Iterable[str]:
        """Return an iterator providing ids of all documents
        matching the given JSON path.

        Raises:
        - ValueError: Invalid JSON path.
        """
        try:
            expression = jsonpathparse(jsonpath)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        cursor = self.cnx.execute("SELECT id, doc FROM docs")
        return (id for id, doc in cursor if expression.find(doc))

    def lack_jsonpath(self, jsonpath: str) -> Iterable[str]:
        """Return an iterator providing ids of all documents 
        not matching the given JSON path.

        Raises:
        - ValueError: Invalid JSON path.
        """
        try:
            expression = jsonpathparse(jsonpath)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        cursor = self.cnx.execute("SELECT id, doc FROM docs")
        return (id for id, doc in cursor if not expression.find(doc))

    def search(self, jsonpath: str, value: Union[str, int]) -> List[Tuple[str, dict]]:
        """Return a list of tuple(id, doc) for all documents that have
        the given value at the given JSON path.

        Raises:
        - ValueError: Invalid JSON path.
        """
        try:
            expression = jsonpathparse(jsonpath)
        except JSONPathError as error:
            raise ValueError(f"Invalid JSON path: {error}")
        result = []
        cursor = self.cnx.execute("SELECT id, doc FROM docs")
        for id, doc in cursor:
            for match in expression.find(doc):
                if match.value == value:
                    result.append((id, doc))
                    break
        return result

    def index_exists(self, indexname: str) -> bool:
        "Does the named index exist?"
        try:
            self.get_index(indexname)
            return True
        except KeyError:
            return False

    def create_index(self, indexname: str, jsonpath: str):
        """Create an index for a given JSON path. If the JSON path
        produces something other than a str or an int for a document,
        then that match is not entered into the index.

        Raises:
        - ValueError: The indexname is invalid or already in use, or
          the given JSON path is invalid.
        - jsondblite.NotInTransaction
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
                " (id TEXT PRIMARY KEY, value NOT NULL)"
            self.cnx.execute(sql)
            sql = f"CREATE INDEX index_{indexname}_ix ON index_{indexname} (value)"
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{indexname}': {error}")
        self._index_cache[indexname] = expression
        sql = "SELECT id, doc FROM docs"
        cursor = self.cnx.execute(sql)
        sql = f"INSERT INTO index_{indexname} (id, value) VALUES(?, ?)"
        for id, doc in cursor:
            for match in expression.find(doc):
                self.cnx.execute(sql, (id, match.value))

    def get_indexes(self) -> List[str]:
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
            cursor = self.cnx.execute(f"SELECT MIN(value) FROM index_{indexname}")
            result["min"] = cursor.fetchone()[0]
            cursor = self.cnx.execute(f"SELECT MAX(value) FROM index_{indexname}")
            result["max"] = cursor.fetchone()[0]
        return result

    def get_index_values(self, indexname: str) -> Iterable[Tuple[str, int]]:
        """Return an iterator to provide all tuples (id, value) in the index.

        Raises:
        - KeyError: If there is no such index.
        """
        try:
            sql = f"SELECT id, value FROM index_{indexname}"
            cursor = self.cnx.execute(sql)
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")
        return (row for row in cursor)

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
        - jsondblite.NotInTransaction
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not self.get_index(indexname):
            raise ValueError(f"No such index '{indexname}'.")
        self.cnx.execute("DELETE FROM indexes WHERE indexname=?", (indexname,))
        self.cnx.execute(f"DROP TABLE index_{indexname}")
        self._index_cache.pop(indexname, None)

    def lookup(self, indexname: str, value):
        """Return a list of all ids for the documents having
        the given value in the named index.

        Raises:
        - ValueError: The value cannot be None, since not in the index.
        - KeyError: If there is no such index.
        """
        if value is None:
            raise ValueError("Value cannot be None; not in index.")
        sql = f"SELECT docs.id FROM index_{indexname}, docs" \
            f" WHERE index_{indexname}.value=? AND docs.id=index_{indexname}.id"
        try:
            return [row[0] for row in self.cnx.execute(sql, (value,))]
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")

    def range(self, indexname: str,
              low: Union[str, int], 
              high: Union[str, int],
              limit: Optional[int]=None, 
              offset: Optional[int]=None) -> Iterable[str]:
        """Return an iterator over all ids for the documents having 
        a value in the named index within the given inclusive range.

        Raises:
        - ValueError: The types of 'low' and 'high' are not the same.
        - KeyError: If there is no such index.
        """
        if type(low) != type(high): 
            raise ValueError("Values are not of the same type.")
        sql = f"SELECT docs.id, docs.doc FROM index_{indexname}, docs"\
            f" WHERE ?<=value AND value<=? AND docs.id=index_{indexname}.id" \
            f" ORDER BY index_{indexname}.value"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            return (row[0] for row in self.cnx.execute(sql, (low, high)))
        except sqlite3.Error:
            raise KeyError(f"No such index '{indexname}'.")

    def backup(self, dbfilepath: str):
        """Backup this database safely into a new file at the given path.

        Raises:
        - IOError: If a file already exists at the new path.
        - jsondblite.InTransactionError
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
            sql = f"INSERT INTO index_{indexname} (id, value) VALUES(?, ?)"
            for match in expression.find(doc):
                if isinstance(match.value, (str, int)):
                    self.cnx.execute(sql, (id, match.value))

    def _remove_from_indexes(self, id: str):
        "Remove the document with the given id from the indexes."
        sql = "SELECT indexes.indexname FROM indexes, docs WHERE docs.id=?"
        cursor = self.cnx.execute(sql, (id,))
        for (indexname,) in cursor:
            self.cnx.execute(f"DELETE FROM index_{indexname} WHERE id=?", (id,))


class BaseError(Exception):
    "Base class for jsondblite-specific errors."
    pass

class InvalidDatabaseError(BaseError):
    "The file is not a valid jsondblite database."
    pass

class InTransactionError(BaseError):
    "Attempt to begin a transaction when already within one."
    pass

class NotInTransactionError(BaseError):
    "Attempted operation requires being in a transaction."
    pass


@click.group()
def cli():
    "jsondblite command-line interface."
    pass

@cli.command()
@click.argument("dbfilepath", type=click.Path(writable=True, dir_okay=False))
def create(dbfilepath):
    "Create a jsondblite database at DBFILEPATH."
    if os.path.exists(dbfilepath):
        raise click.BadParameter(f"File {dbfilepath} already exists.")
    try:
        Database(dbfilepath, create=True)
    except IOError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfilepath", type=click.Path(writable=True, dir_okay=False))
def check(dbfilepath):
    "Check that DBFILEPATH refers to a readable jsondblite file."
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
    allowing handling of conflicts with existing ids in the database.
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
def update(dbfilepath, id, docfile):
    """Update the given JSON document in the database at DBFILEPATH by
    the JSON document at DOCFILE.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.update(id, json.loads(docfile.read()))
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
@click.argument("jsonpath")
@click.argument("value")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def search(dbfilepath, jsonpath, value, indent):
    """Print a list of tuple(id, doc) for all documents that have
    the given VALUE at the given JSONPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        value = int(value)
    except ValueError:
        pass
    try:
        result = db.search(jsonpath, value)
    except KeyError as error:
        raise click.ClickException(error)
    click.echo(_json_str(result, indent))

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
            doc["values"] = list(db.get_index_values(indexname))
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
@click.argument("value")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def lookup(dbfilepath, indexname, value, indent):
    """Print the ids and documents in the index INDEXNAME
    with the given VALUE in the database at DBFILEPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        value = int(value)
    except ValueError:
        pass
    try:
        ids = db.lookup(indexname, value)
    except ValueError as error:
        raise click.ClickException(error)
    result = {"index": indexname,
              "value": value,
              "count": len(ids),
              "docs": dict([(id, db[id]) for id in ids])}
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfilepath", type=click.Path(exists=True, dir_okay=False))
@click.argument("indexname")
@click.argument("low")
@click.argument("high")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def range(dbfilepath, indexname, low, high, limit, offset, indent):
    """Print the ids and documents in the index INDEXNAME within
    the given inclusive range in the database at DBFILEPATH.
    """
    try:
        db = Database(dbfilepath)
    except IOError as error:
        raise click.ClickException(error)
    try:
        low = int(low)
    except ValueError:
        pass
    try:
        high = int(high)
    except ValueError:
        pass
    try:
        ids = list(db.range(indexname, low, high, limit=limit, offset=offset))
    except KeyError as error:
        raise click.ClickException(error)
    result = {"index": indexname,
              "low": low,
              "high": high,
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
