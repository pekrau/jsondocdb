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

__version__ = "0.4.0"

NAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    return json.loads(data)

def _jsondoc_adapter(jsondoc):
    return json.dumps(jsondoc, ensure_ascii=False)

sqlite3.register_converter("JSONDOC", _jsondoc_converter)
sqlite3.register_adapter(dict, _jsondoc_adapter)

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
        self.cnx = sqlite3.connect(path,
                                   detect_types=sqlite3.PARSE_DECLTYPES,
                                   isolation_level="DEFERRED")

    def __str__(self):
        return f"YasonDB: {len(self)} documents, {len(self.get_indexes())} indexes."

    def __iter__(self):
        "Return an iterator over tuples (iuid, doc) for all documents."
        sql = "SELECT iuid, doc FROM docs ORDER BY iuid"
        return iter(self.cnx.execute(sql))

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

    def __enter__(self):
        "Begin a transaction."
        self.cnx.execute("BEGIN")

    def __exit__(self, type, value, tb):
        "End a transaction; commit if successful, rollback if exception."
        if type is None:
            self.cnx.execute("COMMIT")
        else:
            self.cnx.execute("ROLLBACK")
        return False

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
        self._remove_from_indexes(iuid)
        cursor = self.cnx.execute("DELETE FROM docs WHERE iuid=?", (iuid,))
        if cursor.rowcount == 0:
            raise KeyError(f"No such document '{iuid}' to delete.")

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
            sql = "INSERT INTO indexes (name, path) VALUES (?, ?)"
            self.cnx.execute(sql, (name, path))
            sql = f"CREATE TABLE index_{name}" \
                " (iuid TEXT PRIMARY KEY, ikey NOT NULL)"
            self.cnx.execute(sql)
            sql = f"CREATE INDEX index_{name}_ix ON index_{name} (ikey)"
        except sqlite3.Error as error:
            raise ValueError(f"Could not create index '{name}': {error}")
        self._index_cache[name] = expression
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

    def get_index_keys(self, name):
        "Return a generator to provide all tuples (iuid, key) in the index."
        try:
            cursor = self.cnx.execute(f"SELECT iuid, ikey FROM index_{name}")
            return (row for row in cursor)
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")

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
        self.cnx.execute("DELETE FROM indexes WHERE name=?", (name,))
        self.cnx.execute(f"DROP TABLE index_{name}")
        self._index_cache.pop(name, None)

    def find(self, name, key, limit=None, offset=None):
        """Return an iterator over tuples (iuid, doc) for all documents
        having the given key in the named index.
        """
        sql = f"SELECT docs.iuid, docs.doc FROM index_{name}, docs" \
            f" WHERE ikey=? AND docs.iuid=index_{name}.iuid"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            return iter(self.cnx.execute(sql, (key,)))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")

    def range(self, name, lowkey, highkey, limit=None, offset=None):
        """Return an iterator over tuples (iuid, doc) or all documents
        having a key in the named index within the given inclusive range.
        """
        sql = f"SELECT docs.iuid, docs.doc FROM index_{name}, docs"\
            f" WHERE ?<=ikey AND ikey<=? AND docs.iuid=index_{name}.iuid" \
            f" ORDER BY index_{name}.ikey"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
        try:
            return iter(self.cnx.execute(sql, (lowkey, highkey)))
        except sqlite3.Error:
            raise KeyError(f"No such index '{name}'.")

    def backup(self, path):
        "Backup this database in a safe manner into a file given by the path."
        if os.path.exists(path):
            raise IOError(f"File '{path}' already exists.")
        bck = sqlite3.connect(path,
                              detect_types=sqlite3.PARSE_DECLTYPES)
        with bck:
            self.cnx.backup(bck)
        bck.close()

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


@click.group()
def cli():
    "YasonDB command-line interface."
    pass

@cli.command()
@click.argument("dbfile", type=click.Path(writable=True, dir_okay=False))
def create(dbfile):
    "Create a YasonDB file at the path DBFILE."
    if os.path.exists(dbfile):
        raise click.BadParameter(f"File {dbfile} already exists.")
    try:
        YasonDB(dbfile, create=True)
    except IOError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(writable=True, dir_okay=False))
def check(dbfile):
    "Check that the given file path refers to a YasonDB file."
    try:
        db = YasonDB(dbfile)
    except (IOError, ValueError) as error:
        raise click.ClickException(error)
    click.echo(str(db))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def dump(dbfile, indent):
    "Write out all JSON documents from the database."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    result = {"n_documents": len(db),
              "docs": dict(list(db))}
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
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
def load(dbfile, dumpfile, handle, indent):
    """Load the documents (not the indexes) from a dump file, allowing 
    different handling of conflicts with existing iuid's in the database.
    """
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    docs = json.load(dumpfile).get("docs") or {}
    result = {"documents": len(docs)}
    if handle == "add":
        with db:
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
        with db:
            for iuid, doc in docs.items():
                if iuid in db:
                    conflicts[iuid] = doc
        result["conflicts"] = len(conflicts)
        result["docs"] = conflicts
    elif handle == "update":
        with db:
            for iuid, doc in docs.items():
                db.update(iuid, doc, add=True)
        result["updated"] = len(docs)
    elif handle == "skip":
        skipped = {}
        with db:
            for iuid, doc in docs.items():
                try:
                    db.add(doc, iuid=iuid)
                except KeyError:
                    skipped[iuid] = doc
        result["skipped"] = len(skipped)
        result["docs"] = docs
    click.echo(_json_str(result, indent=indent))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("iuid")
@click.argument("doc", type=click.File("r"))
def add(dbfile, iuid, doc):
    "Add the given JSON document with the given iuid into the database."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.add(json.loads(doc.read()), iuid=iuid)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("iuid")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def get(dbfile, iuid, indent):
    "Print the JSON document given its iuid."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        click.ClickException(error)
    try:
        click.echo(_json_str(db[iuid], indent=indent))
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("iuid")
@click.argument("doc", type=click.File("r"))
@click.option("-a", "--add", is_flag=True,
              help="Add the document if the iuid does not already exist.")
def update(dbfile, iuid, doc, add):
    "Update the given JSON document the given iuid."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.update(iuid, json.loads(doc.read()), add=add)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("iuid")
def delete(dbfile, iuid):
    "Delete the JSON document with the given iuid."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    try:
        with db:
            db.delete(iuid)
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("name")
@click.option("--keys", is_flag=True, 
              help="List the contents of the named index.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def index(dbfile, name, keys, indent):
    "Show the index definition and keys."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    try:
        doc = db.get_index(name)
        if keys:
            doc["keys"] = list(db.get_index_keys(name))
        click.echo(_json_str(doc, indent))
    except KeyError as error:
        raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def indexes(dbfile, indent):
    "List the current indexes."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    result = {"indexes": {}}
    for name in db.get_indexes():
        result["indexes"][name] = db.get_index(name)
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("name")
@click.argument("path")
def index_create(dbfile, name, path):
    "Create an index with the given name and JSONPath path."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    with db:
        try:
            db.create_index(name, path)
        except (KeyError, ValueError) as error:
            raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("name")
def index_delete(dbfile, name):
    "Delete the index with the given name."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    with db:
        try:
            db.delete_index(name)
        except KeyError as error:
            raise click.ClickException(error)

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("name")
@click.argument("key")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def find(dbfile, name, key, limit, offset, indent):
    "Find the iuids and documents in the given index with the given key."
    try:
        db = YasonDB(dbfile)
    except IOError as error:
        raise click.ClickException(error)
    try:
        key = int(key)
    except ValueError:
        pass
    try:
        contents = list(db.find(name, key, limit=limit, offset=offset))
    except KeyError as error:
        raise click.ClickException(error)
    result = {"index": name,
              "key": key,
              "count": len(contents),
              "docs": dict(contents)}
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("name")
@click.argument("lowkey")
@click.argument("highkey")
@click.option("-l", "--limit", default=100,
              help="Limit the number of result items.")
@click.option("-o", "--offset", default=None, type=int,
              help="Offset of the list of returned items.")
@click.option("-I", "--indent", default=2,
              help="Pretty-print the resulting JSON document.")
def range(dbfile, name, lowkey, highkey, limit, offset, indent):
    """Find the iuids or documents in the given index within
    the given inclusive range.
    """
    try:
        db = YasonDB(dbfile)
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
        contents = list(db.range(name, lowkey, highkey,
                                 limit=limit, offset=offset))
    except KeyError as error:
        raise click.ClickException(error)
    result = {"index": name,
              "lowkey": lowkey,
              "highkey": highkey,
              "count": len(contents),
              "docs": dict(contents)}
    click.echo(_json_str(result, indent))

@cli.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("backupfile", type=click.Path(writable=True, dir_okay=False))
def backup(dbfile, backupfile):
    """Backup the current database into a backup file given by the path
    BACKUPFILE, in a safe manner.
    """
    try:
        db = YasonDB(dbfile)
        db.backup(backupfile)
    except IOError as error:
        raise click.ClickException(error)


if __name__ == "__main__":
    cli()
