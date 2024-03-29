"""jsondocdb

A Python Sqlite3 database for JSON documents. Simple indexing using JsonLogic.

The JsonLogic class was adapted from https://github.com/nadirizr/json-logic-py
"""

__version__ = "0.9.4"


import functools
import json
import mimetypes
import os.path
import re
import sqlite3


_INDEXNAME_RX = re.compile(r"[a-z][a-z0-9_]*", re.IGNORECASE)


def _jsondoc_converter(data):
    if data is None:
        return None
    else:
        return json.loads(data)


def _jsondoc_adapter(jsondoc):
    if jsondoc is None:
        return None
    else:
        return json.dumps(jsondoc, ensure_ascii=False)


sqlite3.register_converter("JSONDOC", _jsondoc_converter)
sqlite3.register_adapter(dict, _jsondoc_adapter)


class Database:
    "A Python Sqlite3 database for JSON documents. Simple indexing using JsonLogic."

    def __init__(self, filepath=None, readonly=False, **kwargs):
        """If a filepath is given, open or create the database file.

        If the file exists, checks that it has the tables required for jsondocdb.

        If the file is created, creates the tables required for jsondocdb.

        The 'filepath' and any additional keyword arguments are passed  to
        sqlite3.connect, except for 'detect_types', which is hard-wired
        to sqlite3.PARSE_DECLTYPES, and 'isolation_level' which is set to None,
        i.e. explicit transactions.
        """
        if filepath:
            if os.path.exists(filepath):
                self.open(filepath, readonly=readonly, **kwargs)
            elif readonly:
                raise OSError("Cannot create database file for 'readonly' mode.")
            else:
                self.create(filepath, **kwargs)

    def create(self, filepath, **kwargs):
        """Create the database file and initialize it with the required tables.

        The 'filepath' and any additional keyword arguments are passed  to
        sqlite3.connect, except for 'detect_types', which is hard-wired
        to sqlite3.PARSE_DECLTYPES, and 'isolation_level' which is set to None,
        i.e. explicit transactions.

        Creates the required tables.
        """
        if hasattr(self, 'cnx'):
            raise ConnectionError("There is already an open connection.")
        if os.path.exists(filepath):
            raise OSError(f"The file '{filepath}' exists; cannot create it.")

        self.filepath = filepath
        kwargs["detect_types"] = sqlite3.PARSE_DECLTYPES  # For JSONDOC handling.
        kwargs["isolation_level"] = None                  # Use explicit transactions.

        try:
            self.cnx = sqlite3.connect(self.filepath, **kwargs)
        except sqlite3.DatabaseError as error:
            raise InvalidFileError(str(error))
        cursor = self.cnx.cursor()
        cursor.execute(
            "CREATE TABLE documents"
            "(identifier TEXT PRIMARY KEY, document JSONDOC NOT NULL)"
        )
        cursor.execute(
            "CREATE TABLE indexes"
            "(name TEXT PRIMARY KEY,"
            " keypath TEXT NOT NULL,"
            " uniq INTEGER NOT NULL,"  # Avoid conflict with reserved word.
            " require JSONDOC)"  # Allow NULL.
        )
        cursor.execute(
            "CREATE TABLE attachments"
            "(identifier TEXT NOT NULL,"  # Foreign key to documents.identifier
            " name TEXT NOT NULL,"
            " content_type TEXT NOT NULL,"
            " content BLOB NOT NULL)"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX attachments_index ON attachments (identifier, name)"
        )

    def open(self, filepath, readonly=False, **kwargs):
        """Open the existing database file.

        Checks that it has the tables appropriate for jsondocdb.

        The 'filepath' and any additional keyword arguments are passed  to
        sqlite3.connect, except for:
        - 'detect_types', which is hard-wired to sqlite3.PARSE_DECLTYPES
        - 'isolation_level' which is set to None, i.e. explicit transactions.

        'readonly' is a flag that thinly wraps the SQLite3 way of doing read-only.
        """
        if hasattr(self, 'cnx'):
            raise ConnectionError("There is already an open connection.")
        if not os.path.exists(filepath):
            raise OSError(f"The file '{filepath}' does not exist.")

        self.filepath = filepath
        kwargs["detect_types"] = sqlite3.PARSE_DECLTYPES  # For JSONDOC handling.
        kwargs["isolation_level"] = None                  # Use explicit transactions.
        if readonly:
            filepath = f"file:{self.filepath}?mode=ro"
            kwargs["uri"] = True

        try:
            self.cnx = sqlite3.connect(filepath, **kwargs)
            cursor = self.cnx.cursor()
            cursor = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            names = [n[0] for n in cursor.fetchall()]
        except sqlite3.DatabaseError as error:
            raise InvalidFileError(str(error))
        else:
            if set(["documents", "indexes", "attachments"]).difference(names):
                raise InvalidFileError("Database does not contain the required tables.")


    def close(self):
        "Close the connection to the database."
        if not hasattr(self, "cnx"):
            raise ConnectionError("There is no open connection.")

        self.cnx.close()
        del self.cnx

    @property
    def info(self):
        "Return a dictionary with information about the database."
        sql = "SELECT COUNT(*) FROM indexes"
        n_indexes = self.cnx.execute(sql).fetchone()[0]
        sql = "SELECT COUNT(*) FROM attachments"
        n_attachments = self.cnx.execute(sql).fetchone()[0]
        return dict(version=__version__,
                    n_documents=len(self),
                    n_indexes=n_indexes,
                    n_attachments=n_attachments)

    def __str__(self):
        "Return a string with info about the database."
        return f'jsondocdb.Database("{self.filepath}"): {len(self)} documents, {self.info["n_indexes"]} indexes, {self.info["n_attachments"]} attachments.'

    def __iter__(self):
        """Return an iterator (generator, actually) over document identifiers
        in the database.
        """
        return (row[0] for row in self.cnx.execute("SELECT identifier FROM documents"))

    def __len__(self):
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM documents").fetchone()[0]

    def __contains__(self, identifier):
        "Return `True` if the given identifier is in the database, else `False`."
        sql = "SELECT COUNT(*) FROM documents WHERE identifier=?"
        try:
            return bool(self.cnx.execute(sql, (identifier,)).fetchone()[0])
        except sqlite3.InterfaceError: # When bad identifier.
            return False

    def __getitem__(self, identifier):
        """Return the document with the given identifier.

        Raises TypeError if the identifier is of invalid type.
        Raises NoSuchDocumentError
        """
        if not isinstance(identifier, str):
            raise TypeError("'identifier' must be an instance of 'str'.")

        sql = "SELECT document FROM documents WHERE identifier=?"
        row = self.cnx.execute(sql, (identifier,)).fetchone()
        if not row:
            raise NoSuchDocumentError(f"No document '{identifier}'.")
        return row[0]

    def __setitem__(self, identifier, document):
        """Add or update the document in the database with the given identifier.

        Raises NotInTransactionError
        Raises TypeError if identifier or document are of invalid type.
        Raises ValueError if a key value is not a simple type, or list of simple types.
        """
        if not self.in_transaction:
            raise NotInTransactionError
        if not isinstance(document, dict):
            raise TypeError("'document' must be an instance of 'dict'.")

        cursor = self.cnx.cursor()
        try:
            cursor.execute(
                "INSERT INTO documents (identifier, document) VALUES (?, ?)",
                (identifier, document),
            )
        except sqlite3.IntegrityError:
            cursor.execute(
                "UPDATE documents SET document=? where identifier=?",
                (document, identifier),
            )
        for index in self.indexes():
            index._put(identifier, document)

    def __delitem__(self, identifier):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        if not self.in_transaction:
            raise NotInTransactionError

        cursor = self.cnx.cursor()
        cursor.execute("DELETE FROM documents WHERE identifier=?", (identifier,))
        if cursor.rowcount != 1:
            raise NoSuchDocumentError(f"No document '{identifier}'.")
        for index in self.indexes():
            index._remove(identifier)
        cursor.execute("DELETE FROM attachments WHERE identifier=?", (identifier,))

    def __enter__(self):
        """A context manager for a transaction. All operations that modify
        data in the database must occur within a transaction.
        If all goes well, the transaction is committed.
        If an error occurs within the context block, the transaction is rolled back.

        Raises InTransactionError
        """
        if self.in_transaction:
            raise InTransactionError
        self.cnx.execute("BEGIN")

    def __exit__(self, type, value, tb):
        "End the transaction; commit if OK, rollback if not."
        if type is None:
            self.cnx.execute("COMMIT")
        else:
            self.cnx.execute("ROLLBACK")
        return False

    @property
    def in_transaction(self):
        "Are we within a transaction?"
        return self.cnx.in_transaction

    def get(self, identifier, default=None):
        """Return the document with the given identifier.
        If not found, return the 'default'.
        """
        try:
            return self[id]
        except NoSuchDocumentError:
            return default

    def put(self, identifier, document):
        """Add or update the document in the database with the given identifier.

        Raises NotInTransactionError
        Raises TypeError if identifier or document are of invalid type.
        """
        self[identifier] = document

    def keys(self):
        "Return an iterator over the identifiers for all documents in the database."
        return iter(self)

    def values(self):
        "Return a generator producing all documents in the database."
        sql = "SELECT document FROM documents ORDER BY identifier"
        return (row[0] for row in self.cnx.execute(sql))

    def items(self):
        """Return a generator producing all tuples (identifier, document)
        in the database.
        """
        sql = "SELECT identifier, document FROM documents ORDER BY identifier"
        return (tuple(row) for row in self.cnx.execute(sql))

    def delete(self, identifier):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        del self[identifier]

    def index(self, name, keypath=None, unique=False, require=None):
        """Return the index with the given name, or create it.

        'keypath': If None, return an existing index. If given, create a new index.
        The keypath is the path to the data item in the JSON document to index.
        If the keypath yields None, the document is not included in the index.
        If the keypath yields a list, all elements in the list will be
        included in separate entries in the index.

        'unique': Specifies if the keys in the index must be unique, or not.

        'require': An optional jsonLogic expression. If given, only
        documents satisfying the expression are included in the index.

        Raises NoSuchIndexError
        Raises InTransactionError
        Raises IndexSpecificationError
        Raises IndexExistsError
        """
        return Index(self, name, keypath=keypath, unique=unique, require=require)

    def indexes(self):
        "Return a list of all current indexes."
        cursor = self.cnx.cursor()
        cursor.execute("SELECT name FROM indexes")
        return [Index(self, row[0]) for row in cursor]

    def attachments(self, identifier):
        """Return the attachments interface for the given identifier.

        Raises NoSuchDocumentError
        """
        return Attachments(self, identifier)


class Index:
    "Interface to the named index in the database."

    def __init__(self, db, name, keypath=None, unique=False, require=None):
        "New or existing index."
        if not _INDEXNAME_RX.match(name):
            raise IndexSpecificationError(f"Invalid index name '{name}'.")
        self.db = db
        self.name = name
        if keypath:
            self._create(keypath, unique, require)
        else:
            self._fetch()

    def __str__(self):
        "Return a string with infor about the index."
        return f'jsondocdb.Index("{self.name}", keypath="{self.keypath}"): {len(self)} entries.'

    def _fetch(self):
        """Fetch the definition of this index from the database.

        Raises NoSuchIndexError
        """
        sql = "SELECT keypath, uniq, require FROM indexes WHERE name=?"
        row = self.db.cnx.execute(sql, (self.name,)).fetchone()
        if not row:
            raise NoSuchIndexError(f"No index '{self.name}'")
        self.keypath = row[0]
        self.unique = bool(row[1])
        self.require = row[2]
        self.keypathlogic = JsonLogic({"var": self.keypath})
        self.requirelogic = JsonLogic(self.require)

    def _create(self, keypath, unique, require):
        """Create this index in the database.

        'keypath': The path in the data JSON document to index.
        If the keypath yields None, the document is not included in the index.
        If the keypath yields a list, all elements in the list will be
        included in separate entries in the index.

        'unique': The keys in the index must be unique, or not.

        'require': An optional jsonLogic expression. If given, only documents
        satisfying the expression are included in the index.

        Raises InTransactionError
        Raises IndexSpecificationError
        Raises IndexExistsError
        """
        if self.db.in_transaction:
            raise InTransactionError
        if not isinstance(keypath, str):
            raise IndexSpecificationError("Invalid index 'keypath'; is not a str.")
        if require is not None and not isinstance(require, dict):
            raise IndexSpecificationError("Invalid index 'require'; is not a dict.")

        self.keypath = keypath
        self.unique = bool(unique)
        self.require = require
        self.keypathlogic = JsonLogic({"var": keypath})
        self.requirelogic = JsonLogic(require)

        with self.db:
            try:  # 'uniq' since 'unique' is a reserved word.
                sql = "INSERT INTO indexes (name, keypath, uniq, require) VALUES (?, ?, ?, ?)"
                self.db.cnx.execute(sql, (self.name, self.keypath, self.unique, self.require))
            except sqlite3.IntegrityError:
                raise IndexExistsError(f"Index named '{self.name}' already exists.")

            # This relies on Sqlite3's peculiar take on column type.
            sql = f"CREATE TABLE i_{self.name} (identifier TEXT NOT NULL, key INTEGER NOT NULL)"
            self.db.cnx.execute(sql)
            sql = f"CREATE INDEX xi_{self.name} ON i_{self.name} (identifier)"
            self.db.cnx.execute(sql)
            sql = f"CREATE {self.unique and 'UNIQUE' or ''} INDEX xk_{self.name} ON i_{self.name} (key)"
            self.db.cnx.execute(sql)

            # Add all existing documents in the database into this index.
            try:
                sql = "SELECT identifier, document FROM documents"
                for identifier, document in self.db.cnx.execute(sql).fetchall():
                    self._add(identifier, document)
            except sqlite3.IntegrityError:
                self.db.cnx.execute(f"DROP TABLE i_{self.name}")
                raise NotUniqueError

    def __len__(self):
        "Return the number of entries in the index."
        sql = f"SELECT COUNT(*) FROM i_{self.name}"
        return self.db.cnx.execute(sql).fetchone()[0]

    def __contains__(self, key):
        "Is there at least one entry in the index for the given key?"
        sql = f"SELECT COUNT(*) FROM i_{self.name} WHERE key=?"
        try:
            return bool(self.db.cnx.execute(sql, (key,)).fetchone()[0])
        except sqlite3.InterfaceError:
            return False
    
    def delete(self):
        """Delete this index from the database.

        Raises InTransactionError
        """
        if self.db.in_transaction:
            raise InTransactionError

        with self.db:
            self.db.cnx.execute("DELETE FROM indexes WHERE name=?", (self.name,))
        self.db.cnx.execute(f"DROP TABLE i_{self.name}")
        self.keypath = None
        self.unique = False
        self.require = None
        self.keypathlogic = None
        self.requirelogic = None

    def get(self, key):
        """Return a generator producing the identifiers having the given key
        in this index.
        """
        sql = f"SELECT identifier FROM i_{self.name} WHERE key=?"
        return (row[0] for row in self.db.cnx.execute(sql, (key,)))

    def get_documents(self, key):
        """Return a generator producing tuples of (identifier, document)
        having the given key in this index.
        """
        sql = f"SELECT i.identifier, d.document FROM i_{self.name} AS i, documents AS d WHERE i.key=? AND i.identifier = d.identifier"
        return (tuple(row) for row in self.db.cnx.execute(sql, (key,)))

    def range(self, low=None, high=None, reverse=False):
        """Return a generator producing all tuples (identifier, key) from the
        index given low (inclusive) and high (exclusive) bounds.
        """
        sql = f"SELECT identifier, key FROM i_{self.name}"
        if low is None:
            comparison = []
            keys = []
        else:
            comparison = ["key >= ?"]
            keys = [low]
        if high is not None:
            comparison.append("key < ?")
            keys.append(high)
        if comparison:
            sql += " WHERE "
            if len(comparison) == 2:
                sql += " AND ".join(comparison)
            else:
                sql += comparison[0]
        if reverse:
            sql += " ORDER BY key DESC"
        else:
            sql += " ORDER BY key ASC"
        return (tuple(row) for row in self.db.cnx.execute(sql, keys))

    def range_documents(self, low=None, high=None, reverse=False):
        """Return a generator producing all tuples (identifier, key, document) from
        the index given given low (inclusive) and high (exclusive) bounds.
        """
        sql = f"SELECT i.identifier, i.key, d.document FROM i_{self.name} AS i, documents AS d WHERE i.identifier = d.identifier"
        if low is None:
            comparison = []
            keys = []
        else:
            comparison = ["i.key >= ?"]
            keys = [low]
        if high is not None:
            comparison.append("i.key < ?")
            keys.append(high)
        if comparison:
            sql += " AND "
            if len(comparison) == 2:
                sql += " AND ".join(comparison)
            else:
                sql += comparison[0]
        if reverse:
            sql += " ORDER BY i.key DESC"
        else:
            sql += " ORDER BY i.key ASC"
        return (tuple(row) for row in self.db.cnx.execute(sql, keys))

    def _put(self, identifier, document):
        """Put the given identifier and document to the index.
        Any previous entries are first removed.

        Raises NotInTransactionError
        """
        if not self.db.in_transaction:
            raise NotInTransactionError
        self._remove(identifier)
        self._add(identifier, document)

    def _remove(self, identifier):
        "Remove the entries for this identifier from the index."
        self.db.cnx.execute(f"DELETE FROM i_{self.name} WHERE identifier=?", (identifier,))

    def _add(self, identifier, document):
        """Add entries for the identifier and document to this index.
        Previous entries are not removed.
        """
        if not self.requirelogic.apply(document):
            return
        key = self.keypathlogic.apply(document)
        if key is None:
            return
        try:
            if isinstance(key, (str, int, float)):
                keys = [key]
            elif isinstance(key, list):
                keys = key
                for key in keys:
                    if not isinstance(key, (str, int, float)):
                        raise ValueError
            else:
                raise ValueError
        except ValueError:
            raise ValueError(
                f"Document {identifier}, keypath {keypathlogic.expression.var}, key {key} is not a simple type, or list of simple types."
            )
        for key in keys:
            try:
                sql = f"INSERT INTO i_{self.name} (identifier, key) VALUES (?, ?)"
                self.db.cnx.execute(sql, (identifier, key))
            except sqlite3.IntegrityError:
                raise NotUniqueError(
                    f"Document {identifier}, index {self.name}, keypath {self.keypath}, key {key} is not unique."
                )


class Attachments:
    "Interface to attachments for a document with the given identifier in the database."

    def __init__(self, db, identifier):
        if identifier not in db:
            raise NoSuchDocumentError(f"No document '{identifier}'.")
        self._db = db
        self._identifier = identifier

    def __iter__(self):
        "Return an iterator (generator, actually) over all attachment names for the document."
        sql = "SELECT name FROM attachments WHERE identifier=?"
        return (row[0] for row in self.db.cnx.execute(sql, (self.identifier,)))

    def __len__(self):
        "The number of attachments for this document."
        sql = "SELECT COUNT(*) FROM attachments WHERE identifier=?"
        return self.db.cnx.execute(sql, (self.identifier,)).fetchone()[0]

    def __getitem__(self, name):
        return self.get(name)

    def __setitem__(self, name, content):
        self.put(name, content)

    def __delitem__(self, name):
        self.get(name).delete()

    @property
    def db(self):
        return self._db

    @property
    def identifier(self):
        return self._identifier

    def get(self, name):
        """Return the named attachment.

        Raises NoSuchAttachment
        """
        return Attachment(self.db, self.identifier, name)

    def put(self, name, content, content_type=None):
        """Add or update the given content as attachment to the document.

        The content_type is guessed from the name, if not given explicitly.

        Raises TypeError if name is not str, or the content is not bytes.
        Raises NotInTransactionError
        """
        if not isinstance(name, str):
            raise TypeError("Atachment name must be 'str'.")
        if not isinstance(content, bytes):
            raise TypeError("Attachment contents must be 'bytes'.")
        if not self.db.in_transaction:
            raise NotInTransactionError

        if content_type is None:
            content_type = mimetypes.guess_type(name)[0]
        cursor = self.db.cnx.cursor()
        try:
            cursor.execute(
                "INSERT INTO attachments (identifier, name, content_type, content) VALUES (?, ?, ?, ?)",
                (self.identifier, name, content_type, content),
            )
        except sqlite3.IntegrityError:
            cursor.execute(
                "UPDATE attachments SET content_type=?, content=? WHERE identifier=? AND name=?",
                (content_type, content, self.identifier, name),
            )

    def keys(self):
        "Return an iterator over the names of all attachments for the identifier."
        return iter(self)

    def values(self):
        "Return a generator producing all attachments for the identifier."
        return (Attachment(self.db, self.identifier, name) for name in self)

    def items(self):
        """Return a generator producing all tuples (name, attachment)
        for the identifier.
        """
        return ((name, Attachment(self.db, self.identifier, name)) for name in self)


class Attachment:
    "Interface to an existing attachment."

    def __init__(self, db, identifier, name):
        self._db = db
        self._identifier = identifier
        self._name = name
        sql = "SELECT content_type, content FROM attachments WHERE identifier=? AND name=?"
        row = db.cnx.execute(sql, (identifier, name)).fetchone()
        if not row:
            raise NoSuchAttachment(f"No attachment '{identifier}' '{name}'.")
        self._content_type = row[0]
        self._content = row[1]

    def __len__(self):
        return len(self.content)

    @property
    def db(self):
        return self._db

    @property
    def identifier(self):
        return self._identifier

    @property
    def name(self):
        return self._name

    @property
    def content_type(self):
        return self._content_type

    @property
    def content(self):
        "Return the content of the attachment."
        return self._content

    def delete(self):
        """Delete the attachment. The instance becomes useless after this operation.

        Raises NotInTransactionError
        Raises NoSuchAttachmentError
        """
        if not self.db.in_transaction:
            raise NotInTransactionError
        if not self.name:
            raise NoSuchAttachmenError("This attachment has been deleted.")

        cursor = self.db.cnx.cursor()
        cursor.execute("DELETE FROM attachments WHERE identifier=? AND name=?", (self.identifier, self.name))
        if cursor.rowcount != 1:
            raise NoSuchAttachmentError(f"No such attachment '{identifier}' '{name}'.")
        self._name = None
        

class JsonLogic:
    """Implementation of JsonLogic https://jsonlogic.com/

    Code copied and adapted from https://github.com/nadirizr/json-logic-py
    """

    def __init__(self, expression):
        self.expression = expression or {}

    @staticmethod
    def if_(*args):
        """Implements the 'if' operator with support for multiple elseif-s."""
        for i in range(0, len(args) - 1, 2):
            if args[i]:
                return args[i + 1]
        if len(args) % 2:
            return args[-1]
        else:
            return None

    @staticmethod
    def soft_equals(a, b):
        """Implements the '==' operator, which does type JS-style coercion."""
        if isinstance(a, str) or isinstance(b, str):
            return str(a) == str(b)
        if isinstance(a, bool) or isinstance(b, bool):
            return bool(a) is bool(b)
        return a == b

    @staticmethod
    def hard_equals(a, b):
        """Implements the '===' operator."""
        if type(a) != type(b):
            return False
        return a == b

    @staticmethod
    def less(a, b, *args):
        """Implements the '<' operator with JS-style type coercion."""
        types = set([type(a), type(b)])
        if float in types or int in types:
            try:
                a, b = float(a), float(b)
            except TypeError:
                # NaN
                return False
        return a < b and (not args or less(b, *args))

    @staticmethod
    def less_or_equal(a, b, *args):
        """Implements the '<=' operator with JS-style type coercion."""
        return (less(a, b) or soft_equals(a, b)) and (
            not args or less_or_equal(b, *args)
        )

    @staticmethod
    def to_numeric(arg):
        """
        Converts a string either to int or to float.
        This is important, because e.g. {"!==": [{"+": "0"}, 0.0]}
        """
        if isinstance(arg, str):
            if "." in arg:
                return float(arg)
            else:
                return int(arg)
        return arg

    @staticmethod
    def plus(*args):
        """Sum converts either to ints or to floats."""
        return sum(to_numeric(arg) for arg in args)

    @staticmethod
    def minus(*args):
        """Also, converts either to ints or to floats."""
        if len(args) == 1:
            return -to_numeric(args[0])
        return to_numeric(args[0]) - to_numeric(args[1])

    @staticmethod
    def merge(*args):
        """Implements the 'merge' operator for merging lists."""
        ret = []
        for arg in args:
            if isinstance(arg, list) or isinstance(arg, tuple):
                ret += list(arg)
            else:
                ret.append(arg)
        return ret

    @staticmethod
    def get_var(data, var_name, not_found=None):
        """Gets variable value from data dictionary."""
        try:
            for key in str(var_name).split("."):
                try:
                    data = data[key]
                except TypeError:
                    data = data[int(key)]
        except (KeyError, TypeError, ValueError):
            return not_found
        else:
            return data

    @staticmethod
    def missing(data, *args):
        """Implements the missing operator for finding missing variables."""
        not_found = object()
        if args and isinstance(args[0], list):
            args = args[0]
        ret = []
        for arg in args:
            if get_var(data, arg, not_found) is not_found:
                ret.append(arg)
        return ret

    @staticmethod
    def missing_some(data, min_required, args):
        """Implements the missing_some operator for finding missing variables."""
        if min_required < 1:
            return []
        found = 0
        not_found = object()
        ret = []
        for arg in args:
            if get_var(data, arg, not_found) is not_found:
                ret.append(arg)
            else:
                found += 1
                if found >= min_required:
                    return []
        return ret

    operations = {
        "==": soft_equals,
        "===": hard_equals,
        "!=": lambda a, b: not soft_equals(a, b),
        "!==": lambda a, b: not hard_equals(a, b),
        ">": lambda a, b: less(b, a),
        ">=": lambda a, b: less(b, a) or soft_equals(a, b),
        "<": less,
        "<=": less_or_equal,
        "!": lambda a: not a,
        "!!": bool,
        "%": lambda a, b: a % b,
        "and": lambda *args: functools.reduce(
            lambda total, arg: total and arg, args, True
        ),
        "or": lambda *args: functools.reduce(
            lambda total, arg: total or arg, args, False
        ),
        "?:": lambda a, b, c: b if a else c,
        "if": if_,
        "in": lambda a, b: a in b if "__contains__" in dir(b) else False,
        "cat": lambda *args: "".join(str(arg) for arg in args),
        "+": plus,
        "*": lambda *args: functools.reduce(
            lambda total, arg: total * float(arg), args, 1
        ),
        "-": minus,
        "/": lambda a, b=None: a if b is None else float(a) / float(b),
        "min": lambda *args: min(args),
        "max": lambda *args: max(args),
        "merge": merge,
        "count": lambda *args: sum(1 if a else 0 for a in args),
    }

    def apply(self, data):
        """Does the given data satisfy the expression?
        If the expression is empty, then trivially True.
        """
        if self.expression:
            return self._apply(self.expression, data)
        else:
            return True

    def _apply(self, expression, data):
        """Executes the json-logic with given data."""
        # You've recursed to a primitive, stop!
        if expression is None or not isinstance(expression, dict):
            return expression

        data = data or {}

        operator = list(expression)[0]
        values = expression[operator]

        # Easy syntax for unary operators, like {"var": "x"} instead of strict
        # {"var": ["x"]}
        if not isinstance(values, list) and not isinstance(values, tuple):
            values = [values]

        # Recursion!
        values = [self._apply(val, data) for val in values]

        if operator == "var":
            return self.get_var(data, *values)
        if operator == "missing":
            return self.missing(data, *values)
        if operator == "missing_some":
            return self.missing_some(data, *values)

        try:
            return self.operations[operator](*values)
        except KeyError:
            raise ValueError("Unrecognized operation %s" % operator)


class jsondocdbException(Exception):
    "Base class for jsondocdb errors."
    pass


class ConnectionError(jsondocdbException):
    "Connection exists, or does not exist, when the reverse is expected."
    pass

class InvalidFileError(jsondocdbException):
    "The existing file is not an Sqlite3 or jsondocdb file."
    pass


class NoSuchDocumentError(jsondocdbException):
    "The document does not exist in the jsondocdb database."
    pass


class NoSuchIndexError(jsondocdbException):
    "There is no such index."


class NoSuchAttachmentError(jsondocdbException):
    "The attachment was not found in the jsondocdb database."
    pass


class NotInTransactionError(jsondocdbException):
    "Operation cannot be performed when not in a transaction."


class InTransactionError(jsondocdbException):
    "Operation cannot be performed while in a transaction."


class IndexSpecificationError(jsondocdbException):
    "Index specification is invalid."


class IndexExistsError(jsondocdbException):
    "Index exists already."


class NotUniqueError(jsondocdbException):
    "Index unique constraint was violated."
