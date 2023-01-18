"""jsondocdb

Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic.

The JsonLogic class was adapted from https://github.com/nadirizr/json-logic-py
"""

__version__ = "1.0.0"


import functools
import json
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
    "Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic."

    def __init__(self, filepath, readonly=False, **kwargs):
        """Open or create the database file.

        If the file exists, checks that it has the tables appropriate for jsondocdb.

        If the file is created, creates the required tables.

        The filepath and any additional keyword arguments are passed  to
        sqlite3.connect, except for 'detect_types', which is hard-wired
        to sqlite3.PARSE_DECLTYPES.
        """
        kwargs["detect_types"] = sqlite3.PARSE_DECLTYPES  # To handle JSONDOC.
        if readonly:
            filepath = f"file:{filepath}?mode=ro"
            kwargs["uri"] = True
        self.cnx = sqlite3.connect(filepath, **kwargs)

        cursor = self.cnx.cursor()
        cursor = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = [n[0] for n in cursor.fetchall()]

        if names:  # Check that this is a jsondocdb database file.
            if set(["documents", "indexes", "attachments"]).difference(names):
                raise InvalidFileError

        else:  # Empty; initialize tables required for jsondocdb.
            cursor.execute(
                "CREATE TABLE documents"
                "(identifier TEXT PRIMARY KEY, document JSONDOC NOT NULL)"
            )
            cursor.execute(
                "CREATE TABLE indexes"
                "(name TEXT PRIMARY KEY,"
                " path TEXT NOT NULL,"  # The path string, not the JsonLogic document.
                " uniq INTEGER NOT NULL,"  # Avoid conflict with reserved word.
                " require JSONDOC)"  # Allow NULL.
            )
            cursor.execute(
                "CREATE TABLE attachments"
                "(identifier TEXT NOT NULL,"  # Foreign key to documents.identifier
                " name TEXT NOT NULL,"
                " mimetype TEXT NOT NULL,"
                " size INTEGER NOT NULL,"
                " content BLOB NOT NULL)"
            )
            cursor.execute(
                "CREATE UNIQUE INDEX attachments_index ON attachments (identifier, name)"
            )

        self._indexes = dict(
            [
                (row[0], {"path": row[1], "unique": bool(row[2]), "require": row[3]})
                for row in cursor.execute(
                    "SELECT name, path, uniq, require FROM indexes"
                ).fetchall()
            ]
        )

    def __str__(self):
        "Return a string with info on number of documents and indexes."
        return f"jsondocdb {__version__}: {len(self)} documents, {self.index_count()} indexes, {self.attachment_count()} attachments."

    def __iter__(self):
        "Return an iterator over document identifiers in the database."
        sql = "SELECT identifier FROM documents ORDER BY identifier"
        return (row[0] for row in self.cnx.execute())

    def __len__(self):
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM documents").fetchone()[0]

    def __contains__(self, identifier):
        "Return `True` if the given identifier is in the database, else `False`."
        sql = "SELECT COUNT(*) FROM documents WHERE identifier=?"
        return bool(self.cnx.execute(sql, (identifier,)).fetchone()[0])

    def __getitem__(self, identifier):
        "Return the document with the given identifier."
        sql = "SELECT document FROM documents WHERE identifier=?"
        row = self.cnx.execute(sql, (identifier,)).fetchone()
        if not row:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        return row[0]

    def __setitem__(self, identifier, document):
        """Add or update the document in the database with the given identifier.

        Raises NotInTransactionError
        Raises KeyError if identifier or document are of invalid type.
        """
        if not self.in_transaction:
            raise NotInTransactionError(
                "Cannot add or update document when not in transaction."
            )
        if not isinstance(identifier, str):
            raise KeyError("'identifier' must be an instance of 'str'.")
        if not isinstance(document, dict):
            raise KeyError("'document' must be an instance of 'dict'.")
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
        # Run through the indexes.
        for name, indexdoc in self._indexes.items():
            # First remove document entries if already indexed.
            cursor.execute(f"DELETE FROM i_{name} WHERE identifier=?", (identifier,))
            # Should the document be included in this index?
            if JsonLogic(indexdoc["require"])(document):
                path = indexdoc["path"]
                key = JsonLogic({"var": path})(document)
                if key is None:
                    continue
                if not isinstance(key, (str, int, float)):
                    raise ValueError(
                        f"Index {name}, path {path}: key is not a simple type in document {identifier}."
                    )
                try:
                    cursor.execute(
                        f"INSERT INTO i_{name} (identifier, key) VALUES (?, ?)",
                        (identifier, key),
                    )
                except sqlite3.IntegrityError:
                    raise IndexUniqueError(
                        f"Document {identifier}, index {name}, path {path}, key {key} is not unique."
                    )

    def __delitem__(self, identifier):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        self.delete(identifier)

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
        Raises KeyError if identifier or document are of invalid type.
        """
        self[identifier] = document

    def keys(self):
        "Return an iterator over identifiers for all documents in the database."
        return iter(self)

    def values(self):
        "Return an iterator over all documents in the database."
        sql = "SELECT document FROM documents ORDER BY id"
        return (row[0] for row in self.cnx.execute(sql))

    def items(self):
        "Return an iterator over all tuples (identifier, document) in the database."
        sql = "SELECT identifier, document FROM documents ORDER BY identifier"
        return ((row[0], row[1]) for row in self.cnx.execute())

    def delete(self, identifier):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        if not self.in_transaction:
            raise NotInTransactionError(
                "Cannot delete an item when not in a transaction."
            )
        cursor = self.cnx.cursor()
        cursor.execute("DELETE FROM documents WHERE identifier=?", (identifier,))
        if cursor.rowcount != 1:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        cursor.execute("DELETE FROM attachments WHERE identifier=?", (identifier,))
        for name in self._indexes:
            cursor.execute(f"DELETE FROM i_{name} WHERE identifier=?", (identifier,))

    def document_count(self):
        "Return the number of documents in the database."
        return len(self)

    def create_index(self, name, path, unique=False, require=None):
        """Create an index with the given name to the database.
        All current documents will be indexed, so this might take a while.

        path: The path in the data JSON document to index.
        If the path yields None, the document is not included in the index.
        If the path yields a list, all elements in the list will be
        included in the index.

        unique: Are the keys in the index required to be unique?

        require: An optional jsonLogic expression. If given, only documents
        satisfying the expression are included in the index.
        """
        if not _INDEXNAME_RX.match(name):
            raise IndexSpecificationError(f"Invalid index name '{name}'.")
        if name in self._indexes:
            raise IndexSpecificationError(f"Index '{name}' already exists.")
        if not isinstance(path, str):
            raise IndexSpecificationError("Invalid index path; is not a str.")
        unique = bool(unique)
        if require is not None and not isinstance(require, dict):
            raise IndexSpecificationError("Invalid index require; is not a dict.")
        cursor = self.cnx.cursor()
        cursor.execute("BEGIN")
        try:  # 'uniq' since 'unique' is a reserved word.
            sql = "INSERT INTO indexes (name, path, uniq, require) VALUES (?, ?, ?, ?)"
            cursor.execute(sql, (name, path, unique, require))
        except sqlite3.IntegrityError:
            raise IndexSpecificationError
        # This relies on Sqlite3 peculiar take on column type.
        sql = f"CREATE TABLE i_{name} (identifier TEXT NOT NULL, key INTEGER NOT NULL)"
        cursor.execute(sql)
        cursor.execute(f"CREATE INDEX xi_{name} ON i_{name} (identifier)")
        cursor.execute(
            f"CREATE {unique and 'UNIQUE' or ''} INDEX xv_{name} ON i_{name} (key)"
        )
        self._indexes[name] = {"path": path, "unique": unique, "require": require}
        cursor.execute("SELECT identifier, document FROM documents")
        pathlogic = JsonLogic({"var": path})
        requirelogic = JsonLogic(require)
        for identifier, document in cursor.fetchall():
            if requirelogic(document):
                key = pathlogic(document)
                if key is None:
                    continue
                if not isinstance(key, (str, int, float)):
                    raise ValueError(
                        f"Document {identifier}, path {path}, key {key} is not a simple type."
                    )
                try:
                    sql = f"INSERT INTO i_{name} (identifier, key) VALUES (?, ?)"
                    cursor.execute(sql, (identifier, key))
                except sqlite3.IntegrityError:
                    raise IndexUniqueError(
                        f"Document {identifier}, index {name}, path {path}, key {key} is not unique."
                    )
        cursor.execute("COMMIT")

    def delete_index(self, name):
        """Delete the named index.

        Raises InTransactionError
        Raises NoSuchIndexError
        """
        if self.in_transaction:
            raise InTransactionError(
                "Cannot delete an index while within a transaction."
            )
        try:
            self._indexes.pop(name)
        except KeyError:
            raise NoSuchIndexError(f"No such index '{name}'.")
        else:
            cursor = self.cnx.cursor()
            cursor.execute("BEGIN")
            cursor.execute("DELETE FROM indexes WHERE name=?", (name,))
            cursor.execute("COMMIT")
            cursor.execute(f"DROP TABLE i_{name}")

    def get_indexes(self):
        "Return a copy of the information about all current indexes."
        return self._indexes.copy()

    def is_index(self, name):
        "Is there an index with the given name?"
        return name in self._indexes

    def index_count(self):
        "Return the number of indexes in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM indexes").fetchone()[0]

    def lookup(self, name, key):
        """Get the document identifiers in the named index having the given key.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT identifier FROM i_{name} WHERE key=?"
        return (row[0] for row in self.cnx.execute(sql, (key,)))

    def lookup_documents(self, name, key):
        """Get the tuples (identifier, document) in the named index
        having the given key.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT i.identifier, d.document FROM i_{name} AS i, documents AS d WHERE key=? AND i.identifier=d.identifier"
        return ((row[0], row[1]) for row in self.cnx.execute(sql, (key,)))

    def lookup_count(self, name, key):
        """Return the number of documents in the named index having the given key.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT COUNT(*) FROM i_{name} WHERE key=?"
        return self.cnx.execute(sql, (key,)).fetchone()[0]

    def range(self, name, low=None, high=None, reverse=False):
        """Return an iterator over tuples (identifier, key) in the
        named index given low (inclusive) and high (exclusive) bounds.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT identifier, key FROM i_{name}"
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
        return ((row[0], row[1]) for row in self.cnx.execute(sql, keys))

    def range_documents(self, name, low=None, high=None, reverse=False):
        """Return an iterator over tuples (identifier, document, key) in the
        named index given given low (inclusive) and high (exclusive) bounds.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT i.identifier, d.document, i.key FROM i_{name} AS i, documents AS d WHERE i.identifier = d.identifier"
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
        return ((row[0], row[1], row[2]) for row in self.cnx.execute(sql, keys))

    def range_count(self, name, low=None, high=None, reverse=False):
        """Return the number of documents in the named index 
        given low (inclusive) and high (exclusive) bounds.

        Raises NoSuchIndexError
        """
        if not self.is_index(name):
            raise NoSuchIndexError(f"No such index '{name}'.")
        sql = f"SELECT COUNT(*) FROM i_{name}"
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
        return self.cnx.execute(sql, keys).fetchone()[0]

    def put_attachment(self, identifier, name, mimetype, content):
        """Add the given attachment to the document. Overwrites the attachment
        if it already exists.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        if not self.in_transaction:
            raise NotInTransactionError(
                "Cannot put attachment when not in transaction."
            )
        if not identifier in self:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        if not isinstance(content, bytes):
            raise TypeError("Attachment contents must be bytes.")
        cursor = self.cnx.cursor()
        try:
            cursor.execute(
                "INSERT INTO attachments (identifier, name, mimetype, size, content) VALUES (?, ?, ?, ?, ?)",
                (identifier, name, mimetype, len(content), content),
            )
        except sqlite3.IntegrityError:
            cursor.execute(
                "UPDATE attachments SET mimetype=?, size=?, content=? WHERE identifier=? AND name=?",
                (mimetype, len(content), content, identifier, name),
            )

    def get_attachments(self, identifier):
        """Get the information, but not the content, of all attachments for
        the document as a dictionary with the name as key and the values for
        that name as a dictionary.

        Raises NoSuchDocumentError
        """
        if identifier not in self:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        sql = "SELECT name, mimetype, size FROM attachments WHERE identifier=?"
        return dict([(row[0], {"mimetype": row[1], "size": row[2]})
                     for row in self.cnx.execute(sql, (identifier,)).fetchall()])

    def get_attachment(self, identifier, name):
        """Return the named attachment for the document as a dictionary containing
        the content as well as all other information.

        Raises NoSuchDocumentError if no such document.
        Raises NoSuchAttachmentError if no such attachment.
        """
        if identifier not in self:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        sql = "SELECT mimetype, size, content FROM attachments WHERE identifier=? AND name=?"
        rows = self.cnx.execute(sql, (identifier, name)).fetchall()
        if not rows:
            raise NoSuchAttachmentError(f"No such attachment '{identifier}' '{name}'.")
        return {"identifier": identifier,
                "name": name,
                "mimetype": rows[0][0],
                "size": rows[0][1],
                "content": rows[0][2]}

    def delete_attachment(self, identifier, name):
        """Delete the named attachment for the document.

        Raises NotInTransactionError
        Raises NoSuchDocumentError if no such document.
        Raises NoSuchAttachmentError if no such attachment.
        """
        if not self.in_transaction:
            raise NotInTransactionError(
                "Cannot delete an item when not in a transaction."
            )
        if identifier not in self:
            raise NoSuchDocumentError(f"No such document '{identifier}'.")
        cursor = self.cnx.cursor()
        cursor.execute("DELETE FROM attachments WHERE identifier=? AND name=?", (identifier, name))
        if cursor.rowcount != 1:
            raise NoSuchAttachmentError(f"No such attachment '{identifier}' '{name}'.")

    def attachment_count(self):
        "Return the number of attachments in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM attachments").fetchone()[0]


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

    def __call__(self, data):
        """Does the given data satisfy the expression?
        If there is no expression, the trivially True.
        """
        if self.expression:
            return self.apply(self.expression, data)
        else:
            return True

    def apply(self, expression, data):
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
        values = [self.apply(val, data) for val in values]

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


class InvalidFileError(jsondocdbException):
    "The SQLite3 file is not a jsondocdb file."
    pass


class NoSuchDocumentError(jsondocdbException):
    "The document was not found in the jsondocdb database."
    pass


class NoSuchAttachmentError(jsondocdbException):
    "The attachment was not found in the jsondocdb database."
    pass


class InTransactionError(jsondocdbException):
    "Operation is not allowed while in a transaction."


class NotInTransactionError(jsondocdbException):
    "Operation is not allowed when not in a transaction."


class IndexSpecificationError(jsondocdbException):
    "Index specification is invalid, or index exists already."


class IndexUniqueError(jsondocdbException):
    "Index unique constraint was violated."


class NoSuchIndexError(jsondocdbException):
    "There is no such index."
