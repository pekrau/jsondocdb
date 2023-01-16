"""jsondocdb

Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic.

The Logic class was adapted from https://github.com/nadirizr/json-logic-py
"""

__version__ = "0.9.2"


import functools
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


class jsondocdbException(Exception):
    "Base class for jsondocdb errors."
    pass

class InvalidFileError(jsondocdbException):
    "The SQLite3 file is not a jsondocdb file."
    pass

class NoSuchDocumentError(jsondocdbException):
    "The document was not found in the jsondocdb database."
    pass

class InTransactionError(jsondocdbException):
    "Operation is invalid while in a transaction."

class NotInTransactionError(jsondocdbException):
    "Operation is invalid when not in a transaction."

class IndexSpecificationError(jsondocdbException):
    "Index specification is invalid."

class IndexExistsError(jsondocdbException):
    "Index already exists."


class Jsondocdb:
    "Simple JSON document database with indexes; Python, Sqlite3 and JsonLogic."

    def __init__(self, filepath, **kwargs):
        """Open or create the database file.

        If the file exists, checks that it has the tables appropriate for jsondocdb.

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

        if names:     # Check that this is a jsondocdb database file.
            if set(["docs", "indexes", "attachments"]).difference(names):
                raise InvalidFileError

        else:   # Empty; initialize tables required for jsondocdb.
            cursor.execute(
                "CREATE TABLE docs"
                "(docid TEXT PRIMARY KEY,"
                " doc JSONDOC NOT NULL)")
            cursor.execute(
                "CREATE TABLE indexes"
                "(name TEXT PRIMARY KEY,"
                " path JSONDOC NOT NULL,"
                # XXX Add uniqueness flag; look out for reserved word clash.
                " require JSONDOC)"
            )
            cursor.execute(
                "CREATE TABLE attachments"
                "(docid TEXT NOT NULL,"  # Foreign key to docs.docid
                " name TEXT NOT NULL,"
                " mimetype TEXT NOT NULL,"
                " size INT NOT NULL,"
                " data BLOB NOT NULL)"
            )

        self.indexes = dict([(row[0], {"path": row[1], "require": row[2]})
                            for row in cursor.execute("SELECT name, path, require FROM indexes").fetchall()])

    def __str__(self):
        "Return a string with info on number of documents and indexes."
        return f"jsondocdb {__version__}: {len(self)} documents, {self.count_indexes()} indexes, {self.count_attachments()} attachments."

    def __iter__(self):
        "Return an iterator over document identifiers in the database."
        return (row[0] for row in self.cnx.execute("SELECT docid FROM docs ORDER BY docid"))

    def __len__(self):
        "Return the number of documents in the database."
        return self.cnx.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def __getitem__(self, docid):
        "Return the document with the given identifier."
        row = self.cnx.execute("SELECT doc FROM docs WHERE docid=?", (docid,)).fetchone()
        if not row:
            raise NoSuchDocumentError(f"No such document '{docid}'.")
        return row[0]

    def __setitem__(self, docid, doc):
        """Add or update the document in the database with the given identifier.

        Raises NotInTransactionError
        """
        if not self.in_transaction:
            raise NotInTransactionError("Cannot set item when not in transaction.")
        if not isinstance(docid, str):
            raise ValueError("'docid' must be an instance of 'str'.")
        if not isinstance(doc, dict):
            raise ValueError("'doc' must be an instance of 'dict'.")
        cursor = self.cnx.cursor()
        try:
            cursor.execute("INSERT INTO docs (docid, doc) VALUES (?, ?)", (docid, doc))
        except sqlite3.IntegrityError:
            cursor.execute("UPDATE docs SET doc=? where docid=?", (doc, docid))
        # XXX Add to indexes.

    def __delitem__(self, docid):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        self.delete(docid)

    def __contains__(self, docid):
        "Return `True` if the given identifier is in the database, else `False`."
        return bool(self.cnx.execute("SELECT COUNT(*) FROM docs WHERE docid=?", (docid,)).fetchone()[0])

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

    def get(self, docid, default=None):
        """Return the document with the given identifier.
        If not found, return the 'default'.
        """
        try:
            return self[id]
        except NoSuchDocumentError:
            return default

    def keys(self):
        "Return an iterator over identifiers for all documents in the database."
        return iter(self)

    def values(self):
        "Return an iterator over all documents in the database."
        return (row[0] for row in self.cnx.execute("SELECT doc FROM docs ORDER BY id"))

    def items(self):
        "Return an iterator over all tuples (identifier, document) in the database."
        return ((row[0], row[1]) for row in self.cnx.execute("SELECT docid, doc FROM docs ORDER BY docid"))

    def delete(self, docid):
        """Delete the document with the given identifier from the database.

        Raises NotInTransactionError
        Raises NoSuchDocumentError
        """
        if not self.in_transaction:
            raise NotInTransactionError("Cannot delete an item when not in a transaction.")
        cursor = self.cnx.cursor()
        cursor.execute("DELETE FROM docs WHERE docid=?", (docid,))
        if cursor.rowcount != 1:
            raise NoSuchDocumentError
        cursor.execute("DELETE FROM attachments WHERE docid=?", (docid,))
        # XXX Remove from indexes.

    def add_index(self, name, path, require=None):
        """Add an index with the given name to the database.
        All current documents will be indexed, so it might take a while.

        path: The path in the data JSON document to index. If the path yields None,
        the document is not included in the index.

        require: An optional jsonLogic expression. If given, only documents 
        satisfying the expression are included in the index. 
        """
        if not _INDEX_NAME_RX.match(name):
            raise IndexSpecificationError("Invalid index name.")
        try:
            self.cnx.execute("INSERT INTO indexes (name, path, require) VALUES (?, ?, ?)",
                             (name, path, require))
            # XXX Create the table with the index.
        except sqlite3.IntegrityError:
            raise IndexExistsError
        raise NotImplemented

    def delete_index(self, name):
        raise NotImplemented

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



class Logic:
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
        """Implements the '==' operator, which does type JS-style coertion."""
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
        """Implements the '<' operator with JS-style type coertion."""
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
        """Implements the '<=' operator with JS-style type coertion."""
        return (
            less(a, b) or soft_equals(a, b)
        ) and (not args or less_or_equal(b, *args))


    @staticmethod
    def to_numeric(arg):
        """
        Converts a string either to int or to float.
        This is important, because e.g. {"!==": [{"+": "0"}, 0.0]}
        """
        if isinstance(arg, str):
            if '.' in arg:
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
            for key in str(var_name).split('.'):
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
        "and": lambda *args: functools.reduce(lambda total, arg: total and arg, args, True),
        "or": lambda *args: functools.reduce(lambda total, arg: total or arg, args, False),
        "?:": lambda a, b, c: b if a else c,
        "if": if_,
        "in": lambda a, b: a in b if "__contains__" in dir(b) else False,
        "cat": lambda *args: "".join(str(arg) for arg in args),
        "+": plus,
        "*": lambda *args: functools.reduce(lambda total, arg: total * float(arg), args, 1),
        "-": minus,
        "/": lambda a, b=None: a if b is None else float(a) / float(b),
        "min": lambda *args: min(args),
        "max": lambda *args: max(args),
        "merge": merge,
        "count": lambda *args: sum(1 if a else 0 for a in args),
    }


    def __call__(self, data):
        "Does the given data satisfy the expression?"
        return self.apply(self.expression, data)
        
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

        if operator == 'var':
            return self.get_var(data, *values)
        if operator == 'missing':
            return self.missing(data, *values)
        if operator == 'missing_some':
            return self.missing_some(data, *values)

        try:
            return self.operations[operator](*values)
        except KeyError:
            raise ValueError("Unrecognized operation %s" % operator)


if __name__ == "__main__":
    db = Jsondocdb("test.db")
    print(db, list(db))
    with db:
        db["b"] = {"b": 2, "c":3, "d": [1,2,3]}
    with db:
        db["x"] = {"erty": "apa"}
    print(db, list(db))
    for expression in [{"==": [{"var": "b"}, 2]},
                  {"or": [
                      {"and": [
                          {"==": [{"var": "b"}, 2]},
                          {"in": [2, {"var": "d"}]}
                      ]
                       },
                      {"===": [{"var": "erty"}, "apa"]}
                  ]
                   }
                  ]:
        logic = Logic(expression)
        print(expression, logic(db["b"]), logic(db["x"]))
