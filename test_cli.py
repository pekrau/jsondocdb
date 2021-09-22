"Test the YasonDB CLI."

import json
import os
import subprocess
import unittest

import yasondb

class Docfile:
    "Temporary file for JSON document."

    FILEPATH = "/tmp/doc.json"

    def __init__(self, doc):
        self.doc = doc

    def __str__(self):
        return self.FILEPATH

    def __enter__(self):
        try:
            os.remove(self.FILEPATH)
        except IOError:
            pass
        with open(self.FILEPATH, "w") as outfile:
            json.dump(self.doc, outfile)
        return self

    def __exit__(self, type, value, tb):
        try:
            os.remove(self.FILEPATH)
        except IOError:
            pass
        return False


class Test(unittest.TestCase):
    "Test the YasonDB CLI."

    DBFILEPATH = "/tmp/test.yasondb"

    def execute(self, command, *args):
        return subprocess.run(["python", "yasondb.py"] +
                              [command, self.DBFILEPATH] +
                              list(args),
                              capture_output=True,
                              encoding="utf-8")

    def get_db():
        return yason.Database(self.DBFILEPATH)

    def setUp(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass
        p = self.execute("create")
        self.assertEqual(p.returncode, 0)

    def tearDown(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass

    def test_00_check(self):
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, "Database has 0 documents, 0 indexes.\n")

    def test_01_add(self):
        with Docfile({"key": "value"}) as docfile:
            p = self.execute("add",  "docid", str(docfile))
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 1 documents, 0 indexes.\n")
        
    def test_02_delete(self):
        with Docfile({"key": "value"}) as docfile:
            p = self.execute("add",  "docid", str(docfile))
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 1 documents, 0 indexes.\n")
        p = self.execute("delete", "docid")
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 0 documents, 0 indexes.\n")
        
    def test_03_get(self):
        doc = {"key": "value"}
        with Docfile(doc) as docfile:
            p = self.execute("add", "docid", str(docfile))
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 1 documents, 0 indexes.\n")
        p = self.execute("get", "docid")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(doc, json.loads(p.stdout))

    def test_04_update(self):
        doc = {"key": "value"}
        with Docfile(doc) as docfile:
            p = self.execute("add", "docid", str(docfile))
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 1 documents, 0 indexes.\n")
        p = self.execute("get", "docid")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(doc, json.loads(p.stdout))
        doc["another"] = 3
        with Docfile(doc) as docfile:
            p = self.execute("update", "docid", str(docfile))
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 1 documents, 0 indexes.\n")
        p = self.execute("get", "docid")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(doc, json.loads(p.stdout))

    def test_05_index(self):
        for number in range(5):
            doc = {"key": f"id{number}"}
            with Docfile(doc) as docfile:
                p = self.execute("add", f"docid{number}", str(docfile))
            self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 5 documents, 0 indexes.\n")
        p = self.execute("index-create", "ix", "$.key")
        self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 5 documents, 1 indexes.\n")
        p = self.execute("index", "ix")
        self.assertEqual(p.returncode, 0)
        indexdef = json.loads(p.stdout)
        self.assertTrue("jsonpath" in indexdef)
        self.assertEqual(indexdef["jsonpath"], "$.key")
        self.assertTrue("count" in indexdef)
        self.assertEqual(indexdef["count"], 5)

    def test_06_lookup(self):
        p = self.execute("index-create", "ix", "$.key")
        self.assertEqual(p.returncode, 0)
        for number in range(5):
            doc = {"key": f"id{number}"}
            with Docfile(doc) as docfile:
                p = self.execute("add", f"docid{number}", str(docfile))
            self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 5 documents, 1 indexes.\n")
        p = self.execute("lookup", "ix", "id3")
        self.assertEqual(p.returncode, 0)
        result = json.loads(p.stdout)
        self.assertTrue("count" in result)
        self.assertEqual(result["count"], 1)
        self.assertTrue("docs" in result)
        self.assertEqual(list(result["docs"].keys()), ["docid3"])
        p = self.execute("lookup", "ix", "id3")
        self.assertEqual(p.returncode, 0)
        result = json.loads(p.stdout)
        self.assertTrue("count" in result)
        self.assertEqual(result["count"], 1)
        self.assertTrue("docs" in result)
        self.assertTrue(len(result["docs"]), 1)
        self.assertEqual(list(result["docs"].keys()), ["docid3"])
        self.assertTrue(result["docs"]["docid3"]["key"], "id3")

    def test_07_range(self):
        p = self.execute("index-create", "ix", "$.key")
        self.assertEqual(p.returncode, 0)
        for number in range(5):
            doc = {"key": f"id{number}"}
            with Docfile(doc) as docfile:
                p = self.execute("add", f"docid{number}", str(docfile))
            self.assertEqual(p.returncode, 0)
        p = self.execute("check")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, f"Database has 5 documents, 1 indexes.\n")
        p = self.execute("range", "ix", "id3", "id4")
        self.assertEqual(p.returncode, 0)
        result = json.loads(p.stdout)
        self.assertTrue("count" in result)
        self.assertEqual(result["count"], 2)
        self.assertTrue("docs" in result)
        self.assertEqual(set(result["docs"].keys()), set(["docid3", "docid4"]))
        self.assertEqual(set(result["docs"].keys()), set(["docid3", "docid4"]))
        self.assertTrue(result["docs"]["docid4"]["key"], "id4")


if __name__ == "__main__":
    unittest.main()
