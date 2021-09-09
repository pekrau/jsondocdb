"Test the YasonDB module."

import os
import unittest

import yasondb


class Test(unittest.TestCase):

    DBFILEPATH = "test.yasondb"

    def setUp(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass

    def tearDown(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass

    def test_00_create(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        self.assertTrue(db.is_valid())
        self.assertEqual(len(db), 0)

    def test_01_create_twice(self):
        db1 = yasondb.YasonDB(self.DBFILEPATH, create=True)
        with self.assertRaises(IOError):
            db2 = yasondb.YasonDB(self.DBFILEPATH, create=True)

    def test_02_open(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        self.assertTrue(db.is_valid())
        db.close()
        db = yasondb.YasonDB(self.DBFILEPATH)
        self.assertTrue(db.is_valid())

    def test_03_open_wrong(self):
        with open(self.DBFILEPATH, "w") as outfile:
            outfile.write("stuff")
        with self.assertRaises(ValueError):
            db = yasondb.YasonDB(self.DBFILEPATH)

    def test_04_add_delete_document(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid = db.put({"key": "value"})
        self.assertTrue(iuid in db)
        del db[iuid]
        self.assertFalse(iuid in db)

    def test_05_add_update_delete_document(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid = db.put({"key": "value"})
        self.assertTrue(iuid in db)
        self.assertEqual(len(db), 1)
        self.assertEqual(db.count("default"), 1)
        value = "another value"
        db.update(iuid, {"key": value})
        doc = db[iuid]
        self.assertEqual(doc["key"], value)
        del db[iuid]
        self.assertFalse(iuid in db)

    def test_06_create_index(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        self.assertFalse(db.index_exists("key_index"))
        db.create_index("key_index", "$.key")
        self.assertTrue(db.index_exists("key_index"))
        iuid1 = db.put({"key": "akey", "field": 2})
        iuid2 = db.put({"key": "anotherkey", "field": 4})
        self.assertEqual(len(db), 2)
        self.assertTrue(iuid1 in db)

    def test_07_exercise_index(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid1 = db.put({"key": "akey", "field": 2})
        iuid2 = db.put({"key": "anotherkey", "field": 4})
        index_name = "key_index"
        self.assertFalse(db.index_exists(index_name))
        db.create_index("key_index", "$.key")
        self.assertTrue(db.index_exists(index_name))
        self.assertEqual(len(db), 2)
        self.assertTrue(iuid1 in db)
        self.assertEqual(db.count_index(index_name), 2)


if __name__ == "__main__":
    unittest.main()
