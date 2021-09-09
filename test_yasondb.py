"Test the YasonDB module."

import os
import unittest

import yasondb


class Test(unittest.TestCase):
    "Test the YasonDB module."

    DBFILEPATH = "/tmp/test.yasondb"

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
        iuid = db.put({"key": "akey", "field": 2})
        db.put({"key": "anotherkey", "field": 4})
        self.assertEqual(len(db), 2)
        self.assertTrue(iuid in db)
        self.assertEqual(db.get_index("key_index")["count"], 2)
        with self.assertRaises(KeyError):
            db.get_index("no_such_index")

    def test_07_exercise_index(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        id1 = "id1"
        id2 = "id2"
        db[id1] = {"key": "akey", "field": 2}
        db[id2] = {"key": "anotherkey", "field": 4}
        self.assertEqual(len(db), 2)
        self.assertTrue(id1 in db)
        index_name = "key_index"
        self.assertFalse(db.index_exists(index_name))
        db.create_index(index_name, "$.key")
        self.assertTrue(db.index_exists(index_name))
        self.assertEqual(db.get_index(index_name)["count"], 2)

    def test_08_several_indexes(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid = db.put({"key": "akey", "field": 2}, "type1")
        db.put({"key": "anotherkey", "field": 4}, "type2")
        db.put({"key": "key2", "field": 8}, "type2")
        db.put({"key": "key3", "field": 4}, "type2")
        index_name1 = "key_index"
        self.assertFalse(db.index_exists(index_name1))
        db.create_index(index_name1, "$.key", "type1")
        self.assertTrue(db.index_exists(index_name1))
        index_name2 = "another_index"
        self.assertFalse(db.index_exists(index_name2))
        db.create_index(index_name2, "$.field", "type2")
        self.assertTrue(db.index_exists(index_name2))
        self.assertEqual(set([index_name1, index_name2]),
                         set(db.get_indexes()))
        self.assertEqual(len(db), 4)
        self.assertTrue(iuid in db)
        self.assertEqual(db.get_index(index_name1)["count"], 1)
        self.assertEqual(db.get_index(index_name2)["count"], 3)

    def test_09_find(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid = db.put({"key": "akey", "key2": 1, "field": 2})
        db.put({"key": "anotherkey", "field": 4})
        db.put({"key": "key2", "key2": 2, "field": 8})
        db.put({"key": "key3", "field": 4})
        index_name1 = "key_index"
        db.create_index(index_name1, "$.key")
        index_name2 = "key2_index"
        db.create_index(index_name2, "field")
        result = list(db.find(index_name1, "akey"))
        self.assertTrue(len(result), 1)
        self.assertEqual(result[0][0], iuid)
        result = db.find(index_name2, 4)
        self.assertTrue(len(list(result)), 2)
        info = db.get_index(index_name1)
        self.assertEqual(info["count"], 4)
        self.assertEqual(info["min"], "akey")
        self.assertEqual(info["max"], "key3")

    def test_10_range(self):
        db = yasondb.YasonDB(self.DBFILEPATH, create=True)
        iuid = db.put({"key": 1, "key2": 1, "field": 2})
        db.put({"key": 2, "field": 4})
        db.put({"key": 1, "field": 8901})
        db.put({"key": 3, "key2": 2, "field": 8})
        db.put({"key": 5, "field": 4})
        db.create_index("index1", "$.key")
        result = list(db.range("index1", 1, 3))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][1]["key"], 1)
        result = list(db.range("index1", 1, 4))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[-1][1]["key"], 3)
        result = list(db.range("index1", 1, 5))
        self.assertEqual(len(result), 5)

if __name__ == "__main__":
    unittest.main()
