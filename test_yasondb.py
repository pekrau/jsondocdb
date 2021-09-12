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
        self.db = yasondb.YasonDB(self.DBFILEPATH, create=True)

    def tearDown(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass

    def test_00_create(self):
        self.assertTrue(self.db.is_valid())
        self.assertEqual(len(self.db), 0)

    def test_01_create_twice(self):
        with self.assertRaises(IOError):
            db2 = yasondb.YasonDB(self.DBFILEPATH, create=True)

    def test_02_open_existing(self):
        self.assertTrue(self.db.is_valid())
        self.db.close()
        db2 = yasondb.YasonDB(self.DBFILEPATH)
        self.assertTrue(db2.is_valid())

    def test_03_open_bad_file(self):
        with self.assertRaises(ValueError):
            db = yasondb.YasonDB(yasondb.__file__)

    def test_04_add_delete_document(self):
        iuid = self.db.put({"key": "value"})
        self.assertTrue(iuid in self.db)
        del self.db[iuid]
        self.assertFalse(iuid in self.db)

    def test_05_add_update_delete_document(self):
        iuid = self.db.put({"key": "value"})
        self.assertTrue(iuid in self.db)
        self.assertEqual(len(self.db), 1)
        self.assertEqual(self.db.count("default"), 1)
        value = "another value"
        self.db.update(iuid, {"key": value})
        doc = self.db[iuid]
        self.assertEqual(doc["key"], value)
        doctype = self.db.get_doctype(iuid)
        self.assertEqual(doctype, "default")
        del self.db[iuid]
        self.assertFalse(iuid in self.db)

    def test_06_iterators(self):
        number = 2 * yasondb.IuidIterator.CHUNK_SIZE + 1
        created = []
        for n in range(number):
            created.append(self.db.put({"key": n, "data": "some data"}))
        self.assertEqual(len(self.db), number)
        iuids = list(self.db)
        self.assertEqual(len(iuids), number)
        self.assertEqual(min(iuids), min(created))
        docs = list(self.db.docs())
        self.assertEqual(type(docs[0]), dict)
        self.assertEqual(len(docs), number)

    def test_07_create_index(self):
        self.assertFalse(self.db.index_exists("key_index"))
        self.db.create_index("key_index", "$.key")
        self.assertTrue(self.db.index_exists("key_index"))
        iuid = self.db.put({"key": "akey", "field": 2})
        self.db.put({"key": "anotherkey", "field": 4})
        self.assertEqual(len(self.db), 2)
        self.assertTrue(iuid in self.db)
        self.assertEqual(self.db.get_index("key_index")["count"], 2)
        with self.assertRaises(KeyError):
            self.db.get_index("no_such_index")

    def test_08_exercise_index(self):
        id1 = "id1"
        id2 = "id2"
        self.db[id1] = {"key": "akey", "field": 2}
        self.db[id2] = {"key": "anotherkey", "field": 4}
        self.assertEqual(len(self.db), 2)
        self.assertTrue(id1 in self.db)
        index_name = "key_index"
        self.assertFalse(self.db.index_exists(index_name))
        self.db.create_index(index_name, "$.key")
        self.assertTrue(self.db.index_exists(index_name))
        self.assertEqual(self.db.get_index(index_name)["count"], 2)

    def test_09_several_indexes(self):
        iuid = self.db.put({"key": "akey", "field": 2}, "type1")
        self.db.put({"key": "anotherkey", "field": 4}, "type2")
        self.db.put({"key": "key2", "field": 8}, "type2")
        self.db.put({"key": "key3", "field": 4}, "type2")
        index_name1 = "key_index"
        self.assertFalse(self.db.index_exists(index_name1))
        self.db.create_index(index_name1, "$.key", "type1")
        self.assertTrue(self.db.index_exists(index_name1))
        index_name2 = "another_index"
        self.assertFalse(self.db.index_exists(index_name2))
        self.db.create_index(index_name2, "$.field", "type2")
        self.assertTrue(self.db.index_exists(index_name2))
        self.assertEqual(set([index_name1, index_name2]),
                         set(self.db.get_indexes()))
        self.assertEqual(len(self.db), 4)
        self.assertTrue(iuid in self.db)
        self.assertEqual(self.db.get_index(index_name1)["count"], 1)
        self.assertEqual(self.db.get_index(index_name2)["count"], 3)

    def test_10_find(self):
        iuid = self.db.put({"key": "akey", "key2": 1, "field": 2})
        self.db.put({"key": "anotherkey", "field": 4})
        self.db.put({"key": "key2", "key2": 2, "field": 8})
        self.db.put({"key": "key3", "field": 4})
        index_name1 = "key_index"
        self.db.create_index(index_name1, "$.key")
        index_name2 = "key2_index"
        self.db.create_index(index_name2, "field")
        result = list(self.db.find(index_name1, "akey"))
        self.assertTrue(len(result), 1)
        self.assertEqual(result[0][0], iuid)
        result = self.db.find(index_name2, 4)
        self.assertTrue(len(list(result)), 2)
        info = self.db.get_index(index_name1)
        self.assertEqual(info["count"], 4)
        self.assertEqual(info["min"], "akey")
        self.assertEqual(info["max"], "key3")

    def test_11_range(self):
        iuid = self.db.put({"key": 1, "key2": 1, "field": 2})
        self.db.put({"key": 2, "field": 4})
        self.db.put({"key": 1, "field": 8901})
        self.db.put({"key": 3, "key2": 2, "field": 8})
        self.db.put({"key": 5, "field": 4})
        self.db.create_index("index1", "$.key")
        result = list(self.db.range("index1", 1, 3))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][1]["key"], 1)
        result = list(self.db.range("index1", 1, 4))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[-1][1]["key"], 3)
        result = list(self.db.range("index1", 1, 5))
        self.assertEqual(len(result), 5)

if __name__ == "__main__":
    unittest.main()
