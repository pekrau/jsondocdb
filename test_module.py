"Test the jsondblite module."

import os
import unittest

import jsondblite


class Test(unittest.TestCase):
    "Test the jsondblite module."

    DBFILEPATH = "/tmp/test.jsondblite"

    def setUp(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass
        self.db = jsondblite.Database(self.DBFILEPATH, create=True)

    def tearDown(self):
        try:
            os.remove(self.DBFILEPATH)
        except IOError:
            pass

    def test_00_create(self):
        self.assertEqual(len(self.db), 0)

    def test_01_create_twice(self):
        with self.assertRaises(IOError):
            db2 = jsondblite.Database(self.DBFILEPATH, create=True)

    def test_02_open_existing(self):
        self.db.close()
        db2 = jsondblite.Database(self.DBFILEPATH)

    def test_03_open_bad_file(self):
        with self.assertRaises(jsondblite.InvalidDatabaseError):
            db = jsondblite.Database(jsondblite.__file__)

    def test_04_add_delete_document(self):
        with self.db:
            id = self.db.add({"key": "value"})
            self.assertTrue(id in self.db)
        self.assertEqual(len(self.db), 1)
        with self.db:
            del self.db[id]
            self.assertFalse(id in self.db)
        self.assertEqual(len(self.db), 0)
        # Test rollback when error within transaction.
        try:
            with self.db:
                id = self.db.add({"key": "value"})
                self.assertTrue(id in self.db)
                self.assertEqual(len(self.db), 1)
                raise ValueError
        except ValueError:
            self.assertFalse(id in self.db)
            self.assertEqual(len(self.db), 0)
        with self.db:
            id = self.db.add({"key": "value"})
        self.assertEqual(len(self.db), 1)
        try:
            with self.db:
                del self.db[id]
                self.assertFalse(id in self.db)
                raise ValueError
        except ValueError:
            self.assertTrue(id in self.db)
            self.assertEqual(len(self.db), 1)

    def test_05_add_update_delete_document(self):
        with self.db:
            id = self.db.add({"key": "value"})
            self.assertTrue(id in self.db)
            self.assertEqual(len(self.db), 1)
            value = "another value"
            self.db.update(id, {"key": value})
        doc = self.db[id]
        self.assertEqual(doc["key"], value)
        with self.db:
            del self.db[id]
        self.assertFalse(id in self.db)

    def test_06_many(self):
        MANY = 1000
        with self.db:
            created = []
            for n in range(MANY):
                created.append(self.db.add({"key": n, "data": "some data"}))
        self.assertEqual(len(self.db), MANY)
        ids = list(self.db)
        self.assertEqual(len(ids), MANY)
        self.assertEqual(min(ids), min(created))

    def test_07_create_index(self):
        with self.db:
            self.assertFalse(self.db.index_exists("key_index"))
            self.db.create_index("key_index", "$.key")
            self.assertTrue(self.db.index_exists("key_index"))
            id = self.db.add({"key": "akey", "field": 2})
            self.db.add({"key": "anotherkey", "field": 4})
        self.assertEqual(len(self.db), 2)
        self.assertTrue(id in self.db)
        self.assertEqual(self.db.get_index("key_index")["count"], 2)
        with self.assertRaises(KeyError):
            self.db.get_index("no_such_index")

    def test_08_exercise_index(self):
        with self.db:
            id1 = "id1"
            self.db[id1] = {"key": "akey", "field": 2}
            self.assertEqual(len(self.db), 1)
            id2 = "id2"
            self.db[id2] = {"key": "anotherkey", "field": 4}
            self.assertEqual(len(self.db), 2)
            self.assertTrue(id1 in self.db)
            index_name = "key_index"
            self.assertFalse(self.db.index_exists(index_name))
            self.db.create_index(index_name, "$.key")
            self.assertTrue(self.db.index_exists(index_name))
            self.assertEqual(self.db.get_index(index_name)["count"], 2)

    def test_09_several_indexes(self):
        with self.db:
            id = self.db.add({"key": "akey", "id": "id1", "field": 2})
            self.db.add({"key": "anotherkey", "id": "id2", "field": 4})
            self.db.add({"key": "key2", "field": 8})
            self.db.add({"key": "key3", "field": 4})
            index_name1 = "key_index"
            self.assertFalse(self.db.index_exists(index_name1))
            self.db.create_index(index_name1, "$.key")
            self.assertTrue(self.db.index_exists(index_name1))
            index_name2 = "another_index"
            self.assertFalse(self.db.index_exists(index_name2))
            self.db.create_index(index_name2, "$.id")
        self.assertTrue(self.db.index_exists(index_name2))
        self.assertEqual(set([index_name1, index_name2]),
                         set(self.db.get_indexes()))
        self.assertEqual(len(self.db), 4)
        self.assertTrue(id in self.db)
        self.assertEqual(self.db.get_index(index_name1)["count"], 4)
        self.assertEqual(self.db.get_index(index_name2)["count"], 2)

    def test_10_lookup(self):
        with self.db:
            doc = {"key": "akey", "key2": 1, "field": 2}
            id = self.db.add(doc)
            self.db.add({"key": "anotherkey", "field": 4})
            self.db.add({"key": "key2", "key2": 2, "field": 8})
            self.db.add({"key": "key3", "field": 4})
            index_name1 = "key_index"
            self.db.create_index(index_name1, "$.key")
            index_name2 = "key2_index"
            self.db.create_index(index_name2, "field")
        info = self.db.get_index(index_name1)
        self.assertEqual(info["count"], 4)
        self.assertEqual(info["min"], "akey")
        self.assertEqual(info["max"], "key3")
        result = self.db.lookup(index_name1, "akey")
        self.assertTrue(len(result), 1)
        self.assertEqual(result[0], id)
        self.assertEqual(self.db[result[0]], doc)
        result = self.db.lookup(index_name2, 4)
        self.assertTrue(len(result), 2)

    def test_11_range(self):
        with self.db:
            id = self.db.add({"key": 1, "key2": 1, "field": 2})
            self.db.add({"key": 2, "field": 4})
            self.db.add({"key": 1, "field": 8901})
            self.db.add({"key": 3, "key2": 2, "field": 8})
            self.db.add({"key": 5, "field": 4})
            self.db.create_index("index1", "$.key")
        result = list(self.db.range("index1", 1, 3))
        self.assertEqual(len(result), 4)
        self.assertEqual(self.db[result[0]]["key"], 1)
        self.assertEqual(self.db[result[-1]]["key"], 3)
        result = list(self.db.range("index1", 1, 5))
        self.assertEqual(len(result), 5)

    def test_12_search(self):
        with self.db:
            id = self.db.add({"key": 1, "key2": 1, "field": 2})
            self.db.add({"key": 2, "field": 4})
            self.db.add({"key": 1, "field": 8901})
            self.db.add({"key": 3, "key2": 2, "field": 8})
            self.db.add({"key": 5, "field": 4})
        result = self.db.search("$.key")
        self.assertEqual(len(result), 5)
        result = self.db.search("$.key2")
        self.assertEqual(len(result), 2)
        result = self.db.search("$.no_such_key")
        self.assertEqual(len(result), 0)

if __name__ == "__main__":
    unittest.main()
