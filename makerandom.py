"Create a database with some random documents."

import random
import string
import sqlite3
import time
import uuid

import yasondb

class Timer:
    "CPU timer."

    def __init__(self):
        self.start()

    def start(self):
        self._start = time.process_time()

    def __call__(self):
        "Return CPU time (in seconds) since start of this timer."
        return time.process_time() - self._start

    @property
    def milliseconds(self):
        "Return CPU time (in milliseconds) since start of this timer."
        return round(1000 * self())

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, tb):
        print(self())
        return False


random.seed(314)


INDEXKEYS = ["name", "key", "id", "title"]
CHARS = string.ascii_lowercase
NUMBERS = list(range(2, 10))


def main(total):
    timer = Timer()
    with timer:
        # Creating a large-ish database is much faster in memory.
        db = yasondb.YasonDB(":memory:", create=True)
        for key in INDEXKEYS:
            db.create_index(f"{key}", f"$.{key}")
        db.create_index("a", "$.a")
        for i in range(total):
            doc = {random.choice(INDEXKEYS): i}
            for number in range(random.choice(NUMBERS)):
                key = random.choice(CHARS)
                if number % 2:
                    doc[key] = round(1000 * random.random())
                else:
                    doc[key] = "".join(random.sample(CHARS, 10))
            db[uuid.uuid4().hex] = doc
        cnx = sqlite3.connect(f"r{total}.db")
        db.cnx.backup(cnx)

if __name__ == "__main__":
    main(100000)
