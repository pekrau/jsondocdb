"Time YasonDB performance."

import os
import time

import yasondb

DBFILEPATH = "/tmp/test.yasondb"
# DBFILEPATH = ":memory:"


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


def main():
    timer = Timer()
    with timer:
        db = yasondb.YasonDB(DBFILEPATH, create=True)
        for n in range(10000):
            doc = {"n": n, "key": "some value", f"stuff {n}": {"more": "data"}}
            db.put(doc)
    with timer:
        db.create_index("ix", "$.n")
    db.close()
    try:
        os.remove(DBFILEPATH)
    except IOError:
        pass

if __name__ == "__main__":
    main()
