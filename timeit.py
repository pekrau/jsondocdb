"Time YasonDB performance."

import os
import time

import click
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
        print(f"{self():.3g} seconds")
        return False


@click.command()
@click.option("-n", "--number", default=10000)
def cli(number):
    try:
        os.remove(DBFILEPATH)
    except IOError:
        pass
    timer = Timer()
    with timer:
        db = yasondb.YasonDB(DBFILEPATH, create=True)
        with db:
            for n in range(number):
                doc = {"n": n, 
                       "key": "some value",
                       f"stuff {n}": {"more": "data"}}
                db.add(doc)
    print(timer.milliseconds / number, "ms per document")
    with timer:
        with db:
            db.create_index("ix", "$.n")
    print(timer.milliseconds / number, "ms per document")
    db.close()
    try:
        os.remove(DBFILEPATH)
    except IOError:
        pass

if __name__ == "__main__":
    cli()
