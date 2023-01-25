"Test undumping a substantial Anubis dump into jsondocdb."

import json
import os.path
import random
import tarfile
import time

import jsondocdb

import tqdm


def create_indexes(db):
    require_call = {"==": [{"var": "doctype"}, "call"]}
    db.index("call_identifier", "identifier", require=require_call)
    db.index("call_closes", "closes", require=require_call)
    db.index("call_opens", "opens", require=require_call)
    db.index("call_owner", "owner", require=require_call)
    require_proposal = {"==": [{"var": "doctype"}, "proposal"]}
    db.index("proposal_identifier", "identifier", require=require_proposal)
    db.index("proposal_call", "call", require=require_proposal)
    db.index("proposal_user", "user", require=require_proposal)
    require_review = {"==": [{"var": "doctype"}, "review"]}
    db.index("review_call", "call", require=require_review)
    db.index("review_proposal", "proposal", require=require_review)
    db.index("review_reviewer", "reviewer", require=require_review)
    require_decision = {"==": [{"var": "doctype"}, "decision"]}
    db.index("decision_call", "call", require=require_decision)
    db.index("decision_proposal", "proposal", require=require_decision)
    require_grant = {"==": [{"var": "doctype"}, "grant"]}
    db.index("grant_identifier", "identifier", require=require_grant)
    db.index("grant_call", "call", require=require_grant)
    db.index("grant_proposal", "proposal", require=require_grant)
    db.index("grant_user", "user", require=require_grant)
    require_user = {"==": [{"var": "doctype"}, "user"]}
    db.index("user_username", "username", require=require_user)
    db.index("user_email", "email", require=require_user)
    db.index("user_orcid", "orcid", require=require_user)
    db.index("user_role", "role", require=require_user)
    db.index("user_status", "status", require=require_user)
    db.index("user_last_login", "last_login", require=require_user)

def undump(filepath, db):
    """Load the `tar` file given by the path into the database.
    It must have been produced by `db.dump`.

    Returns a tuple `(ndocs, nfiles)` giving the number of documents
    and attached files read from the file.

    NOTE: The documents are just added to the database, ignoring any
    `_rev` items. This means that no document with the same identifier
    must exist in the database.
    """
    ndocs = 0
    nfiles = 0
    atts = dict()
    with tarfile.open(filepath, mode="r") as infile:
        total = sum(1 for member in infile if member.isreg())
    with tarfile.open(filepath, mode="r") as infile:
        iterator = tqdm.tqdm(infile, total=total)
        for item in iterator:
            itemfile = infile.extractfile(item)
            itemdata = itemfile.read()
            itemfile.close()
            if item.name in atts:
                # An attachment follows its document.
                a = atts.pop(item.name)
                with db:
                    db.attachments(doc["_id"]).put(a["filename"], itemdata, a["content_type"])
                nfiles += 1
            else:
                doc = json.loads(itemdata.decode("utf-8"))
                doc.pop("_rev", None)
                atts = doc.pop("_attachments", dict())
                with db:
                    db[doc["_id"]] = doc
                ndocs += 1
                for attname, attinfo in list(atts.items()):
                    key = u"{}_att/{}".format(doc["_id"], attname)
                    atts[key] = dict(filename=attname,
                                     content_type=attinfo["content_type"])
    return (ndocs, nfiles)


if __name__ == "__main__":
    dbfilepath = "dump.db"
    db = jsondocdb.Database(dbfilepath)
    if len(db) == 0:
        time0 = time.perf_counter()
        print(undump("anubis_dump_2023-01-17.tar.gz", db))
        print(time.perf_counter() - time0, "seconds")
        time0 = time.perf_counter()
        create_indexes(db)
        print(time.perf_counter() - time0, "seconds")
    print(db)
    identifiers = list(db)
    time0 = time.perf_counter()
    for identifier in random.sample(identifiers, 10000):
        doc = db[identifier]
        a = db.attachments(identifier)
        if a:
            atts = list(a.items())
    print(time.perf_counter() - time0, "seconds")
