#
#  Yes.  We use the bson and psycopg2 modules.
#  Why?
#  1.  Programmatic interaction with postgres is much more specific with
#      respect to datatypes and that is the main use case, not throwing
#      SQL at it from the psql CLI.
#  2.  We can test SHA2 and roundtripping *much easier* from a program.
#  3.  python heredocs (triple quoted strings) facilitate construction
#      of big chunks of SQL
#  4.  We show "safe" codecs here to ensure python does not scramble
#      the BSON in a dict.  VERY good practice to use in your code.
#

DSN = 'host=localhost port=5432 dbname=mydb user=postgres password=postgres'

import bson  
import psycopg2

import hashlib
from decimal import Decimal
import datetime

import json
import argparse
import sys
import os


import collections  # From Python standard library.
from bson.codec_options import CodecOptions
codec_options = CodecOptions(document_class=collections.OrderedDict)

def safe_bson_decode(bson_bytea):
    return bson.decode(bson_bytea, codec_options=codec_options)

def safe_bson_encode(doc):
    # Nothing fancy but nice for symmetry with safe_bson_decode
    return bson.encode(doc) 


# python datetime has res to micros but BSON is only to millis.
# optional no millis
def makeDatetime(y, m, d, h, min, sec, millis=0):
    return datetime.datetime(y,m,d,h,min,sec,millis*1000)    
    
def makeDecimal128(str_val):
    return bson.decimal128.Decimal128(Decimal(str_val))


#
#  G L O B A L S
#
#  Executed before we even start main
#
conn = psycopg2.connect(DSN)

#  Common values to insert and then check for:
a_decimal = Decimal("77777809838.97")
a_datetime = makeDatetime(2022,6,6,12,13,14,500) # 6-Jun-2022 12:13:14.500

#  A reasonably complex piece of data:
sdata = {
    "header": {
	"ts": makeDatetime(2022,5,5,12,13,14,456),
        "evId":"E23234",
        "type":"X",
        "active": True
    },
    "data": {
        "recordId":"ID0",
        "notIndexed":"N0",        
	"amt": makeDecimal128(a_decimal),  # must do this...
        "txDate": a_datetime,
        "sub1": {
            "sub2": {
                "corn":"dog"
            }
        },
        "refundDate": datetime.datetime(2022,8,8,12,13,14,456),        
        "userPrefs": [
            {"type": "DEP", "updated":datetime.datetime(2021,4,4,12,13,14),"u":{
                "favoriteCar":"Bugatti",
                "thumbnail": bson.binary.Binary(bytes("Pretend this is a JPEG", 'utf-8'))
                }},
            {"type": "X2A", "updated":datetime.datetime(2021,3,3,12,13,14),"u":{
                "listOfPrimes":[2,3,5,7,11,13,17,19],
                "atomsInPlanet": 283572834759209881,
                "pi": 3.1415926
                }}
            ],
        "payments": [
            {"date": makeDatetime(2022,5,5,12,0,0,0), "amt": makeDecimal128("10.09")}
            ,{"date": makeDatetime(2022,6,8,12,0,0,0), "amt": makeDecimal128("98.23")}
            ,{"date": makeDatetime(2022,7,1,12,0,0,0), "amt": makeDecimal128("212.87")}
            ,{"date": makeDatetime(2022,8,1,12,0,0,0), "amt": makeDecimal128("154.55")}
            ,{"date": makeDatetime(2022,9,1,12,0,0,0), "amt": makeDecimal128("154.55")}                                                
        ]
    }
}


    
curs = conn.cursor()

#  The test table is called BSONTEST.  I don't want to go through the
#  drudgery of making it a variable.
#
def init(rargs):

    print("dropping/creating BSONTEST...")

    sql = """
drop table if exists bsontest CASCADE;
drop extension if exists pgbson CASCADE;

create extension pgbson;

create table bsontest (
    marker text,
    bdata BSON,
    bdata_len integer,    
    bdata2 BSON,
    jbdata jsonb,
    jdata json,
    raw bytea
    );
"""
    try:
        curs.execute(sql);
    except Exception as errmsg:
        print("FAIL: cannot init environment:", errmsg)
        print("check that extension was installed (make install) and/or DB permissions are set properly")
        sys.exit(1)
        
    conn.commit()


#  Construct JSON equiv.  Datetimes won't go in so we need a custom
#  converter to string (this is living proof of a BSON advantage...)    
def cvt(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, bson.binary.Binary):
        return str(o) 
    if isinstance(o, bson.decimal128.Decimal128):
        return str(o)
        

def insertBson(pydata):
    rb7 = safe_bson_encode(pydata)
    curs.execute("TRUNCATE TABLE bsontest")
    curs.execute("INSERT INTO bsontest (bdata,bdata_len) VALUES (%s,%s)", (rb7,len(rb7)))
    conn.commit()
    return rb7

def insert2Bson(pydata,pydata2):
    rb7 = safe_bson_encode(pydata)
    rb8 = safe_bson_encode(pydata2)    
    curs.execute("TRUNCATE TABLE bsontest")
    curs.execute("INSERT INTO bsontest (bdata,bdata2) VALUES (%s,%s)", (rb7,rb8))
    conn.commit()
    return (rb7,rb8)



def get_doc_from_bson(sql):
    curs.execute(sql)
    all_recs = curs.fetchall()
    if len(all_recs) != 1:
        err = "did not find 1 record"
    else:
        item = all_recs[0][0]  # Get column 1
        if type(item) is memoryview:
            item = bytes(item)
        return safe_bson_decode(item)
    

def fetchRowNCol(sql,N):
    items = None
    curs.execute(sql)
    all_recs = curs.fetchall()
    if len(all_recs) == 1:
        items = [ all_recs[0][n] for n in range(0,N) ]
    return items

def fetchRow1Col(sql):
    items = fetchRowNCol(sql,1)
    if items != None:
        items = items[0] # turn list into single scalar
    return items


def check1(args):
    sql,expected = args

    insertBson(sdata)  # always put in the big structure...
    
    msg = None
    
    item = fetchRow1Col(sql)  # OK to be None

    if type(item) is memoryview:
        # Love memoryview for performance -- but here we need to
        # hydrate to get the bytes...
        item = bytes(item)

    if expected != item:
        msg = "got\n%s::%s\nexpected\n%s::%s" % (item,type(item),expected,type(expected))

    return msg


#  Regression test for a stack buffer overflow in bson_as_text().
#  The old code did sprintf(valbuf, "%lf", v) into a char[64].  "%lf" is
#  *fixed* notation, so the rendered length tracks the exponent: 1e308 comes
#  out as 316 characters and smashed the stack, taking the backend with it.
#  As a bonus it rounded everything to 6 decimal places.
#  Note these all go through ->> i.e. bson_as_text(), which is a completely
#  different code path than bson_get_double().
def double_as_text():
    doc = {
        "big":    1e308,                   # the one that used to crash
        "neg":   -1.7976931348623157e308,  # -DBL_MAX
        "small":  3.1415926,               # used to come back "3.141593"
        "whole":  2.0                      # used to come back "2.000000"
    }
    insertBson(doc)

    for fld, expected in [
            ("big",   "1e+308"),
            ("neg",   "-1.7976931348623157e+308"),
            ("small", "3.1415926"),
            ("whole", "2")
    ]:
        item = fetchRow1Col("SELECT bdata->>'%s' FROM bsontest" % fld)
        if item != expected:
            return "%s: got %s, expected %s" % (fld, item, expected)

    return None


#  bool and ObjectId used to fall off the end of the switch in bson_as_text()
#  and come back NULL, even though bson_get_boolean() handled bool just fine.
#  bool renders "true"/"false" to match jsonb's ->>, not postgres' own "t"/"f".
#  ObjectId renders as the bare 24 char hex, no {"$oid": ...} wrapper -- same
#  as decimal128 and datetime, which also drop their $-wrappers here.
#  There is deliberately no bson_get_objectid(); it has no high fidelity
#  postgres type to land in.
def bool_oid_as_text():
    an_oid = "64b7f0c2a1b2c3d4e5f60718"

    doc = {
        "yes": True,
        "no":  False,
        "oid": bson.ObjectId(an_oid)
    }
    insertBson(doc)

    for fld, expected in [
            ("yes", "true"),
            ("no",  "false"),
            ("oid", an_oid)
    ]:
        item = fetchRow1Col("SELECT bdata->>'%s' FROM bsontest" % fld)
        if item != expected:
            return "%s: got %s, expected %s" % (fld, item, expected)

    return None


#  Regression test for a backend segfault in _get_obj_or_arr().
#  BSON cannot be built from a bare scalar (see README "Arrays, BSON, and
#  JSON"), so -> on a scalar has nothing to return and must yield NULL.
#  The old code returned the result of bson_iter_find_descendant -- i.e.
#  "was the path found", NOT "was it a document/array" -- so a found scalar
#  reported success while leaving the child bson_t completely uninitialized.
#  bson_get_bson() then handed that garbage stack struct to mk_palloc_bytea(),
#  which read b->len out of it: SIGSEGV, and a segfaulting backend takes the
#  whole postmaster down with it.  bson_get_jsonb_array() had it identically.
#  Every BSON scalar type is exercised because the bug was type-independent;
#  only reaching the switch's default: branch mattered.
def scalar_arrow_returns_null():
    insertBson(sdata)

    scalar_paths = [
        "header.type",                        # utf8
        "header.active",                      # bool
        "header.ts",                          # datetime
        "data.amt",                           # decimal128
        "data.userPrefs.1.u.atomsInPlanet",   # int64
        "data.userPrefs.1.u.pi",              # double
        "data.userPrefs.1.u.listOfPrimes.0",  # int32
        "data.userPrefs.0.u.thumbnail"        # binary -- the original repro
    ]

    for path in scalar_paths:
        #  Both spellings of the same call: the -> operator and the function
        #  it binds to.
        for sql in [
                "SELECT bdata->'%s' FROM bsontest" % path,
                "SELECT bson_get_bson(bdata,'%s') FROM bsontest" % path,
                "SELECT bson_get_jsonb_array(bdata,'%s') FROM bsontest" % path
        ]:
            item = fetchRow1Col(sql)
            if item is not None:
                return "%s: got %s, expected None" % (sql, item)

    #  ... and make sure the fix did not break the cases that must still work.
    #  ::bytea or psycopg2 hands back the bson_out text rendering as a str;
    #  get_doc_from_bson() feeds that to bson.decode(), which needs bytes.
    doc = get_doc_from_bson("SELECT (bdata->'data.sub1.sub2')::bytea FROM bsontest")
    if doc != {"corn": "dog"}:
        return "subdocument: got %s, expected {'corn': 'dog'}" % doc

    arr = fetchRow1Col("SELECT bson_get_jsonb_array(bdata,'data.userPrefs.1.u.listOfPrimes') FROM bsontest")
    if arr != [2, 3, 5, 7, 11, 13, 17, 19]:
        return "array: got %s, expected the list of primes" % arr

    #  A path that does not exist at all was always NULL; keep it that way.
    if fetchRow1Col("SELECT bdata->'no.such.path' FROM bsontest") is not None:
        return "missing path did not return None"

    return None


#  bson_get_value() is the general extractor: every BSON type, scalar or not,
#  returned wrapped under the key "_".  The wrap is UNCONDITIONAL -- peel
#  exactly one layer and you have the true value, with no ambiguity between
#  "the scalar 5" and "a document that is {'_': 5}".
def get_value_wraps_everything():
    insertBson(sdata)

    #  Not get_doc_from_bson(): it decodes unconditionally, so a SQL NULL
    #  (missing path) would blow up in bson.decode() before we could test it.
    def unwrap(path):
        raw = fetchRow1Col(
            "SELECT (bson_get_value(bdata,'%s'))::bytea FROM bsontest" % path)
        if raw is None:
            return None
        if type(raw) is memoryview:
            raw = bytes(raw)
        doc = safe_bson_decode(raw)
        if list(doc.keys()) != ["_"]:
            raise Exception("%s: wrapper keys were %s, expected ['_']"
                            % (path, list(doc.keys())))
        return doc["_"]

    #  Scalars: full type fidelity, which is exactly what ->> cannot give you.
    for path, expected in [
            ("header.type",                       "X"),
            ("header.active",                     True),
            ("data.recordId",                     "ID0"),
            ("data.amt",                          makeDecimal128(a_decimal)),
            ("data.userPrefs.1.u.atomsInPlanet",  283572834759209881),
            ("data.userPrefs.1.u.pi",             3.1415926),
            ("data.userPrefs.1.u.listOfPrimes.0", 2)
    ]:
        item = unwrap(path)
        if item != expected:
            return "%s: got %s::%s, expected %s::%s" % (
                path, item, type(item), expected, type(expected))

    #  Datetime and binary survive as real types, not stringifications.
    if unwrap("header.ts") != makeDatetime(2022,5,5,12,13,14,456):
        return "header.ts did not round trip as a datetime"

    thumb = unwrap("data.userPrefs.0.u.thumbnail")
    if thumb != bytes("Pretend this is a JPEG", 'utf-8'):
        return "thumbnail did not round trip as binary: %s" % thumb

    #  Documents wrap too -- the rule is uniform, not scalars-only.
    if unwrap("data.sub1.sub2") != {"corn": "dog"}:
        return "subdocument did not come back wrapped"

    #  The payoff: inside the wrapper an array is a VALUE, so it decodes as a
    #  real array rather than the string-indexed {"0":..,"1":..} form that
    #  bson_get_bson() is forced into.  Compare the two here.
    primes = unwrap("data.userPrefs.1.u.listOfPrimes")
    if primes != [2,3,5,7,11,13,17,19]:
        return "array did not come back as a real array: %s" % primes

    forced = get_doc_from_bson(
        "SELECT (bson_get_bson(bdata,'data.userPrefs.1.u.listOfPrimes'))::bytea FROM bsontest")
    if forced != {"0":2,"1":3,"2":5,"3":7,"4":11,"5":13,"6":17,"7":19}:
        return "bson_get_bson no longer returns the string-indexed form: %s" % forced

    #  The ambiguity case that unconditional wrapping exists to prevent: a
    #  document that genuinely IS {"_": ...} must survive distinguishably.
    insertBson({"tricky": {"_": 5}, "plain": 5})
    if unwrap("tricky") != {"_": 5}:
        return "a stored {'_':5} document did not survive the wrap"
    if unwrap("plain") != 5:
        return "the scalar 5 did not survive the wrap"

    #  Missing path is NULL, like every other accessor.
    insertBson(sdata)
    if unwrap("no.such.path") is not None:
        return "missing path did not return None"

    return None


#  Exercises the extension version machinery, not the C code: that an install
#  still sitting at 2.0 can be moved forward with ALTER EXTENSION UPDATE and
#  come out with the 2.1 function set.  This matters because CREATE EXTENSION
#  runs the install script only at install time, so shipping a new function in
#  pgbson--2.1.sql does nothing for somebody already on 2.0 -- the upgrade
#  script pgbson--2.0--2.1.sql is what carries it across.
#
#  Note the shlib is NOT versioned along with the SQL: $libdir/pgbson is
#  whatever the last make install put there and it serves every version, so
#  an install marked 2.0 is already running current C code.  Only the SQL
#  surface is gated.  That is why the 2.0 assertion below is about
#  bson_get_value being absent from the CATALOG, not about behavior.
#
#  This test drops and recreates the extension, which takes bsontest with it
#  (bson typed columns), so it calls init() at the end to put the world back
#  for whatever runs after it.
def extension_upgrade_path():
    def one(sql):
        curs.execute(sql)
        conn.commit()

    def version():
        return fetchRow1Col(
            "SELECT extversion FROM pg_extension WHERE extname='pgbson'")

    def has_get_value():
        return 0 != fetchRow1Col(
            "SELECT count(*) FROM pg_proc WHERE proname='bson_get_value'")

    msg = None

    try:
        one("drop table if exists bsontest CASCADE")
        one("drop extension if exists pgbson CASCADE")

        #  Land on the old version deliberately.
        one("create extension pgbson version '2.0'")
        if version() != '2.0':
            return "could not install 2.0; got version %s" % version()
        if has_get_value():
            return "bson_get_value is in pgbson--2.0.sql; it must not be"

        #  ...and walk it forward.
        one("alter extension pgbson update to '2.1'")
        if version() != '2.1':
            return "ALTER EXTENSION UPDATE did not land on 2.1; got %s" % version()
        if not has_get_value():
            return "bson_get_value missing after upgrade to 2.1"

        item = fetchRow1Col(
            """SELECT bson_get_value('{"a":{"b":"scalar!"}}'::bson,'a.b')::text""")
        if item != '{ "_" : "scalar!" }':
            return "upgraded bson_get_value returned %s" % item

        #  A fresh install must land on 2.1 by itself, i.e. pgbson.control's
        #  default_version is the "main switch" and it got bumped.
        one("drop extension pgbson CASCADE")
        one("create extension pgbson")
        if version() != '2.1':
            return "default_version is not 2.1; fresh install gave %s" % version()

    finally:
        #  Put back what we tore down, whatever happened above.
        init(None)

    return msg


def toast_test():
    """Put a big obj in there.  Look at 
https://hakibenita.com/sql-medium-text-performance to get an idea of how
to check that the internals are actually doing something."""

    # A big chunk of data that lz cannot compress (thanks to randomness) to fit
    # in a page.  This will force TOASTing:
    rand_bytes = os.urandom(8000)
    data = {
        "id":"ID0",
        "notIndexed":"N0",        
        "biggie": bson.binary.Binary(rand_bytes)
    }

    bson1 = insertBson(data)

    rb2 = fetchRow1Col('SELECT bdata::bytea FROM bsontest')
    # ALT:  select length(bson_get_binary(bdata, 'biggie')) from bsontest;

    msg = None
    if bson1 != bytes(rb2):  # For compare, must be bytes-to-bytes!
        msg = "FAIL; roundtrip of TOAST sized content does not equal"
    
    return msg
        
    
def jsonb_test():  
    """Here is why jsonb type is not the same as BSON."""

    # Here is some simple data; forget about dates and decimal for the moment:
    dd = {"B":2,"A":1,"ZZ":0,"C":{"E":2,"D":1}}

    # Store it.  There is no way to directly pass a dictionary; it must be turned into
    # JSON somehow; we'll use the standard json module:
    some_json = json.dumps(dd)

    # Get the HASH:
    h2 = hashlib.sha256(bytes(some_json,'utf-8')).hexdigest()
    #print("outgoing hash:", h2)

    curs.execute("TRUNCATE TABLE bsontest")
    curs.execute("INSERT INTO bsontest (jbdata) VALUES (%s)", (some_json,))
    conn.commit()  

    
    # Attempt 1:  Fetch with no cast:
    j2 = fetchRow1Col('SELECT jbdata FROM bsontest' )

    #
    # psycopg2 will by default turn jsonb into a dict.  That is actually
    # a neat trick and in many (most?) cases highly desirable and convenient.
    # But here we want the raw JSON back, so our first attempt is to
    # turn the dict in a JSON string...
    s2 = json.dumps(j2)
    # ... which of course yields a DIFFERENT hash:
    h3 = hashlib.sha256(bytes(s2,'utf-8')).hexdigest()
    #print("incoming hash:", h3)
    #print("hashes the same?", h2 == h3)


    # Attempt 2:  Fetch casting to string, which will return JSON directly:
    s2 = fetchRow1Col('SELECT jbdata::text FROM bsontest' )
    #print(s2.__class__,":",s2)
    # ... but of course, same problem:
    h3 = hashlib.sha256(bytes(s2,'utf-8')).hexdigest()
    #print("incoming hash:", h3)
    #print("hashes the same?", h2 == h3)    

    #  Bottom line:  you cannot roundtrip jsonb in a cryptoassured manner.
    
    msg = None
    if(h2 != h3):
        msg = "roundtrip of hashes not the same"
        
    return msg
    
    
def binary_checks():
    msg = None

    def x(p1,p2,expect):
        result = True
        insert2Bson(p1,p2)        
        rc = fetchRow1Col("SELECT pgbson_compare(bdata,bdata2) FROM bsontest")
        if rc != expect:
            result = False
        return result
    
    for tt in [
            ({'foo':{'bar':[1,2,3]}, 'baz':3.14159},
             {'foo':{'bar':[1,2,3]}, 'baz':3.14159},
             0,
             "returns non-zero for two identical objects")

            ,({'foo':{'bar':[1,2,3]}, 'baz':3.14159},
             {'foo':{'bar':[1,2]}, 'baz':3.14159},
              1,
              "p1 should be > p2")

            ,({'foo':{'bar':[1,2]}, 'baz':3.14159},
             {'foo':{'bar':[1,2,7]}, 'baz':3.14159},
             -1,
              "p1 should be < p2"
             )                        

    ]:
        if False == x(tt[0],tt[1],tt[2]):
            msg = tt[3]
            break

    return msg



def bson_test():
    """Calling bson_get_bson() via SQL will, behind the scenes, *also* invoke the
    bson_out() function in the extension to render a text type"""
    insertBson(sdata)

    msg = None
        
    item = fetchRow1Col("SELECT bson_get_bson(bdata, 'data') FROM bsontest")
    if item.__class__.__name__ != 'str':
        msg = "SELECT bson_get_bson(bdata, 'data') did not return JSON equiv"
    else:
        item = fetchRow1Col("SELECT (bson_get_bson(bdata, 'data'))::bytea FROM bsontest")
        if item.__class__.__name__ != 'memoryview':
            msg = "SELECT (bson_get_bson(bdata, 'data'))::bytea did not return memoryview (raw bytes)"
    return msg


def cast_insert_good_json():
    msg = None

    json = '{"A":"X"}'
    try:
        curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)", (json,))
    except Exception as errmsg:
        msg = "valid JSON did not insert"
        
    return msg
    

def cast_short_bson():
    msg = None

    #  BSON must be minimum 5 bytes (4 bytes of length followed by trailing NULL
    #  byte).   This is a good way to quickly catch clearly malformed data:
    try:
        bb = bytes('Z', 'utf-8')   # one byte, 'Z'
        curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)", (bb,))
        msg = "malformed BSON (short bytes) inserted OK"        
    except Exception as errmsg:
        pass  # bytea too short is OK

    return msg
    

def cast_badnull_bson():
    msg = None
    # Craft a good byte array that represents {'A':'A'} but then
    # deliberately make last byte not NULL, another quick internal test:
    d = bytes([0x0e, 0x00, 0x00, 0x00, 0x02, 0x41, 0x00, 0x02, 0x00, 0x00,0x00,0x41,0x00,0x01])
    try:
        curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)", (d,))
        msg = "non-NULL last byte did not provoke insert fail"
    except Exception as errmsg:
        pass   # good to raise exception

    return msg

def cast_corrupt_bson():
    msg = None
    # Deliberately corrupt BSON:       Some FF here!  But overall structure
    # is OK:
    d = bytes([0x0e, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00,0x00,0x41,0x00, 0x00])
    try:
        curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)", (d,))
        msg = "bad BSON bytes did not provoke insert fail"
    except Exception as errmsg:
        pass  # good: general "invalid BSON"

    return msg



def ejson_builtin_test1():
    msg = None
    
    insertBson(sdata)

    #  Fancy!  This will take payments array, add a new payment, delete
    #  the oldest (first) one, and update the record!
    #
    #  Notes:
    #  1.  In SQL, update can only update a whole column so e.g.
    #          update bsontest set bdata->data->payments[3] ...
    #      is NOT possible.  So....
    #  2.  We use jsonb_set() to touch only the {data,payments} path. This
    #      allows all other material in bdata to NOT be dropped/overwritten
    #  3.  We append a new EJSON payment object with the || operator
    #  4.  Then we use the minus operator '- 0' to remove the zeroeth element
    #

    new_pmt = '{"amt": {"$numberDecimal": "888"}, "date": {"$date": "2023-11-11T12:00:00Z"}}'

    sql = """
update bsontest set bdata = jsonb_set(bdata::jsonb, '{data,payments}', ((((bdata->'data')::jsonb)->'payments' || '%s') - 0))::bson
    """ % new_pmt

    curs.execute(sql);
    conn.commit()

    def _chk(sql,exp,blurb):
        msg = None

        got = fetchRow1Col(sql)  # OK to be None
        
        #  type check to avoid implicit casts...
        if got.__class__ != exp.__class__:
            msg = "%s type mismatch; expected [%s], got [%s]" % (blurb,exp.__class__,got.__class__)
        else:
            if got != exp:
                msg = "%s data mismatch; expected [%s], got [%s]" % (blurb,exp,got)        
        return msg
            
    def _f1():
        sql = """
select (bdata->'data'->'payments'->'0'->>'amt')::numeric from bsontest;
    """
        #  Careful:  Pulling casted numerics from postgres yields Decimal
        #  in psycopg, NOT bson.decimal128; this is OK.
        return _chk(sql, Decimal("98.23"), "new first payment date")        


    def _f2():
        #  Go for it again using function; note we can use >=0 index into
        #  array here thanks to dotpaths and we get a native numeric as a
        #  return type; no casting:
        sql = """
select bson_get_decimal128(bdata,'data.payments.0.amt') from bsontest;
    """
        return _chk(sql, Decimal("98.23"), "new first payment date")


    def _f4():
        sql = """
select bson_get_datetime(bdata,'data.payments.4.date') from bsontest;
    """
        return _chk(sql, makeDatetime(2023,11,11,12,0,0,0), "last payment date")

    def _f3():
        sql = """
select jsonb_array_length(((bdata->'data')::jsonb)->'payments') from bsontest
        """
        return _chk(sql, 5, "array length")

    for f in [_f1,_f2,_f3,_f4]:
        msg = f()
        if msg != None:
            break
        
    return msg


def ejson_builtin_test2():
    msg = None
    
    insertBson(sdata)

    # Sort of the same but using bson_get_jsonb_array
    
    new_pmt = '{"amt": {"$numberDecimal": "888"}, "date": {"$date": "2023-11-11T12:00:00Z"}}'

    sql = """
update bsontest set bdata = jsonb_set(bdata::jsonb, '{data,payments}', (( bson_get_jsonb_array(bdata,'data.payments') || '%s') - 0))::bson
    """ % new_pmt

    curs.execute(sql);
    conn.commit()
    
    sql = """
select (bdata->'data'->'payments'->'0'->>'amt')::numeric from bsontest;
    """
    item = fetchRow1Col(sql)  # OK to be None

    #  Careful:  Pulling casted numerics from postgres yields Decimal
    #  in psycopg, NOT bson.decimal128; this is OK.
    exp = "98.23"
    if item != Decimal(exp):
        msg = "expected new first payment to be [%s], got [%s]" % (exp,item)

    # select bson_to_jsonb_array(bdata,'data.payments')
        
    else:
        sql = """
        select jsonb_array_length(bson_get_jsonb_array(bdata,'data.payments')) from bsontest
        """
        item = fetchRow1Col(sql)  # OK to be None
        if item != 5:
            msg = "expected payment array length to remain 5, got [%s]" % item
        
    return msg



def array_update():
    msg = None
    
    insertBson(sdata)

    # Sort of the same but using BSON
    
    new_pmt = {"amt": makeDecimal128(a_decimal), "date":makeDatetime(2022,5,5,12,13,14,456)}
    rb7 = safe_bson_encode(new_pmt)
    
    sql = """
update bsontest set bdata = jsonb_set(bdata::jsonb, '{data,payments}', (( bson_get_jsonb_array(bdata,'data.payments') || %s) - 0))::bson
    """

    curs.execute(sql, (rb7,));
    conn.commit()
        
    return msg




def basic_internal_update():
    insertBson(sdata)

    msg = None
    
    # cool!
    curs.execute("UPDATE bsontest set bdata2 = bdata")

    # Basic internal matching:
    rc = fetchRow1Col("SELECT bdata2 = bdata FROM bsontest")
    if rc != True:
        msg = "internal update of bdata2 = bdata do not equal"

    # Go for roundtrip:
    (rb1, rb2) = fetchRowNCol("SELECT bdata::bytea, bdata2::bytea FROM bsontest",2)   
    if rb1 != rb2:
        msg = "fetch of bdata2 and bdata yields non-equal BSON"

    rb4 = rb1
    if True:
        # Mess with rb2 -- BUT in a way that does not corrupt BSON itself.
        # So go to end and back up 2 bytes to leave the trailing NULL intact:
        idx = len(rb2) - 2    
        rb3 = bytearray(rb2)
        rb3[idx] = 217  # arbitrary
        idx -= 1
        rb3[idx] = 119  # arbitrary
        rb4 = bytes(rb3)

    curs.execute("UPDATE bsontest set bdata2 = %s", (rb4,))

    rc = fetchRow1Col("SELECT bdata2 = bdata FROM bsontest")
    if rc != False:
        msg = "bdata2 should not equal bdata after deliberate hack"
        
    return msg

    
    

#  Regression test for bson_recv(), the type's binary receive function.  It
#  used to hand libbson the StringInfo *struct* rather than the message bytes
#  in it, so any binary format input read garbage and could crash the
#  backend.  psycopg2 sends text format, so nothing else in this suite ever
#  reached it; COPY (FORMAT binary) does.  Out through bson_send, back in
#  through bson_recv, and the bytes must be identical.
def binary_copy_roundtrip():
    import io

    rb = insertBson(sdata)

    buf = io.BytesIO()
    curs.copy_expert("COPY (SELECT bdata FROM bsontest) TO STDOUT (FORMAT binary)", buf)
    buf.seek(0)

    curs.execute("TRUNCATE TABLE bsontest")
    curs.copy_expert("COPY bsontest (bdata) FROM STDIN (FORMAT binary)", buf)

    item = fetchRow1Col("SELECT bdata::bytea FROM bsontest")
    if bytes(item) != rb:
        return "bytes after binary COPY round trip do not match"

    return None


#  Binary input gets the same validation as bytea::bson.  Hand build a
#  binary COPY stream around the corrupt bytes from cast_corrupt_bson: the
#  COPY must fail cleanly, and the connection must still be usable after
#  (a crashed backend would also "fail", so check that it did not).
def binary_copy_rejects_corrupt():
    import io
    import struct

    bad = bytes([0x0e, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00,0x00,0x41,0x00, 0x00])

    stream = (b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)
              + struct.pack("!hi", 1, len(bad)) + bad
              + struct.pack("!h", -1))

    try:
        curs.copy_expert("COPY bsontest (bdata) FROM STDIN (FORMAT binary)", io.BytesIO(stream))
        return "corrupt BSON accepted by binary COPY"
    except psycopg2.OperationalError as err:
        return "connection lost; backend crashed? %s" % err
    except Exception:
        pass  # good:  "invalid BSON"

    conn.rollback()
    if fetchRow1Col("SELECT 1") != 1:
        return "connection unusable after rejected binary COPY"

    return None


#  BSON datetime is signed millis since 1970, so pre-1970 values are
#  negative and the seconds/millis split has to round toward -infinity, not
#  zero.  Values beyond postgres' timestamp range (4713 BC - 294276 AD) must
#  be NULL, not garbage.
def datetime_edges():
    epoch = datetime.datetime(1970,1,1)
    ms = datetime.timedelta(milliseconds=1)

    for millis, expected in [
            (0,     epoch),
            (1,     epoch + ms),
            (-1,    epoch - ms),
            (-500,  epoch - 500*ms),
            (-999,  epoch - 999*ms),
            (-1000, epoch - 1000*ms),
            (-1500, epoch - 1500*ms),
            (-86400000 * 365 * 100 - 1, epoch - (86400000 * 365 * 100 + 1)*ms),
            ( 9000000000000000000, None),
            (-9000000000000000000, None)
    ]:
        sql = """SELECT bson_get_datetime('{"d":{"$date":{"$numberLong":"%d"}}}'::bson, 'd')""" % millis
        item = fetchRow1Col(sql)
        if item != expected:
            return "%d millis: got %s, expected %s" % (millis, item, expected)

    return None


#  BSON strings may contain a NUL; postgres text may not.  Truncating at the
#  NUL is the documented behavior for both bson_get_string and ->>.
def embedded_nul_truncates():
    insertBson({"s": "abc\x00def"})

    for sql in ["SELECT bson_get_string(bdata, 's') FROM bsontest",
                "SELECT bdata->>'s' FROM bsontest"]:
        item = fetchRow1Col(sql)
        if item != "abc":
            return "%s: got %r, expected 'abc'" % (sql, item)

    return None


#  ->> on a date renders ISO8601 UTC, ".mmm" only when non-zero.  Includes
#  pre-epoch, leap day, century boundaries and the extremes of 4 digit years.
def datetime_as_text_format():
    for iso in ["1970-01-01T00:00:00Z",
                "1969-12-31T23:59:59.999Z",
                "1969-12-31T23:59:59Z",
                "1900-03-01T00:00:00Z",
                "2000-02-29T12:34:56.789Z",
                "2100-03-01T00:00:00.001Z",
                "0001-01-01T00:00:00Z",
                "9999-12-31T23:59:59.999Z"]:
        curs.execute("TRUNCATE TABLE bsontest")
        curs.execute("INSERT INTO bsontest (bdata) VALUES (%s::bson)",
                     ('{"d": {"$date": "%s"}}' % iso,))
        conn.commit()
        got = fetchRow1Col("SELECT bdata->>'d' FROM bsontest")
        if got != iso:
            return "date %s rendered as %r" % (iso, got)

    return None


#  bson_array_as_json() returns NULL for an array holding a string that is
#  not valid UTF-8, and bson_get_jsonb_array() used to hand that NULL to
#  jsonb_in(), crashing the backend.  Whether the bytes get past the bytea
#  cast depends on write-side validation; either a clean error or a NULL is
#  fine here.  What is not fine is the connection dying.
def jsonb_array_bad_utf8_no_crash():
    # {"a": ["\xff"]}
    d = bytes([0x16,0,0,0, 0x04,0x61,0, 0x0e,0,0,0, 0x02,0x30,0, 0x02,0,0,0,0xff,0, 0, 0])
    try:
        curs.execute("SELECT bson_get_jsonb_array(%s::bytea::bson, 'a')", (d,))
        curs.fetchall()
    except psycopg2.OperationalError as e:
        conn.rollback()
        return "backend died: %s" % e
    except Exception:
        pass  # clean ERROR is acceptable
    conn.rollback()

    #  Prove the session still works:
    if fetchRow1Col("SELECT 1") != 1:
        return "connection unusable after bad array"

    return None


#  Strings and keys must be valid UTF-8 on the way in.  This goes through the
#  bytea::bson cast; binary COPY shares _validate_and_copy.  Otherwise a row
#  can make bson_out() error, which breaks SELECT, pg_dump and the json casts
#  for that row.
def reject_invalid_utf8():
    bad = {
        # {"s": "\xff"}
        "bad string": bytes([0x0e,0,0,0, 0x02,0x73,0, 0x02,0,0,0,0xff,0, 0]),
        # {"\xff": 1}
        "bad key":    bytes([0x0c,0,0,0, 0x10,0xff,0, 0x01,0,0,0, 0]),
    }
    for what, d in bad.items():
        try:
            curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)", (d,))
            conn.rollback()
            return "%s accepted by bytea insert" % what
        except psycopg2.Error:
            conn.rollback()

    # Multi-byte UTF-8 must still go in, and come out intact:
    insertBson({"s": "h\u00e9llo \u20ac \U0001F600"})
    got = fetchRow1Col("SELECT bson_get_string(bdata, 's') FROM bsontest")
    if got != "h\u00e9llo \u20ac \U0001F600":
        return "valid UTF-8 did not round trip: %r" % got
    if fetchRow1Col("SELECT bdata::text FROM bsontest") is None:
        return "bson_out failed on valid UTF-8"

    return None


#  BSON strings are UTF-8 but postgres text must be in the server encoding.
#  Needs a non-UTF-8 database to mean anything, so make a throwaway LATIN1
#  one; if we are not allowed to, there is nothing to check.
def text_in_server_encoding():
    from psycopg2 import extensions
    dbname = "pgbson_latin1_test"
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    ac = admin.cursor()
    try:
        ac.execute("DROP DATABASE IF EXISTS %s" % dbname)
        ac.execute("CREATE DATABASE %s TEMPLATE template0 ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'" % dbname)
    except psycopg2.Error as e:
        print("  (cannot create LATIN1 database, skipping: %s)" % str(e).strip())
        admin.close()
        return None

    msg = None
    try:
        c2 = psycopg2.connect(extensions.make_dsn(DSN, dbname=dbname))
        c2.autocommit = True
        cu = c2.cursor()
        cu.execute("CREATE EXTENSION pgbson")
        cu.execute("SELECT 1 FROM pg_database WHERE datname = current_database() AND pg_encoding_to_char(encoding) = 'LATIN1'")
        if cu.fetchone() is None:
            raise Exception("test database is not LATIN1")

        # "caf\u00e9" is 4 characters: 5 bytes in UTF-8, 4 in LATIN1
        doc = safe_bson_encode({"s": "caf\u00e9"})
        for expr in ["bson_get_string(%s::bytea::bson, 's')",
                     "(%s::bytea::bson)->>'s'"]:
            cu.execute("SELECT octet_length(" + expr + ")", (doc,))
            n = cu.fetchone()[0]
            if n != 4:
                msg = "%s: %d bytes, expected 4 (not converted to server encoding?)" % (expr, n)
                break

        # No equivalent in LATIN1: a clean ERROR, not mojibake
        euro = safe_bson_encode({"s": "\u20ac"})
        try:
            cu.execute("SELECT bson_get_string(%s::bytea::bson, 's')", (euro,))
            cu.fetchall()
            msg = msg or "unrepresentable character did not raise an error"
        except psycopg2.Error:
            pass
        c2.close()
    finally:
        ac.execute("DROP DATABASE IF EXISTS %s" % dbname)
        admin.close()

    return msg


#  bson_hash moved from signed int (overflow is undefined behavior) to
#  uint32.  The bits must be exactly the old wrapping DJB2, or existing hash
#  indexes would silently stop finding rows.
def hash_bits_unchanged():
    rb = insertBson(sdata)

    h = 5381
    for c in rb:
        h = (h * 33 + c) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000  # int4 as postgres returns it

    item = fetchRow1Col("SELECT bson_hash(bdata) FROM bsontest")
    if item != h:
        return "got %s, expected %s" % (item, h)

    return None


def basic_roundtrip(args):
    """If this does not work, the whole thing is pointless."""

    pydata = args[0]

    msg = None

    insertBson(pydata)
    
    bson1 = fetchRow1Col('SELECT bdata::bytea FROM bsontest')

    # This is the critical part.  It shows we can convert to a dict
    # hen re-encode it and save it back:
    doc = safe_bson_decode(bson1)
    bson2 = safe_bson_encode(doc)

    curs.execute("INSERT INTO bsontest (marker, bdata) values (%s,%s)",
                 ("ZZZ", bson2))
    
    bson3 = fetchRow1Col("SELECT bdata::bytea FROM bsontest where marker = 'ZZZ'")
    
    if bytes(bson1) != bytes(bson3):
        msg = "roundtrip bytes do not equal"
    
    return msg
        


def create_view():
    msg = None
    try:
        curs.execute("""
        CREATE VIEW conv1 AS
        select
        bson_get_string(bdata, 'data.recordId') as recordId,
        bson_get_datetime(bdata, 'data.txDate') as ts,
        bson_get_decimal128(bdata, 'data.amt') as amt,    
        bdata as bdata
        from bsontest
        """)
    except Exception as err:    
        msg = str(err)  # invoke "toString"
        
    conn.commit()

    return msg
    




    
    
def main(argv):
    parser = argparse.ArgumentParser(description=
"""A reasonable test suite for postgres BSON.
We use python and the bson and psycopg2 modules because that is the target
use case; bson "on its own" in and out of postgres is not very interesting.
"""
   )

    parser.add_argument('--verbose',
                        action='store_true',
                        help='Chat')

    rargs = parser.parse_args()

    init(rargs)

    #  Tests with - will be run.
    #  Replace one or more '-' with 'S' for (S)olo.  Like a mixing board,
    #  this will only run tests marked 'S'. Note that once any one test is
    #  marked (S), all other tests are skipped and NOT reported as such.
    #
    #  Replace one or more '-' with 'M' for (M)ute.  The test will be
    #  skipped but it WILL be reported as such.  
    #  
    #  Standard way to add test is set it up solo to start, get it to work,
    #  then un-solo to add it to the overall test suite.
    #
    all_tests = [
        {'M':jsonb_test }  # No need to rub in the salt....

        
        ,{'-':cast_insert_good_json}
        ,{'-':cast_short_bson}
        ,{'-':cast_badnull_bson}
        ,{'-':cast_corrupt_bson}                

        ,{'-':basic_roundtrip, 'desc':"roundtrip smallest BSON", "args":[{'A':'X'}]}
        ,{'-':basic_roundtrip, 'desc':"roundtrip BIG structure", "args":[sdata]}    
        ,{'-':toast_test }
        ,{'-':bson_test }
        ,{'-':double_as_text}
        ,{'-':bool_oid_as_text}
        ,{'-':scalar_arrow_returns_null}
        ,{'-':get_value_wraps_everything}
        ,{'-':binary_copy_roundtrip}
        ,{'-':binary_copy_rejects_corrupt}
        ,{'-':datetime_edges}
        ,{'-':embedded_nul_truncates}
        ,{'-':hash_bits_unchanged}
        ,{'-':datetime_as_text_format}
        ,{'-':jsonb_array_bad_utf8_no_crash}
        ,{'-':reject_invalid_utf8}
        ,{'-':text_in_server_encoding}
        ,{'-':extension_upgrade_path}
        ,{'-':basic_internal_update}

        ,{'-':check1, 'desc':"string exists",
          "args": ["SELECT bson_get_string(bdata, 'header.type') FROM bsontest", "X"] }
        ,{'-':check1, 'desc':"string !exists",
          "args": ["SELECT bson_get_string(bdata, 'header.NOT_IN_FILM') FROM bsontest", None] }
        ,{'-':check1, 'desc':"nested string exists",
          "args": ["SELECT bson_get_string(bdata, 'data.sub1.sub2.corn') FROM bsontest", 'dog'] }
        ,{'-':check1, 'desc':"decimal exists",
          "args": ["SELECT bson_get_decimal128(bdata, 'data.amt') FROM bsontest", a_decimal ] }
        ,{'-':check1, 'desc':"datetime exists",
          "args": ["SELECT bson_get_datetime(bdata, 'data.txDate') FROM bsontest", a_datetime ] }

        #  ->> is bson_as_text(), a different code path than bson_get_datetime()
        #  above.  The trailing "Z" used to get chopped: bson_strncpy() takes
        #  the size of the DESTINATION, but was being handed the length of the
        #  source.  a_datetime is 6-Jun-2022 12:13:14.500 UTC.
        ,{'-':check1, 'desc':"datetime as text keeps trailing Z",
          "args": ["SELECT bdata->'data'->>'txDate' FROM bsontest",
                   "2022-06-06T12:13:14.500Z" ] }

        #  Likewise ->> on a double.  Used to come back "3.141593" because
        #  "%lf" rounds to 6 decimal places.  See double_as_text().
        ,{'-':check1, 'desc':"double as text keeps precision",
          "args": ["SELECT bdata->'data'->'userPrefs'->'1'->'u'->>'pi' FROM bsontest",
                   "3.1415926" ] }

        #  sdata has carried header.active all along; ->> just never asked
        #  for it, so the bool returning NULL went unnoticed.
        ,{'-':check1, 'desc':"boolean as text",
          "args": ["SELECT bdata->'header'->>'active' FROM bsontest", "true" ] }

        ,{'-':binary_checks}  # should be broken up....

        # '0' in quotes for bson...        
        ,{'-':check1, 'desc':"bson arrow nav",
          "args": [ """
select bson_get_string(bdata,'header.evId') from bsontest
where bdata->'data'->'userPrefs'->'0'->>'type' = 'DEP'
    """, "E23234" ] }

        # 0 is integer for jsonb; note also the jsonb cast...
        ,{'-':check1, 'desc':"jsonb cast arrow nav",
          "args": [ """
select bson_get_string(bdata,'header.evId') from bsontest
where (bdata->'data')::jsonb->'userPrefs'->0->>'type' = 'DEP'
    """, "E23234" ] }

        ,{'-':create_view}

        ,{'-':check1, 'desc':"scalar ts via view",
          "args": [ "SELECT ts FROM conv1 where recordId = 'ID0'", a_datetime ] }

        # Check a_datetime above; it is AFTER midnight on 6/6/2022:
        ,{'-':check1, 'desc':"scalar recordId via view",
          "args": [ "SELECT recordId FROM conv1 where ts > '2022-06-06'::date",'ID0' ] }

        # We use checks of actual python expressions (e.g. a_decimal * 2)
        # because that is the use case we are testing against.  It's not just
        # the number; it is the path you take in computing it.        
        ,{'-':check1, 'desc':"fancy multiplication via view",
          "args": [ "SELECT amt * 2 FROM conv1 where recordId = 'ID0'", a_decimal * 2 ] }


        # Need to make 5.55 Decimal for proper handling with a_decimal!
        ,{'-':check1, 'desc':"fancy subtraction via view",
          "args": [ "SELECT amt - 5.55 FROM conv1 where recordId = 'ID0'",
                    a_decimal - Decimal("5.55")
                    ]}

        ,{'-':ejson_builtin_test1} # pretty cool
        ,{'-':ejson_builtin_test2}

        # Cannot directly cast BSON bytea to jsonb in complex expression.
        # That is OK; leave it muted for now.
        ,{'M':array_update}
    ]

    tests = []
    
    for t in all_tests:
        if 'S' in t:
            # Solos exist; rework the tests list to contain only solos:
            for t2 in all_tests:
                if 'S' in t2:
                    t2['-'] = t2['S']
                    tests.append(t2)
            break

    if len(tests) == 0:
        tests = all_tests # no solos, so use all_tests

    # Get the longest description for nice formatting
    maxnlen = 0
    for t in tests:
        if 'M' in t:
            fn = t['M']
        elif '-' in t:
            fn = t['-']
        if 'desc' not in t:
            t['desc'] = fn.__name__
        x = len(t['desc'])
        if x > maxnlen:
            maxnlen = x

    class Color:
        GREEN = '\033[92m'
        RED = '\033[91m'
        RESET = '\033[0m'
            
    for t in tests:
        if 'M' in t:
            print(f"{t['desc']:<{maxnlen}} : skip")
            continue
        elif '-' in t:
            ff = t['-']
        else:
            ff = t['S']

        try:
            if 'args' in t:
                msg = ff(t['args'])
            else:
                msg = ff()
        except Exception as err:
            msg = str(err)

        if msg is not None:
            msg = f": {Color.RED}FAIL{Color.RESET}: {msg}"
        else:
            msg = f": {Color.GREEN}ok{Color.RESET}"
            
        print(f"{t['desc']:<{maxnlen}}",msg)

        conn.rollback() # always recover...
    
if __name__ == "__main__":        
    main(sys.argv)
