#
#  Yes.  We use the bson and psycopg2 modules.
#  Why?
#  1.  Programmatic interaction with postgres is much more specific with
#      respect to datatypes and that is the main use case, not throwing
#      SQL at it from the psql CLI.
#  2.  We can test SHA3 and roundtripping *much easier* from a program.
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
a_datetime = makeDatetime(2022,6,6,12,13,14,500)

sdata = {
    "header": {
	"ts": makeDatetime(2022,5,5,12,13,14,456),
        "evId":"E23234",
        "type":"X"
    },
    "data": {
        "id":"ID0",
        "notIndexed":"N0",        
	"amt": makeDecimal128(a_decimal),  # must do this...
        "txDate": a_datetime,
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
            ]
    }
}

raw_bson = safe_bson_encode(sdata)
    
curs = conn.cursor()

#  The test table is called BSONTEST.  I don't want to go through the
#  drudgery of making it a variable.
#
def init(rargs):

    print("dropping/creating BSONTEST...")

    sql = """
drop table if exists bsontest;
drop extension if exists pgbson CASCADE;

create extension pgbson;

create table bsontest (
    marker text,
    bdata BSON,
    bdata2 BSON,
    jbdata jsonb,
    jdata json
    );
"""
    try:
        curs.execute(sql);
    except Exception as errmsg:
        print("FAIL: cannot init environment:", errmsg)
        print("check that extension was installed (make install) and/or DB permissions are set properly")
        sys.exit(1)
        
    conn.commit()


def insert():
    #  Construct JSON equiv.  Datetimes won't go in so we need a custom
    #  converter to string (this is living proof of a BSON advantage...)
    def cvt(o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        if isinstance(o, bson.binary.Binary):
            return str(o) 
        if isinstance(o, bson.decimal128.Decimal128):
            return str(o) 

    jstr = json.dumps(sdata,default=cvt)

    curs.execute("TRUNCATE TABLE bsontest")
    curs.execute("INSERT INTO bsontest (bdata,jdata,jbdata) VALUES (%s,%s,%s)",
                 (raw_bson,jstr,jstr))
    conn.commit()


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
    
def fetch1(sql):
    item = None
    curs.execute(sql)
    all_recs = curs.fetchall()
    if len(all_recs) != 1:
        print("did not find 1 record")
    else:
        item = all_recs[0][0]  # Get column 1 of record 1
    return item

def fetch2(sql):
    item = None
    curs.execute(sql)
    all_recs = curs.fetchall()
    if len(all_recs) != 1:
        print("did not find 1 record")
    else:
        item1 = all_recs[0][0]  # Get column 1 of record 1
        item2 = all_recs[0][1]  # Get column 2 of record 1
    return (item1, item2)

def check1(msg, sql, expected):
    err = ""
    item = fetch1(sql)
    if item != None:
        if type(item) is memoryview:
            # Love memoryview for performance -- but here we need to
            # hydrate to get the bytes...
            item = bytes(item)
        if expected != item:
            err = "got\n%s::%s\nexpected\n%s::%s" % (item,type(item),expected,type(expected))
        
    if err != "":
        print("%s...FAIL; %s" % (msg,err))
    else:
        print("%s...ok" % msg)
    


def toast_test():
    """Put a big obj in there.  Look at 
https://hakibenita.com/sql-medium-text-performance to get an idea of how
to check that the internals are actually doing something."""

    curs.execute("TRUNCATE TABLE bsontest");

    # A big chunk of data that lz cannot compress (thanks to randomness) to fit
    # in a page.  This will force TOASTing:
    rand_bytes = os.urandom(16000)

    data = {
        "id":"ID0",
        "notIndexed":"N0",        
        "biggie": bson.binary.Binary(rand_bytes)
    }

    bson1 = safe_bson_encode(data)
    
    curs.execute("INSERT INTO bsontest (bdata) VALUES (%s)",
                 (bson1,))

    rb2 = fetch1('SELECT bdata::bytea FROM bsontest')    

    # ALT:  select length(bson_get_binary(bdata, 'biggie')) from bsontest;

    msg = "ok"
    if bson1 != bytes(rb2):  # For compare, must be bytes-to-bytes!
        msg = "FAIL; roundtrip of TOAST sized content does not equal"
    
    print("toast_test...%s" % msg)    
        
    conn.commit()
    

def basic_internal_update():

    insert()

    msg = None
    
    # cool!
    curs.execute("UPDATE bsontest set bdata2 = bdata")

    # Basic internal matching:
    rc = fetch1("SELECT bdata2 = bdata FROM bsontest")
    if rc != True:
        msg = "FAIL; internal update of bdata2 = bdata do not equal"

    # Go for roundtrip:
    (rb1, rb2) = fetch2("SELECT bdata::bytea, bdata2::bytea FROM bsontest")   
    if rb1 != rb2:
        msg = "FAIL; fetch of bdata2 and bdata yields non-equal BSON"

    rb4 = rb1
    if True:
        # Mess with rb2 -- BUT in a way that does not corrupt BSON itself.
        # So go to end and back up 2 bytes to leave the trailing NULL intact:
        idx = len(rb2) - 2    
        rb3 = bytearray(rb2)
        rb3[idx] = 217
        idx -= 1
        rb3[idx] = 119
        rb4 = bytes(rb3)

    curs.execute("UPDATE bsontest set bdata2 = %s", (rb4,))

    rc = fetch1("SELECT bdata2 = bdata FROM bsontest")
    if rc != False:
        msg = "FAIL; bdata2 should not equal bdata after deliberate hack"
    
    if msg is None:
        msg = "ok"
        
    print("basic_internal_update...%s" % msg)    

    
    
def basic_roundtrip():
    """If this does not work, the whole thing is pointless."""

    insert()
    
    bson1 = fetch1('SELECT bdata::bytea FROM bsontest')

    # This is the critical part.  It shows we can ADD a field to a dict
    # then remove it, then re-encode it and save it back
    doc = safe_bson_decode(bson1)
    doc['header']['NEW_FIELD'] = 'ZZZ'
    del doc['header']['NEW_FIELD']
    bson2 = safe_bson_encode(doc)

    curs.execute("INSERT INTO bsontest (marker, bdata) values (%s,%s)",
                 ("ZZZ", bson2))
    
    bson3 = fetch1("SELECT bdata::bytea FROM bsontest where marker = 'ZZZ'")
    
    msg = "ok"

    if bytes(bson1) != bytes(bson3):
        msg = "FAIL; roundtrip bytes do not equal"
    
    print("basic_roundtrip...%s" % msg)
        

def scalar_checks():
    insert()

    check1("basic roundtrip", 'SELECT bdata::bytea FROM bsontest', raw_bson)

    check1("string exists", "SELECT bson_get_string(bdata, 'header.type') FROM bsontest", "X")
    check1("string !exists", "SELECT bson_get_string(bdata, 'header.NOT_IN_FILM') FROM bsontest", None)
    check1("decimal exists", "SELECT bson_get_decimal128(bdata, 'data.amt') FROM bsontest", a_decimal)
    check1("datetime exists", "SELECT bson_get_datetime(bdata, 'data.txDate') FROM bsontest",       a_datetime)


def arrow_checks():
    check1("jsonb cast arrow nav", "select (bdata->'data')::jsonb->'userPrefs'->0->'type' from bsontest", "DEP")

    # TBD:  Have to do something about '0' vs. 0.  At least document it....
#check1("bson arrow nav", "select bdata->'data'->'userPrefs'->'0'->>'type' from bsontest", "DEP")

    #check1("bson_get_bson", "select bson_get_bson(bdata,'data.userPrefs.0')::bytea from bsontest", "X")
    pass
    
        
def main(argv):
    parser = argparse.ArgumentParser(description=
"""A reasonable test suite for postgres BSON.
We use python and the bson and psycopg2 modules because that is the target
use case; bson "on its own" in and out of postgres is not very interesting.
"""
   )

    parser.add_argument('--init',
                        action='store_true',
                        help='Drop tables, reload extension, and recreate test table.  Recommended for all tests except mega-inserts')


    parser.add_argument('--verbose',
                        action='store_true',
                        help='Chat')
        

    rargs = parser.parse_args()

    if rargs.init:
        init(rargs)


    basic_roundtrip()
    toast_test()
    basic_internal_update()
    scalar_checks()
    arrow_checks()        



if __name__ == "__main__":        
    main(sys.argv)
