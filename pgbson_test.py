#
#  Yes.  We use the python and the bson and psycopg2 modules.
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
import hashlib
import psycopg2
from decimal import Decimal
import datetime

import json
import sys
import os


import collections  # From Python standard library.
from bson.codec_options import CodecOptions
codec_options = CodecOptions(document_class=collections.OrderedDict)

def safe_bson_decode(bson_bytea):
    return bson.decode(bson_bytea, codec_options=codec_options)

def safe_bson_encode(doc):
    return bson.BSON.encode(doc) # For symmetry with decode


# python datetime has res to micros but BSON is only to millis.
# optional no millis
def makeDatetime(y, m, d, h, min, sec, millis=0):
    return datetime.datetime(y,m,d,h,min,sec,millis*1000)    
    
    
def makeDecimal128(str_val):
    return bson.decimal128.Decimal128(Decimal(str_val))

conn = psycopg2.connect(DSN)

# A big chunk of data that lz cannot compress to fit in a page.  This
# will force TOASTing:
rand_bytes = os.urandom(8096)

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
def init():

    print("dropping/creating BSONTEST...")

    sql = """
drop table if exists bsontest;
drop extension if exists pgbson CASCADE;

create extension pgbson;

create table bsontest (
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

    print("inserting data into BSONTEST...")

    curs.execute("INSERT INTO bsontest (bdata,jdata,jbdata) VALUES (%s,%s,%s)",
                 (raw_bson,jstr,jstr))
    conn.commit()


def check1(msg, sql, expected):
    err = ""
    curs.execute(sql)
    all_recs = curs.fetchall()
    if len(all_recs) != 1:
        err = "did not find 1 record"
    else:
        item = all_recs[0][0]  # Get column 1
        if type(item) is memoryview:
            item = bytes(item)
        if expected != item:
            err = "got [%s]::%s, expected [%s]::%s" % (item,type(item),expected,type(expected))

    if err != "":
        print("%s...FAIL; %s" % (msg,err))
    else:
        print("%s...ok" % msg)
    




init()
insert()

check1("basic roundtrip", 'SELECT bdata::bytea FROM bsontest', raw_bson)
check1("string exists", "SELECT bson_get_string(bdata, 'header.type') FROM bsontest", "X")
check1("string !exists", "SELECT bson_get_string(bdata, 'header.NOT_IN_FILM') FROM bsontest", None)
check1("decimal exists", "SELECT bson_get_decimal128(bdata, 'data.amt') FROM bsontest", a_decimal)
check1("datetime exists", "SELECT bson_get_datetime(bdata, 'data.txDate') FROM bsontest",       a_datetime)


check1("jsonb cast arrow nav", "select (bdata->'data')::jsonb->'userPrefs'->0->'type' from bsontest", "DEP")

# TBD:  Have to do something about '0' vs. 0.  At least document it....
check1("bson arrow nav", "select bdata->'data'->'userPrefs'->'0'->>'type' from bsontest", "DEP")
    

sys.exit(0)

