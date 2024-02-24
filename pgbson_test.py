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

    # psycopg2 will by default turn jsonb into a dict...
    #print(j2.__class__,":",j2)
    # ... and turning that into a JSON string...
    s2 = json.dumps(j2)
    # ... of course yields a DIFFERENT hash:
    h3 = hashlib.sha256(bytes(s2,'utf-8')).hexdigest()
    #print("incoming hash:", h3)
    #print("hashes the same?", h2 == h3)


    # Attempt 1:  Fetch casting to string, which will return JSON directly:
    s2 = fetchRow1Col('SELECT jbdata::text FROM bsontest' )
    #print(s2.__class__,":",s2)
    # ... but of course, same problem:
    h3 = hashlib.sha256(bytes(s2,'utf-8')).hexdigest()
    #print("incoming hash:", h3)
    #print("hashes the same?", h2 == h3)    

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
        bb = bytes('Z', 'utf-8')
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
