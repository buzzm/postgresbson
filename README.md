postgresbson
============

BSON support for PostgreSQL

Introduction
============

This PostgreSQL extension realizes the BSON data type, together with functions to create and inspect BSON objects for the purposes of expressive and performant
querying.


BSON (http://bsonspec.org/) is a high-performance, richly-typed data carrier
similar to JSON but offers a number of attractive features including:

 *  Datetimes, decimal (numeric), and byte[] are first class types.  In pure
    JSON these must all be represented as a string, requiring conversion,
    potentially introducing lossiness, and impairing native operations
    like `>` and `<=`.
 *  Performance.  Moving binary BSON in and out of the database under some
    conditions is almost 10x faster than using native `jsonb` or `json` because
    it avoids to- and from-string and to-dictionary conversion.
 *  Roundtrip ability.  BSON is binary spec, not a string.  There is no whitespace,
    quoting rules, etc.  BSON that goes into Postgres comes out *exactly* the
    same way, each time, every time.
 *  Standard SDK implementations in upwards of 20 languages

Roundtripping and many available language SDKs enables seamless creation,
manipulation, transmission, and querying of data in a distributed system without
coordinating IDLs, compile-time dependencies, nuances in platform type representation, etc.   Here is an example of a typical processing chain: Java program -> message bus -> python util -> save to database -> other Java program wakes up on
insert trigger:

  1.  Java program constructs an `org.bson.Document` (which honors `java.util.Map` interface), e.g. `doc.put("name", "Steve")`, `doc.put("balance", new BigDecimal("143.99"))` etc.
  2.  Java program encodes `org.bson.Document` using Java BSON SDK to a BSON `byte[]`.
  3.  Java program publishes `byte[]` to a Kafka topic.
  4.  python listener wakes up on topic and receives `byte[]`.
  5.  python listener decodes `byte[]` using python BSON SDK into `dict`, **not**
      a string.  It is a fully reconstituted `dict` object with substructures, `datetime.datetime` for date fieldss, `Decimal` for penny-precise fields, etc.  The listener prints some things but does not change anything in the dict.
  6.  python listener encodes `dict` to back to `byte[]` and `INSERT`s it to a BSON column in Postgres using the `psycopg2` module.
  7.  A different Java program wakes up on a Postgres insert trigger and `SELECT`s
  the BSON column as a `byte[]` (e.g. `select bson_column::bytea where ...`)
  8.  This `byte[]` is *identical* to the one created in step 2.
 
Another example of roundtrip ability involves hashes and digital signatures. BSON
offers an easy, robust, and precise means to associate arbitrarily complex data
with hashes thereof in a way that is basically not possible or certainly not
*reliable* using JSON or JSONB:
```
    #  Create complex data.  Note array g is polymorphic:
    amt = bson.decimal128.Decimal128(Decimal("-10267.01"))
    data = {"dt":datetime.datetime.utcnow(),"amt":amt,"msg":"Hello!","g":["2",2,2.0,{'v':2}]}

    # Probably in a util function, take data and associate it with metadata.
    # Get the SHA2 of this data:
    raw = bson.encode(data)
    h2 = hashlib.sha256(raw).hexdigest()
    meta = {'security':{'hash':{'algo':'SHA256','v':h2}}}
    doc = {'d': data,'m': meta}

    # Encode the *whole* doc (data + meta) and insert into BSON column:
    rb2 = bson.encode(doc)
    curs.execute('insert into foo (bson_data) values (%s);', (rb2,))
    conn.commit()

    #  -- L A T E R --
    
    # Pull the material back out of the database.  Be sure to cast to
    # bytea so the psycopg2 does not try to turn it into an EJSON string!
    curs.execute("select bson_data::bytea from foo;")
    all_recs = curs.fetchall()

    # bytea comes in as type 'memoryview' for efficiency; get the bytes
    # from row 0, column 0:
    r = bytes(all_recs[0][0]) 

    doc2 = bson.decode(r, codec_options=CodecOptions(document_class=collections.OrderedDict))


    # Extract the stored hash value:
    h3 = doc2['m']['security']['hash']['v']

    # REHASH the data portion:
    r2 = bson.encode(doc2['d'])
    h4 = hashlib.sha256(r2).hexdigest()

    if h3 == h4:
        print("rehash of data OK")
    else:
        print("WARN: rehash of data does not match associated hash")
```


The extension offers two kinds of accessor suites:

 *  Typesafe high performance accessor functions that take a dotpath notation
    to get to a field e.g.<br>
    ```
    select bson_get_datetime(bson_column, 'msg.header.event.ts') from table;
    ```
 *  Arrow and double arrow operators similar to those found in the JSON type e.g.<br>
    ```
    select (bson_column->'msg'->'header'->'event'->>'ts')::timestamp from table;
    ```
    
The BSON type is castable to JSON in so-called [EJSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/) format to preserve type fidelity.
Thus, the wealth
of functions and operations and even other extensions built around the JSON type
can be used on BSON.  These of course can be combined:

    -- Use dotpath to quickly get to event substructure, then cast to jsonb and
    -- use ?& operator to ask if both `id` and `type` are present as top level tags:
    select (bson_get_bson(bson_column, 'msg.header.event')::jsonb) ?@ array['id','type'] from table;


Why dotpath accessors are better in BSON (*and* native json and jsonb, too)
---------------------------------------------------------------------------
In general, the dotpath functions will be much faster and memory efficient
especially for larger and/or deeper structures.  This is because the dotpath
implementation in the underlying C library itself will "walk" the BSON structure
and only vend allocated material at the terminal of the path.  The arrow operators
necessitate the construction of a fully inflated substructure at each step in
the path, which is exactly what happens with arrow operators and the native
`json` and `jsonb` types.  For example, consider this instance with approx. 3K of
data, from which we want to query where `id` is `AAA`:
```
  {
    header: { event: {id: "E123"} },
    data: {
        payload: {
	    product: {
	        definition: {
	            id: "AAA",
		    ... another 1K of data ...
	        },
	        constraints: {
		    ... another 1K of data ...
	        },
	        approvals: {
		    ... another 1K of data ...
                }
	    }
	}
    }
  }
```
The dotpath method will make postgres pass the 3K structure to the BSON extension (internally of course, not back to the client!), which will only have to examine roughly 64 bytes of data to dig down to the `id` field to return a string which is then examined for equality to `AAA`:
```
select * from table where bson_get_string(bson_column, 'data.payload.product.definition.id') = 'AAA';
```

With arrow operators, *at each arrow*, almost 3K of data has to be processed:
```
   Remember double arrow operator yields text type, which fortunately is easily
   compared to a literal string; no casting necessary, but the journey there is
   tough because each single arrow forces construction of a whole new object to
   pass to the next stage.  This happens internal to the engine but still...
   
select * from table where
bson_column->'data'->'payload'->'product'->'definition'->>'id' = 'AAA';
           ^       ^          ^          ^             ^     ^          
           |       |          |          |             |     | 
           +- initial pull of a little more than 3K    |     | 
                   |          |          |             |     |                 
                   +- almost 3K reprocessed            |     |
                              |          |             |     |
                              +- another 3K reprocessed|     |
                                         |             |     |
                                         +- another 3K |     |
                                                       |     |     
                                                       +- about 1K here
                                                             |
                                                             +- a handful of bytes

Total: about 13K of data in 4 separate pass **and** construct chunks of 3K processed to extract 3 bytes.
```      
Again, this is *exactly* the same situation that occurs with native `json` and `jsonb`
types using the arrow operators; it is *not* particular to BSON.  This is why postgres
provides the `#>` operator and the corresponding `json_extract_path` and `jsonb_extract_path` functions for these native types.




Arrays, BSON, and JSON
----------------------
Unlike JSON, BSON can only be created from a key:value object (which can be
empty); it cannot be created from a simple scalar or in particular an array.
In the course of descending into substructure, it is of course possible to
encounter an array and have that returned.  The BSON spec gets around this
problem by returning a string indexed object where the keys
are the integer representation of the location in the array starting at offset
zero.  From the CLI, this will be seen as:

    select bson_column->'d'->'payload'->'vector' from ...
      returns  {"0":"some value", "1":"another value", "2":{fldx:"a rich shape"}}
      not      ["some value", "another value", {fldx:"a rich shape"}]
    
The postgres array type does not help here because postgres requires a
homogenous type and BSON allows for heterogenous types.  

There are three ways to deal with this:

  1.  If you don't need the whole array, access individual items using strings; note the double arrow and the quotes around '0':
      ```
      select bson_column->'d'->'payload'->'vector'->>'0' from ...
      ```      	     						 

  2.  "Back up one level" in the arrow chain and cast to `jsonb`:
      ```
      mydb=# select (bson_column->'d'->'payload')::jsonb->'vector' from ...
       ?column?   
      --------------
      [21, 17, 19]

      Note no quotes around 0 because now it is jsonb:
      mydb=# select (bson_column->'d'->'payload')::jsonb->'vector'->>0 from ...
       ?column?   
      --------------
      21
      ```

  3.  Use the `bson_get_jsonb_array` function:
      ```
      mydb=# select bson_get_jsonb_array(bson_column,'d.payload.vector') from ...
       ?column?   
      --------------
      [21, 17, 19]

      mydb=# select bson_get_jsonb_array(bson_column,'d.payload.vector')->0 from ...
       ?column?   
      --------------
      21
      ```
      The subtle but at times very important benefit of `bson_get_jsonb_array`
      over approach #2 above is that the dotpath will quickly and efficiently
      navigate to just the `vector`.  The issue with "backing up one level" is
      that if `payload` is very big, the whole structure must be converted to
      `jsonb` only to pull out what could be a small `vector`.

      Remember: For objects, if you wish to use the `jsonb` functions, simply
      cast to `jsonb`:
      ```
      mydb=# select (bson_column,'d.payload')::jsonb ...
      ```

Status
======

Experimental.  All contribs / PRs / comments / issues welcome.


Example
=======

    CREATE EXTENSION pgbson;
    CREATE TABLE data_collection ( data BSON );

    -- Programmatic insert of material through postgres SDK e.g. psycopg2
    -- can use native types directly; this is this high-fidelity sweet spot
    -- for the BSON extension:

    import bson  
    import psycopg2

    import datetime

    conn = psycopg2.connect(DSN) # 'host=machine port=5432 dname=foo .... '
    sdata = {
        "header": {
    	"ts": datetime.datetime(2022,5,5,12,13,14,456),
            "evId":"E23234"
        },
        "data": {
            "id":"ID0",
            "notIndexed":"N0",        

            # BSON decimal128 adheres more strongly to IEEE spec than native
            # python Decimal esp. wrt NaN so we must use that.  We are
            # encoding to BSON later anyway so no extra dependencies here.
            "amt": bson.decimal128.Decimal128("107.78"),

            "txDate": datetime.datetime(2022,12,31),
            "userPrefs": [
            {"type": "DEP", "updated":datetime.datetime(2021,4,4,12,13,14),"u":{
                "favoriteCar":"Bugatti",
                "thumbnail": bson.binary.Binary(bytes("Pretend this is a JPEG", 'utf-8'))
                }},
            {"type": "X2A", "updated":datetime.datetime(2021,3,3,12,13,14),"u":{
                "listOfPrimes":[2,3,5,7,11,13,17,19],
                "atomsInBowlOfSoup": 283572834759209881,
                "pi": 3.1415926
                }}
            ]
        }
    }

    # raw_bson is byte[].  BSON is castable to/from bytea type in PG.
    # See pgbson--2.0.sql about byte[] validation to ensure that the bytea
    # being stored is real BSON not malformed junk.  Note that null handling is
    # the same as for regular types and the to/from JSON and validation machinery
    # is NOT invoked.  In other words:
    #
    #   mydb=# insert into bsontest (bdata,marker) values (null,'hello!');
    #   INSERT 0 1
    #   mydb=# select bdata,marker from bsontest where bdata is null;
    #    bdata | marker 
    #   -------+--------
    #          | hello!
    #   (1 row)
    #   mydb=# select bdata->'d'->'payload',marker from bsontest where bdata is null;
    #   OR mydb=# select bdata::bytea,marker from bsontest where bdata is null;
    #   OR mydb=# select bdata::json,marker from bsontest where bdata is null;
    #    ?column? | marker 
    #   ----------+--------
    #             | hello!
    #   Following a path from a NULL BSON still produces NULL.

    raw_bson = bson.encode(sdata) 
    
    curs = conn.cursor()
    curs.execute("INSERT INTO bsontest (data) VALUES (%s)",(raw_bson,))
    conn.commit()

    curs.execute("""
    SELECT
      2 * bson_get_decimal128(data, 'data.amt'),
      4 + bson_get_datetime(data, 'data.txDate')::date
    from bsontest
    """)
    rr = curs.fetchall()[0] # one row
    print("amt: ", rr[0], type(rr[0]))  # amt:  215.56 <class 'decimal.Decimal'> fetched as relaxed native Decimal type
    print("txDate: ", rr[1], type(rr[1])) # txDate:  2023-01-04 <class 'datetime.date'>



    -- EJSON is recognized upon insertion. For example, the $date substructure
    --   "ts": {"$date":"2022-03-03T12:13:14.789Z"}
    -- is converted upon parse to a single scalar of type timestamp:
    --   "ts": timestamp
    INSERT INTO data_collection (data) values (
       '{"d":{
           "recordId":"R1",
           "notIndexed":"N1",
           "baz":27,
           "bigint":{"$numberLong":"88888888888888888"},
           "dbl":3.1415,
           "ts": {"$date":"2022-03-03T12:13:14.789Z"},
           "amt": {"$numberDecimal":"77777809838.97"},
           "payload": {
               "fun":"scootering",
               "val":13,
               "vector":[21,17,19],
               "image" : { "$binary" : { "base64" : "VGhpcyBpcyBhIHRlc3Q=", "subType" : "00" } }
            }
         }
    }'
    );


    -- Functional indexes work as well, enabling high performance queries
    -- into any part of the structure; note the dotpath in the example below:
    CREATE INDEX ON data_collection( bson_get_string(data, 'd.recordId'));

    -- ... and those indexes help.  Queries on peer fields in a substructure,
    -- one with an index, one without, yields nearly a 10000x improvement in
    -- performance.
    
    select count(*) from data_collection;
      count  
    ---------
     1000000

    explain analyze select count(*) from btest where bson_get_string(data,'d.recordId') = 'R31';
    ---- QUERY PLAN
     Aggregate  (cost=8.45..8.46 rows=1 width=8) (actual time=0.076..0.077 rows=1 loops=1)
       ->  Index Scan using btest_bson_get_string_idx on btest  (cost=0.42..8.44 rows=1 width=0) (actual time=0.070..0.071 rows=1 loops=1)
             Index Cond: (bson_get_string(data, 'd.recordId'::cstring) = 'R31'::text)
     Planning Time: 0.296 ms
     Execution Time: 0.108 ms


    explain analyze select count(*) from btest where bson_get_string(data,'d.notIndexed') = 'N31';
     Aggregate  (cost=215012.50..215012.51 rows=1 width=8) (actual time=993.471..993.472 rows=1 loops=1)
       ->  Seq Scan on btest  (cost=0.00..215000.00 rows=5000 width=0) (actual time=0.124..993.464 rows=1 loops=1)
             Filter: (bson_get_string(data, 'd.notIndexed'::cstring) = 'N31'::text)
             Rows Removed by Filter: 999999
     Planning Time: 0.079 ms
     Execution Time: 993.505 ms
    


    -- These are equivalent queries but the dotpath accessors will be
    -- significantly faster (and much more memory efficient) if the path depth > 3.
    -- Note of course that the BSON accessor functions can be used in
    -- a selection set (select), a predicate (where), or essentially anywhere else.
    
    select bson_get_bson(data, 'd.payload') from btest where bson_get_string(data,'d.recordId') = 'R1';

    select data->'d'->'payload' from btest where bson_get_string(data,'d.recordId') = 'R1';
                                                                                              ?column?                                                                                              
    ----------------------------------------------------------------------------------------------------------------------------------------------------------------    ------------------------------------
    { "fun" : "scootering", "val" : 13, "vector" : [ 1, 2, 3 ], "fancy" : [ { "A" : 1 }, { "B" : [ "X", "Z" ] } ], "image" : { "$binary" : { "base64" : "VGhpcyBpcyBhIHRlc3Q=", "subType" : "00" } } }
 
    -- The ->> operator, like JSON, yields a text type, which you can cast
    -- whatever you believe is appropriate
    select (data->'d'->>'amt')::numeric as my_amount from btest where bson_get_string(data,'d.recordId') = 'R1';    
       my_amount
    ----------------
     77777809838.97

    

Building
========

Tested on

 * PG 14.4: OS X 10.15.7, OS X 13.2 Ventura, and RHEL 8.6.
 * PG 15.5.3: OS X 13.2 Ventura, RHEL 9.3
 * PG 16.1.3: OS X 13.2 Ventura, RHEL 9.3


Requires:

 *  postgres development SDK (mostly for `.h` files in `.../postgresql/server`).
    On OS X you can use `brew`.  On RH 8 it is a little trickier because many
    repos do not have version 14.x.  Here is a [a good step-by-step install
    of 14.4 for RH 8](https://www.linuxshelltips.com/install-postgresql-rhel)
    Note you will need both server and development (e.g. `postgresql14-devel`)
    packages because you need `.h` files.  It is not necessary to do a complete
    build of postgres.
 *  `pg_config` (which comes with postgres) and is used as part of the Makefile.
    Note that some earlier versions of postgres did not not include the
    `pg_config` exec and the `pgxs` environment.
 *  `libbson.so` and BSON C SDK `.h` files.  You can make or install these
    separately and there is plenty of material on this topic.  There are some
    quick tips at the top of the `Makefile`
 *  C compiler.  No C++ used.  The compiler arguments are driven by the
    environment set up by `pg_config`.



Then:
```
    git clone https://github.com/buzzm/postgresbson.git
    # edit the Makefile to point at the BSON includes and dynamic lib; then:
    make PGUSER=postgres  # compiles pgbson.c to pgbson.so
    make PGUSER=postgres install  # copies .so, .sql, and .control files into target dirs in postgres environment
```

There are too many build directory permissions, install directory permissions,
and other local oddments to document here, but 
neither postgres nor root privilege is required to compile and link the shared
lib but *installation* in your particular environment will vary.  In general,
on OS X using `brew` you won't need root because root does not own `/opt/homebrew`
but on Linux, lots of things are done with `sudo yum` and resources end up
owned as root in `/usr/pgsql-nn` and `/usr/lib64`.

In addition, on RHEL 9, there appears to be an oddment around the compilation
target in make trying to create a `.bc` LLVM file using `clang` in addition to
the regular `.so` shlib using `gcc`.  On many platforms, `clang` may not be
installed and even if it is, the `pg_config` modified `Makefile` may use the
wrong path to it e.g. `/usr/lib64/ccache/clang` instead of `/usr/bin/clang` or
just `clang`.  The good news it appears the `.bc` LLVM output is *not* necessary
for the postgres extension.  As long as the `.so` is installed into the proper
path the extension will work.  At your discretion you can "manually make" the
`.bc` file by editing the command line and then running `make PGUSER=postgres install` again.



Make sure you install *and then restart* your postgres server to properly
pick up the new BSON extension.


Testing
========

```
# Make sure postgresql16-devel (or 14 or 15) is installed *first*. 
# pip3 install psycopg2 uses pg_config; thus the PATH must also be set!
PATH=/path/to/pgsql-16/bin/pg_config:$PATH pip3 install psycopg2  

pip3 install pymongo   # for bson only; we won't be using the mongo driver

python3 pgbson_test.py
```

See excuse at top of file regarding the non-standard test driver approach.


Quick reference
===============

The module defines BSON data type with operator families defined for B-TREE and HASH indexes.

Field access (supports dot notation):

*  bson_get_string(bson_column, dotpath) RETURNS text
*  bson_get_int32(bson_column, dotpath) RETURNS int4
*  bson_get_int64(bson_column, dotpath) RETURNS int8
*  bson_get_double(bson_column, dotpath) RETURNS float8
*  bson_get_decimal(bson_column, dotpath) RETURNS numeric
*  bson_get_datetime(bson_column, dotpath) RETURNS timestamp without time zone
*  bson_get_binary(bson_column, dotpath) RETURNS bytea
*  bson_get_boolean(bson_column, dotpath) RETURNS boolean

*  bson_get_bson(bson_column, dotpath) RETURNS bson

*  bson_get_jsonb_array(bson_column, dotpath) RETURNS jsonb

*  bson_as_text(bson_column, dotpath) RETURNS text

Operators and comparison:

*  Operators: =, <>, <=, <, >=, >, == (binary equality), <<>> (binary inequality)
*  bson_hash(bson) RETURNS INT4



TO DO
========
 1.  **Significantly** tidy up test driver
 2.  Need something better when extracting arrays.
 3.  Need something better when bson_get_bson() resolves to a scalar because
     simple scalars like a string are not BSON.  Currently, it just returns NULL
     which is "technically correct" but unsatisfying
 4.  Need additional safety checks when doing BSON compare and other operations
     because corrupted BSON has a tendency to segfault the PG process.
 5.  Need more docs or a more clever solution for when calling `bson_get_{type}`
     points to a field that exists but the type is wrong.  Currently it just
     returns null because that is "safest."   



See also
========

*  PostgreSQL - http://www.postgresql.org/
*  BSON - http://bsonspec.org/
*  MongoDB - http://mongodb.org



