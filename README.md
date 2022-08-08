postgresbson
============

BSON support for PostgreSQL

Introduction
============

This PostgreSQL extension brings BSON data type, together with functions to create, inspect and manipulate BSON objects.

BSON (http://bsonspec.org/) is a high-performance, richly-typed data carrier
similar to JSON but offers these advantages:

 *  Datetimes, decimal (numeric), and byte[] are first class types.  In pure
    JSON these must all be represented as a string, requiring conversion and
    impairing native operations like `>` and `<=`.
 *  Performance.  Moving binary BSON in and out of the database is almost 5x
    faster than using native `jsonb` and over 50x faster than `json`.
 *  Roundtrip ability.  BSON is binary spec, not a string.  There is no whitespace,
    quoting rules, etc.  BSON that goes into Postgres comes out *exactly* the
    same way, each time, every time.
 *  Standard SDK implementations in 13 languages

Roundtripping and many available language SDKs enables seamless creation,
manipulation, transmission, and querying of data in a distributed system without
coordinating IDLs, compile-time dependencies, nuances in platform type representation, etc.   Here is an example:

  1.  Java program constructs an `org.bson.Document` which honors `java.util.Map` interface.
  2.  Java program encodes `org.bson.Document` using Java SDK to a BSON `byte[]`.
  3.  Java program publishes `byte[]` to a Kafka topic.
  4.  python listener wakes up on topic and receives `byte[]`.
  5.  python listener decodes `byte[]` using python BSON SDK into `dict` -- not a string, a fully reconstituted object with substructures, `datetime.datetime` for date fieldss, etc. --  and prints some things -- but does not change anything in the dict.
  6.  python listener encodes `dict` to `byte[]` and saves it to a BSON
column in Postgres
  7.  A different Java program wakes up on an insert trigger and `SELECT`s
  the BSON column as a `byte[]` (e.g. `select bson_column::bytea where ...`)
  8.  This `byte[]` is *identical* to the one created in step 2.
  9.  The different Java program decodes the `byte[]` into an `org.bson.Document`
  10.  The different Java program encodes the `org.bson.Document` into a second `byte[]`
  11. The second `byte[]` is *identical* to the `byte[]` in step 8.
 

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
    
The BSON type is castable to JSON in so-called EJSON format.  Thus, the wealth
of functions and operations and even other extensions built around the JSON type
can be used on BSON.

Status
======

Not sure.


Example
=======

    CREATE EXTENSION pgbson;
    CREATE TABLE data_collection ( data BSON );

    -- EJSON is recognized upon insertion. For example, the $date substructure
    --   "ts": {"$date":"2022-03-03T12:13:14.789Z"}
    -- is converted upon parse to a single scalar of type timestamp:
    --   "ts": timestamp
    INSERT INTO data_collection (amt) values (
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

    -- Programmatic insert of material through postgres SDK e.g. psycopg2
    -- can use native types directly; there is no need to use EJSON.


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
    -- significantly faster (and more memory efficient) if the path depth > 3
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

Tested on OS X, Postgres 14.4. 
Requires: pg_config (which comes with postgres) and a C (not C++) compiler.

    git clone https://github.com/buzzm/postgresbson.git # or unpack downloaded source package
    make PGUSER=postgres
    make PGUSER=postgres install


Quick reference
===============

The module defines BSON data type with operator families defined for B-TREE and HASH indexes.

Operators and comparison:

*  Operators: =, <>, <=, <, >=, >, == (binary equality), <<>> (binary inequality)
*  bson_hash(bson) RETURNS INT4

Field access (supports dot notation):

*  bson_get_string(bson, cstring) RETURNS text
*  bson_get_int32(bson, cstring) RETURNS int4
*  bson_get_int64(bson, cstring) RETURNS int8
*  bson_get_double(bson, text) RETURNS float8
*  bson_get_decimal(bson, text) RETURNS numeric
*  bson_get_datetime(bson, text) RETURNS timestamp without time zone

*  bson_get_bson(bson, text) RETURNS bson

*  bson_as_text(bson, text) RETURNS text

Array field support:

*  bson_array_size(bson, text) RETURNS int8
*  bson_unwind_array(bson, text) RETURNS SETOF bson

See also
========

*  PostgreSQL - http://www.postgresql.org/
*  BSON - http://bsonspec.org/
*  MongoDB - http://mongodb.org



