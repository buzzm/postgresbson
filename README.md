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
 *  Performance.  Moving binary BSON in and out of the database is almost 5x
    faster than using native `jsonb` and over 50x faster than `json`.
 *  Roundtrip ability.  BSON is binary spec, not a string.  There is no whitespace,
    quoting rules, etc.  BSON that goes into Postgres comes out *exactly* the
    same way, each time, every time.
 *  Standard SDK implementations in 13 languages

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
can be used on BSON.

    select json_array_length(bson_column::json->'d'->'payload'->'vector') from table;

These of course can be combined:

    -- Use dotpath to quickly get to event substructure, then cast to jsonb and
    -- use ?@ operator to ask if both `id` and `type` are present as top level tags:
    select (bson_get_bson(bson_column, 'msg.header.event')::jsonb) ?@ array['id','type'] from table;

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
The dotpath method will make postgres vend the 3K structure to the BSON extension, which will only have to examine roughly 64 bytes of data to dig down to the `id` field to return a string which is then examined for equality to `AAA`:
```
select * from table where bson_get_string(bson_column, 'data.payload.product.definition.id') = 'AAA';
```

With arrow operators, *at each arrow*, almost 3K of data has to be processed:
```
   Remember double arrow operator yields text type, which fortunately is easily
   compared to a literal string; no casting necessary, but the journey there is
   tough because each single arrow forces construction of a whole new BSON to
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

Total: about 13K of data in 4 separate vend **and** construct chunks of 3K processed to extract 3 bytes.
```      
Again, this is *exactly* the same situation that occurs with native `json` and `jsonb`
types using the arrow operators; it is *not* particular to BSON.  This is why postgres
provides the `#>` operator and the corresponding `json_extract_path` and `jsonb_extract_path` functions for these native types.




Arrays, BSON, and JSON
----------------------
Unlike JSON, BSON can only be created from a key:value object (which can be
empty); it cannot be created from a simple scalar or in particular an array.
In the course of descending into substructure, it is of course possible to
encounter an array and have that returned.  The C library gets around this
problem by returning a string indexed object where the keys
are the integer representation of the location in the array starting at offset
zero.  From the CLI,
this will be seen as:

    select bson_column->'d'->'payload'->'vector' from ...
      returns  {"0":"some value", "1":"another value", "2":{fldx:"a rich shape"}}
      not      ["some value", "another value", {fldx:"a rich shape"}]
    
The postgres array type does not help here because postgres requires a
homogenous type and BSON allows for heterogenous types.  

There are two ways to deal with this:

  1.  Access items using strings; note the double arrow!
      ```
      select bson_column->'d'->'payload'->'vector'->>'0' from ...
      ```      	     						 

  2.  "Back up one level" in the arrow chain and cast to `jsonb`:
      ```
      mydb=# select (bson_column->'d'->'payload')::jsonb->'vector' from ...
       ?column?   
      --------------
      [21, 17, 19]

      mydb=# select (bson_column->'d'->'payload')::jsonb->'vector'->>0 from ...
       ?column?   
      --------------
      21
      ```


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

Tested using Postgres 14.4 on OS X 10.15.7 and RHEL 8.6.  

Requires:

 *  postgres 14.4 development SDK (mostly for `.h` files in `.../postgresql/server`).
    On OS X you can use `brew`.  On RH 8 it is a little trickier because many
    repos do not have version 14.x.  Here is a [a good step-by-step install
    of 14.4 for RH 8](https://www.linuxshelltips.com/install-postgresql-rhel)
    Note you will need both server and development (e.g. `postgresql14-devel`)
    packages because you need `.h` files.  It is not necessary to do a complete
    build of postgres.
 *  `pg_config` (which comes with postgres) and is used as part of the Makefile.
    Note that some earlier versions of postgres did not not include the
    `pg_config` exec and the `pgxs` environment.
 *  `libbson.so` and BSON C SDK `.h` files.  You can make these separately and
    there is plenty of material on this topic.
 *  C compiler.  No C++ used.  The compiler arguments are driven by the
    environment set up by `pg_config`.
    

Then:
```
    git clone https://github.com/buzzm/postgresbson.git # or unpack downloaded source package
    # edit the Makefile to point at the BSON includes and dynamic lib; then:
    make PGUSER=postgres  # compiles pgbson.c to pgbson.so
    make PGUSER=postgres install  # copies .so, .sql, and .control files into target dirs in postgres environment
```

There are too many build directory permissions, install directory permissions,
and other local oddments to document here, but 
neither postgres nor root privilege is required to compile and link the shared
lib but *installation* in your particular environment will vary.


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

*  bson_get_bson(bson_column, dotpath) RETURNS bson

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



