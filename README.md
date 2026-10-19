postgresbson
============

BSON support for PostgreSQL

Introduction
============

This PostgreSQL extension realizes the BSON data type, together with functions to create and inspect BSON objects for the purposes of expressive and performant
querying.


BSON (http://bsonspec.org/) is a high-performance, richly-typed data carrier
similar to JSON but offers a number of attractive features including:

 *  Datetime is a first class type.  In pure
    JSON these must all be represented as a string (typically ISO 8601),
    requiring conversion, potentially introducing lossiness, and impairing
    native operations like `>` and `<=`.
 *  Numeric values are unambiguously carried in 4 first class types:
    int32, int64, float, and decimal.
    Unlike JSON there is no risk of inadvertently emitting a float of
    1.0 as 1 and having it be parsed by a consumer as an integer.
    A `decimal` value of 10.09 is preserved *exactly* as 10.09 with no risk of
    floating point precision error e.g. `10.08999999999`
 *  `byte[]` is a first class type.  This relieves the program of dealing
    with base64 or other encodings of binary data to stringify it for JSON.
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
    select (bson_get_bson(bson_column, 'msg.header.event')::jsonb) ?& array['id','type'] from table;


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




Scalars, Arrays, BSON, and JSON
------------------------------
Unlike JSON, BSON can only be created from a key:value object (which can be
empty); it cannot be created from a simple scalar or in particular an array.

This single constraint produces two separate annoyances, and `bson_get_value`
(approach #4 below) is the general answer to both.  The short version:

    -- the value at the dotpath, whatever its type, wrapped under "_":
    select bson_get_value(bson_column, 'd.payload.vector') from ...
      returns  {"_": ["some value", "another value", {"fldx":"a rich shape"}]}

    select bson_get_value(bson_column, 'd.recordId') from ...
      returns  {"_": "R1"}

Peel exactly one layer and you have the true value.  The rest of this section
explains why the wrapper is there and what the alternatives are.

**The array annoyance.**
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

**The scalar annoyance.**
The same constraint means a dotpath that lands on a *scalar* has nothing that
`bson_get_bson()` can legally return, because a bare string or int is not a
BSON document.  It returns NULL:

    select bson_column->'d.recordId' from ...    -- NULL, not "R1"

That is technically correct and, if you know the type you are after, entirely
avoidable -- use the typed accessor (`bson_get_string`, `bson_get_int32`, and
friends) or `->>`.  It only bites when you do *not* know the type in advance.

There are four ways to deal with all this:

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

  4.  Use the `bson_get_value` function.  This is the general answer, and the
      only one that covers scalars, arrays, and documents with a single rule:
      the value found at the dotpath is returned wrapped in a one-field
      document under the key `_`.
      ```
      mydb=# select bson_get_value(bson_column,'d.payload.vector') from ...
       ?column?
      --------------------------
      { "_" : [ 21, 17, 19 ] }

      mydb=# select bson_get_value(bson_column,'d.recordId') from ...
       ?column?
      --------------
      { "_" : "R1" }

      mydb=# select bson_get_value(bson_column,'d.payload') from ...
       ?column?
      ------------------------------------
      { "_" : { "vector" : [ 21, 17, 19 ] } }
      ```
      The wrap is **unconditional** -- documents get wrapped too.  That is the
      point: you always peel exactly one layer, so whatever emerges is the
      true value.  Wrapping only scalars would make `{"_": 5}` ambiguous, since
      it could mean the scalar `5` or a document that genuinely *is* `{"_": 5}`.

      Because the array is a *value* inside the wrapper rather than the top
      level document, it stays a real BSON array -- no string indexed
      `{"0":..,"1":..}` form, and no `jsonb` detour.  Client drivers decode it
      as a native list:
      ```
      doc = bson.decode(row[0])   # psycopg2 + pymongo's bson
      doc['_']                    # -> [21, 17, 19], a python list
      ```
      Scalars keep full type fidelity as well, which is what `->>` cannot do:
      a `decimal128` comes back as a `decimal128` and not as its rendering.

      The key is `_` and not `$value` deliberately.  Text I/O for the `bson`
      type round trips through extended JSON, where a leading `$` on a top
      level key risks being read back as an EJSON type wrapper.

      Note there is deliberately **no `->` style operator** for this.  Arrow
      operators must compose with themselves, and the wrapper breaks that --
      every hop in a chain would need an interposed `->'_'`.  `bson_get_value`
      is terminal by design: call it once, at the end of navigation, and
      unwrap.  A missing path returns NULL, like every other accessor.

Status
======

Working and mature.  All contribs / PRs / comments / issues welcome.


Release notes
=============

2.2.0
-----

**The extension version is now 2.2.**  A fresh `CREATE EXTENSION pgbson` picks
it up automatically.  If you already have 2.1 installed, `make install` and
then:

    ALTER EXTENSION pgbson UPDATE TO '2.2';

Install the new shared library *first*: the 2.2 SQL refers to C functions that
earlier builds do not have.  `CREATE EXTENSION pgbson VERSION '2.1'` (or
`'2.0'`) still works if you want the older function set.

**Security: the comparison operators no longer depend on `search_path`.**  `=`,
`<>`, `<`, `<=`, `>`, `>=` and `<<>>` were bound to `LANGUAGE SQL` wrappers that
called `pgbson_compare()` / `bson_binary_equal()` by bare name.  An SQL function
body is resolved through the *caller's* `search_path` each time it runs, so a
function of that name created in a schema earlier on the search path was run
instead -- changing the answer, or running with the caller's privileges if the
caller was a superuser.  They are now C functions with nothing to resolve.  As a
side effect they are `IMMUTABLE STRICT PARALLEL SAFE` like everything else here;
as SQL functions they defaulted to `VOLATILE`.  Operators, operator classes and
indexes are unchanged and need no rebuild.


2.1.2
-----

Bug fixes only; the extension version stays 2.1, so there is no `ALTER
EXTENSION` to run.  `make install` and reconnect.  Thanks to Devrim Gündüz
for all four fixes.

**Loads on current libbson.**  Dates were rendered as text by a private
libbson function that newer releases (e.g. 1.30) no longer export, so the
shared library failed to load with `undefined symbol:
_bson_iso8601_date_format`.  pgbson now formats dates itself.  Output is
unchanged: ISO8601 UTC, with `.mmm` only when the milliseconds are non-zero.

**`bson_get_jsonb_array` no longer crashes the backend** on an array it
cannot render as JSON (for example, one holding invalid UTF-8).  It raises an
ERROR instead.

**Invalid UTF-8 is rejected on the way in.**  BSON strings and keys must be
valid UTF-8 for `bytea::bson` casts and binary `COPY`; such a row used to break
`SELECT`, `pg_dump` and the json casts.  Embedded NULs are still allowed (see
2.1.1).  Rows already stored are not rechecked.  Note: older libbson (1.23
and earlier) does not check UTF-8 in string values even when asked, so there
only keys are checked.

**`bson_get_string` and `->>` return text in the database's encoding.**  BSON
strings are UTF-8 and used to be copied as is, which is wrong in a non-UTF-8
database.  They are now converted; a character with no equivalent in the
database's encoding raises an ERROR.  In a UTF-8 database nothing changes,
except that invalid UTF-8 already stored raises an ERROR rather than producing
invalid text.


2.1.1
-----

Bug fixes only; the extension version stays 2.1, so there is no `ALTER
EXTENSION` to run.  `make install` and reconnect.

**Binary format input works.**  The type's binary receive function read the
wrong memory and could crash the backend, so `COPY ... FROM (FORMAT binary)`
and drivers that send parameters in binary were unusable.  Incoming bytes are
now validated exactly like a `bytea::bson` cast.  Text format input (what
psycopg2 uses) was never affected.

**`bson_get_datetime` returns NULL for dates postgres cannot represent.**  A
BSON datetime spans roughly 292 million years either side of 1970; a postgres
`timestamp` spans 4713 BC to 294276 AD.  Out of range values used to yield a
garbage timestamp and now yield NULL.  Dates before 1970 were and still are
converted correctly.

**Strings with an embedded NUL are truncated at the NUL.**  BSON permits a NUL
byte inside a string; postgres `text` does not.  `bson_get_string` and `->>`
therefore return everything up to the first NUL.  This is not new, just now
documented.


2.1.0
-----

**The extension version is now 2.1.**  A fresh `CREATE EXTENSION pgbson` picks
it up automatically.  If you already have 2.0 installed, `make install` gives
you the new shared library but *not* the new SQL, because `CREATE EXTENSION`
runs its install script only at install time.  One statement finishes the job:

    ALTER EXTENSION pgbson UPDATE TO '2.1';

Nothing is removed and nothing changes behavior, so the upgrade is safe to run
on a live database.  `CREATE EXTENSION pgbson VERSION '2.0'` still works if you
want the older function set.

Note the shared library is not versioned alongside the SQL: `$libdir/pgbson` is
whatever the last `make install` put there and it serves every extension
version.  So an install still marked 2.0 already has the segfault fix below --
only the SQL surface is gated by the version you are on.

**New: `bson_get_value(bson_column, dotpath)`.**  The general purpose
extractor, and the answer to two long standing TO DO items.  Works for every
BSON type, scalar or not; returns the value wrapped in a one-field document
under the key `_`:

    select bson_get_value(bson_column, 'd.recordId');       -->  {"_": "R1"}
    select bson_get_value(bson_column, 'd.vector');         -->  {"_": [21,17,19]}
    select bson_get_value(bson_column, 'd.payload');        -->  {"_": {...}}

Use it when you do not know the type at the dotpath in advance, or when you
want an array back as a real array instead of the string indexed
`{"0":..,"1":..}` form.  Nothing existing changes behavior.  See "Scalars,
Arrays, BSON, and JSON".

**Fixed: backend segfault in `bson_get_bson()` / the `->` operator when a
dotpath resolved to a scalar.**  BSON cannot be built from a bare scalar, so
there was nothing to return; the code detected "path found" but not "path found
AND it is a document or array", and handed back an uninitialized structure.  A
segfaulting backend takes the whole postmaster down with it, so any user with
`SELECT` could bounce the server with an ordinary query:

    -- any of these, where the dotpath ends on a string, int, bool,
    -- datetime, decimal128, double or binary:
    select bson_column->'d.recordId' from ...
    select bson_get_bson(bson_column, 'd.recordId') from ...
    select bson_get_jsonb_array(bson_column, 'd.recordId') from ...

These now return NULL, which is what the TO DO section below has always
claimed they did.  Read-path only; no stored data was ever at risk.

Four fixes to `bson_as_text()`, i.e. the `->>` operator.

**None of these touch the `bson_get_{type}()` dotpath accessors.**  In
particular `bson_get_datetime()`, `bson_get_double()`, and `bson_get_boolean()`
go through an entirely separate code path and have always been correct.  If you
use the dotpath accessors exclusively, nothing below affects you.

**BREAKING: `->>` on a datetime now emits the trailing `Z`.**

    -- before:  2022-06-06T12:13:14.500
    -- after:   2022-06-06T12:13:14.500Z

> ### `bson_get_datetime()` was NOT affected.  It always worked.
>
> This bug lived only in the `->>` operator.  `bson_get_datetime()` hands back
> a real `timestamp` through a completely separate code path and has been
> correct the entire time -- no truncation, no time zone shift, nothing to
> recompute.  If you took the README's advice and used the dotpath accessors
> for dates, you are not affected by any of this and can stop reading here.
>
> This is, incidentally, a decent argument for the dotpath accessors over the
> arrow operators generally: they are faster, they preserve type, and they
> hand you a `timestamp` instead of a string you have to cast.

The `Z` was being chopped because the copy into the output buffer was handed
the length of the source string rather than the size of the destination.
Check your code before upgrading, because there are two ways this bites:

 1.  If you appended the `Z` yourself to work around this, you will now get
     `...500ZZ`.  Remove the workaround.
 2.  Worse and quieter: if you cast the result to `timestamptz`, postgres was
     reading a `Z`-less string as *local* time.  With the `Z` present it is
     now correctly read as UTC, so previously-computed values shift by your
     server's UTC offset.  Any stored results of datetime math on `->>` dates
     are suspect and should be recomputed.

Casting to plain `timestamp` (no time zone) is unaffected either way.

**`->>` on a double no longer overflows, or rounds.**  The double was rendered
with `sprintf("%lf")` into a 64 byte stack buffer.  `%lf` is *fixed* notation,
so the length tracks the exponent: a value like `1e308` rendered to 316
characters and smashed the stack, taking the backend down with it.  It also
silently rounded everything to 6 decimal places.  Doubles now render exactly
as postgres renders any `float8`, so `3.1415926` comes back as `3.1415926`
rather than `3.141593`, and `2.0` as `2` rather than `2.000000`.

**`->>` on a boolean returns `true`/`false` instead of NULL.**  Spelled the
same way `jsonb`'s `->>` spells it, not postgres' own `t`/`f`.  Both cast back
to `boolean` cleanly.

**`->>` on an ObjectId returns the bare 24 character hex instead of NULL**,
with no `{"$oid": ...}` wrapper, consistent with how datetime and decimal128
also drop their EJSON `$`-wrappers here.  There is deliberately no
`bson_get_objectid()`; there is no high fidelity postgres type for it to land
in.  Cast the column to `jsonb` if you want the wrapped form.

Following a path that does not exist still yields NULL, as does a path
resolving to an explicit BSON null.  That is unchanged.


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
    #
    # See pgbson--2.0.sql about byte[] validation with a write operation that
    # results in a cast from bytea to the bson type to ensure that the bytea
    # being stored is real BSON not malformed junk.  Once validated upon write,
    # the bson bytes are assumed to be valid upon read.
    #
    # Note that null handling is
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

Tested on

 * PG 18.2:
   * OS X 26.3 Tahoe; Apple clang version 17.0.0 (clang-1700.6.3.2)
   
 * PG 16.3:
   * OS X 14.3.1 Sonoma; Apple clang version 15.0.0 (clang-1500.1.0.2.5)
   * RHEL 9.3
   
 * PG 16.1.3:
   * OS X 14.3.1 Sonoma; Apple clang version 15.0.0 (clang-1500.1.0.2.5)
   * OS X 13.2 Ventura
   * RHEL 9.3
   
 * PG 15.5.3: OS X 13.2 Ventura, RHEL 9.3
 * PG 14.4: OS X 10.15.7, OS X 13.2 Ventura, and RHEL 8.6.



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

*  bson_get_value(bson_column, dotpath) RETURNS bson

   The general purpose extractor: works for every BSON type, scalar or not.
   The value at the dotpath is returned wrapped in a one-field document under
   the key `_` -- unconditionally, so you always peel exactly one layer.  This
   is the way to retrieve a value whose type you do not know in advance, and
   the way to get an array back as a real array.  Terminal by design; there is
   no `->` equivalent.  See "Scalars, Arrays, BSON, and JSON" above.

*  bson_get_jsonb_array(bson_column, dotpath) RETURNS jsonb

*  bson_as_text(bson_column, dotpath) RETURNS text

Operators and comparison:

*  Operators: =, <>, <=, <, >=, >, == (binary equality), <<>> (binary inequality)
*  bson_hash(bson) RETURNS INT4



TO DO
========
 1.  **Significantly** tidy up test driver
 
 2.  Need more docs or a more clever solution for when calling `bson_get_{type}`
     points to a field that exists but the type is wrong.  Currently it just
     returns null because that is "safest."

     `bson_get_value()` takes some of the sting out of this: a type mismatch is
     no longer a dead end, because you can always retrieve the value regardless
     of its type and inspect it.  But the typed accessors themselves still give
     you no way to tell "field absent" from "field present, wrong type."



See also
========

*  PostgreSQL - http://www.postgresql.org/
*  BSON - http://bsonspec.org/
*  MongoDB - http://mongodb.org



