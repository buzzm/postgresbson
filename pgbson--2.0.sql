-- Copyright (c) 2022  Buzz Moschetti <buzz.moschetti@gmail.com>
-- 
-- Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted,
-- provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.
-- 
-- IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
-- ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-- 
-- THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
-- THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.


-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgbson" to load this file. \quit

------------------------------------
-- type definition and i/o functions
------------------------------------

-- Do this first to establish the type name "bson".  We will need to declare the
-- "main 4" function in, out, recv, and send.
CREATE TYPE bson;

-- from-/to- string.  The string is EJSON.
CREATE FUNCTION bson_in(cstring) RETURNS bson AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
CREATE FUNCTION bson_out(bson) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- binary i/o
-- note recv takes 'internal' type....
CREATE FUNCTION bson_recv(internal) RETURNS bson AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_send(bson) RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now that we have the "main 4" functions defined, we can fill out our new
-- type with the Main 4 functions:
CREATE TYPE bson (
    input = bson_in,
    output = bson_out,
    send = bson_send,
    receive = bson_recv,
    alignment = int4,
    storage = extended  -- Big BSONs will need TOAST!
);


-- To prevent storing junk or accidentally malformed BSON into the DB,
-- we call pgbson_validate during the cast.  Fortunately, testing has shown
-- the performance hit to be negligible.
CREATE FUNCTION pgbson_validate(bytea) RETURNS bson AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
CREATE CAST (bytea AS bson) WITH FUNCTION pgbson_validate(bytea) AS IMPLICIT;

-- Going the other way, however, is easy; the BSON just comes out as bytea:
CREATE CAST (bson AS bytea) WITHOUT FUNCTION AS IMPLICIT;


-- But here's a great trick.  To turn bson into json, just use bson_out!
-- It emits data in EJSON format!  Which means...
-- ALL functions and expressions in Postgres JSON are now available to you.
CREATE CAST (bson AS json) WITH INOUT;
CREATE CAST (bson AS jsonb) WITH INOUT;


------------
-- operators
------------

-- logical comparison
-- Must name it other than bson_compare() because that symbol already
-- exists in libbson.1!
CREATE FUNCTION pgbson_compare(bson, bson) RETURNS INT4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

--  These are "conveniences" in SQL imp in terms of real C pgbson_compare():
CREATE FUNCTION bson_equal(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) = 0;
$$ LANGUAGE SQL;

CREATE FUNCTION bson_not_equal(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) <> 0;
$$ LANGUAGE SQL;

CREATE FUNCTION bson_lt(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) < 0;
$$ LANGUAGE SQL;

CREATE FUNCTION bson_lte(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) <= 0;
$$ LANGUAGE SQL;

CREATE FUNCTION bson_gt(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) > 0;
$$ LANGUAGE SQL;

CREATE FUNCTION bson_gte(bson, bson) RETURNS BOOL AS $$
    SELECT pgbson_compare($1, $2) >= 0;
$$ LANGUAGE SQL;

CREATE OPERATOR = (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_equal,
    NEGATOR = <>
);

CREATE OPERATOR <> (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_not_equal,
    NEGATOR = =
);

CREATE OPERATOR < (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_lt,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_lte,
    NEGATOR = >
);

CREATE OPERATOR > (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_gt,
    NEGATOR = <=
);

CREATE OPERATOR >= (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_gte,
    NEGATOR = <
);



-- binary equality
CREATE FUNCTION bson_binary_equal(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_binary_not_equal(bson, bson) RETURNS BOOL AS $$
    SELECT NOT(bson_binary_equal($1, $2));
$$ LANGUAGE SQL;


CREATE OPERATOR == (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_binary_equal,
    NEGATOR = <<>>
);

CREATE OPERATOR <<>> (
    LEFTARG = bson,
    RIGHTARG = bson,
    PROCEDURE = bson_binary_not_equal,
    NEGATOR = ==
);

---------------------
-- hash index support
---------------------

CREATE FUNCTION bson_hash(bson) RETURNS INT4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR CLASS bson_hash_ops
    DEFAULT FOR TYPE bson USING hash AS
        OPERATOR 1 == (bson, bson) ,
        FUNCTION 1 bson_hash(bson);

-----------------------
-- b-tree index support
-----------------------

CREATE OPERATOR CLASS bson_btree_ops
    DEFAULT FOR TYPE bson USING btree AS
        OPERATOR 1 < (bson, bson),
        OPERATOR 2 <= (bson, bson),
        OPERATOR 3 = (bson, bson),
        OPERATOR 4 >= (bson, bson),
        OPERATOR 5 > (bson, bson),
        FUNCTION 1 pgbson_compare(bson, bson);


-- libbson already has bson_version....
CREATE FUNCTION pgbson_version() RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

------------------------------
-- All the _get_ functions can take a dotpath, e.g.
-- bson_get_string(bson_column, 'user.detail.address.city')
--
-- IMPORTANT NOTE:
-- The default text representation of BSON is *relaxed* EJSON.  Canonical EJSON
-- explicitly identifies all types EXCEPT string using the "dollar-typename"
-- convention, e.g.
--   {"fld1": {"$numberInt": "123"},
--    "fld2": {"$date": 1646309594456},
--    "fld3": {"$numberDecimal": "23498734.34"},
--    "fld4": {"$numberDouble": "3.14159"}
--   }
-- Note how numeric values are represented as strings to prevent the JSON parser
-- from trying to do numeric interpretation.  Also note the date format is a long
-- integer, millis since epoch.  Although this preserves type and precision, it
-- is irritating to work with directly.  Relaxed EJSON changes the format
-- as follows:
-- 1.  Int32, int64, and double values are emitted directly
-- 2.  Date is emitted in ISO8601 format
--   {"fld1": 123},
--    "fld2": {"$date": "2022-03-03T12:13:14.789Z"},
--    "fld3": {"$numberDecimal": "23498734.34"},
--    "fld4": 3.14159}
--   }
--
-- Although the dollar-typename format continues to appear in textual output
-- (most notably in  "select bson_column from table"), it is *NOT* part of the
-- actual path to data.  Example:
--    Correct way: No dollar-typename, returns a postgres numeric type:
--    select bson_get_decimal128(bson_column,'path.to.fld3') from table
--
--    INCORRECT way:
--    select bson_get_decimal128(bson_column,'path.to.fld3.$numberDecimal') from table
--
------------------------------

CREATE FUNCTION bson_get_string(bson, text) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_datetime(bson, text) RETURNS timestamp without time zone
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_decimal128(bson, text) RETURNS numeric
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_int32(bson, text) RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_int64(bson, text) RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_double(bson, text) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_binary(bson, text) RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bson_get_boolean(bson, text) RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;


-- A great workhorse function especially for rapidly descending into a complex
-- BSON structure to yield a subdoc, which can then be cast to JSON for
-- all kinds of processing in Postgres.  For example, a doc with big subdocs
-- a and b does not need to be fully parsed into JSON only to get at c.vector:
--
--   select json_array_length(bson_get_bson(data, 'a.b.c')::json->'vector') from btest;
--
CREATE FUNCTION bson_get_bson(bson, text) RETURNS bson
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;



-- Forces to-text; used in ->> operator
CREATE FUNCTION bson_as_text(bson, text) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR -> (
    LEFTARG = bson,
    RIGHTARG = text,
    FUNCTION = bson_get_bson
);

CREATE OPERATOR ->> (
    LEFTARG = bson,
    RIGHTARG = text,
    FUNCTION = bson_as_text
);

