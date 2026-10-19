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


-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pgbson UPDATE TO '2.2'" to load this file. \quit

-- 2.1 -> 2.2
--
-- The comparison operator functions were LANGUAGE SQL wrappers that called
-- pgbson_compare() / bson_binary_equal() by bare name.  An SQL function body is
-- resolved through the CALLER's search_path when it runs, so a function of that
-- name created in a schema earlier on the caller's search_path was run instead,
-- with the caller's privileges, on every =, <>, <, <=, >, >= and <<>>.  The
-- extension is relocatable, so the body cannot be schema qualified either.
--
-- Rebind them to C functions, which have no body to resolve.  That also makes
-- them IMMUTABLE STRICT PARALLEL SAFE like every other function here (SQL
-- functions default to VOLATILE, so the planner could not treat bson
-- comparisons as immutable).  The operators and operator classes reference the
-- functions by OID and are untouched.
--
-- Needs the 2.2 shared library: the C symbols below do not exist in earlier
-- builds.  `make install` puts it in place before ALTER EXTENSION UPDATE.

CREATE OR REPLACE FUNCTION bson_equal(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_eq'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_not_equal(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_ne'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_lt(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_lt'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_lte(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_le'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_gt(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_gt'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_gte(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_ge'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bson_binary_not_equal(bson, bson) RETURNS BOOL
AS 'MODULE_PATHNAME', 'pgbson_binary_ne'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
