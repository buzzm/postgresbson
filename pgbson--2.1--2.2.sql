-- Copyright (c) 2022-2026 Buzz Moschetti <buzz.moschetti@gmail.com>
-- SPDX-License-Identifier: MIT  (see LICENSE)


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
