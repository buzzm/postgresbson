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
\echo Use "ALTER EXTENSION pgbson UPDATE TO '2.1'" to load this file. \quit

-- 2.0 -> 2.1
--
-- Adds one function.  Nothing existing changes, so there is no matching
-- 2.1 -> 2.0 downgrade script; dropping the function by hand is the whole
-- of it if you ever need to go back.
--
-- Note the shared library is NOT versioned alongside this script: $libdir/pgbson
-- is whatever the last `make install` put there, and it serves every extension
-- version.  So an install still marked 2.0 is already running the current C
-- code -- including the fix for the segfault when a dotpath resolved to a
-- scalar.  Only the SQL surface is gated by the version you are on.

-- The general purpose extractor: works for EVERY BSON type, scalar or not.
-- BSON cannot be built from a bare scalar or array, so the value at the
-- dotpath comes back wrapped in a one-field document under the key "_".
-- The wrap is unconditional: always peel exactly one layer.
--   select bson_get_value(bson_column, 'd.recordId');  -->  {"_": "R1"}
--   select bson_get_value(bson_column, 'd.vector');    -->  {"_": [2,3,5]}
-- Deliberately has no -> operator equivalent; it is terminal, not chainable.
-- See README.md "Scalars, Arrays, BSON, and JSON".
CREATE FUNCTION bson_get_value(bson, text) RETURNS bson
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
