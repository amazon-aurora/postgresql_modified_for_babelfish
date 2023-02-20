/* comtrib/table_variable_am/table_variable_am--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION table_variable_am" to load this file. \quit

CREATE FUNCTION tv_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD table_variable_am TYPE TABLE HANDLER tv_tableam_handler;
COMMENT ON ACCESS METHOD table_variable_am IS 'table variable access method';

