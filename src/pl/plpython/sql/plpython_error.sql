-- test error handling, i forgot to restore Warn_restart in
-- the trigger handler once. the errors and subsequent core dump were
-- interesting.

/* Flat out Python syntax error
 */
CREATE FUNCTION python_syntax_error() RETURNS text
        AS
'.syntaxerror'
        LANGUAGE plpython3u;

/* With check_function_bodies = false the function should get defined
 * and the error reported when called
 */
SET check_function_bodies = false;

CREATE FUNCTION python_syntax_error() RETURNS text
        AS
'.syntaxerror'
        LANGUAGE plpython3u;

SELECT python_syntax_error();
/* Run the function twice to check if the hashtable entry gets cleaned up */
SELECT python_syntax_error();

RESET check_function_bodies;

/* Flat out syntax error
 */
CREATE FUNCTION sql_syntax_error() RETURNS text
        AS
'plpy.execute("syntax error")'
        LANGUAGE plpython3u;

SELECT sql_syntax_error();


/* check the handling of uncaught python exceptions
 */
CREATE FUNCTION exception_index_invalid(text) RETURNS text
	AS
'return args[1]'
	LANGUAGE plpython3u;

SELECT exception_index_invalid('test');


/* check handling of nested exceptions
 */
CREATE FUNCTION exception_index_invalid_nested() RETURNS text
	AS
'rv = plpy.execute("SELECT test5(''foo'')")
return rv[0]'
	LANGUAGE plpython3u;

SELECT exception_index_invalid_nested();


/* a typo
 */
CREATE FUNCTION invalid_type_uncaught(a text) RETURNS text
	AS
'if "plan" not in SD:
	q = "SELECT fname FROM users WHERE lname = $1"
	SD["plan"] = plpy.prepare(q, [ "test" ])
rv = plpy.execute(SD["plan"], [ a ])
if len(rv):
	return rv[0]["fname"]
return None
'
	LANGUAGE plpython3u;

SELECT invalid_type_uncaught('rick');


/* for what it's worth catch the exception generated by
 * the typo, and return None
 */
CREATE FUNCTION invalid_type_caught(a text) RETURNS text
	AS
'if "plan" not in SD:
	q = "SELECT fname FROM users WHERE lname = $1"
	try:
		SD["plan"] = plpy.prepare(q, [ "test" ])
	except plpy.SPIError as ex:
		plpy.notice(str(ex))
		return None
rv = plpy.execute(SD["plan"], [ a ])
if len(rv):
	return rv[0]["fname"]
return None
'
	LANGUAGE plpython3u;

SELECT invalid_type_caught('rick');


/* for what it's worth catch the exception generated by
 * the typo, and reraise it as a plain error
 */
CREATE FUNCTION invalid_type_reraised(a text) RETURNS text
	AS
'if "plan" not in SD:
	q = "SELECT fname FROM users WHERE lname = $1"
	try:
		SD["plan"] = plpy.prepare(q, [ "test" ])
	except plpy.SPIError as ex:
		plpy.error(str(ex))
rv = plpy.execute(SD["plan"], [ a ])
if len(rv):
	return rv[0]["fname"]
return None
'
	LANGUAGE plpython3u;

SELECT invalid_type_reraised('rick');


/* no typo no messing about
 */
CREATE FUNCTION valid_type(a text) RETURNS text
	AS
'if "plan" not in SD:
	SD["plan"] = plpy.prepare("SELECT fname FROM users WHERE lname = $1", [ "text" ])
rv = plpy.execute(SD["plan"], [ a ])
if len(rv):
	return rv[0]["fname"]
return None
'
	LANGUAGE plpython3u;

SELECT valid_type('rick');

/* error in nested functions to get a traceback
*/
CREATE FUNCTION nested_error() RETURNS text
	AS
'def fun1():
	plpy.error("boom")

def fun2():
	fun1()

def fun3():
	fun2()

fun3()
return "not reached"
'
	LANGUAGE plpython3u;

SELECT nested_error();

/* raising plpy.Error is just like calling plpy.error
*/
CREATE FUNCTION nested_error_raise() RETURNS text
	AS
'def fun1():
	raise plpy.Error("boom")

def fun2():
	fun1()

def fun3():
	fun2()

fun3()
return "not reached"
'
	LANGUAGE plpython3u;

SELECT nested_error_raise();

/* using plpy.warning should not produce a traceback
*/
CREATE FUNCTION nested_warning() RETURNS text
	AS
'def fun1():
	plpy.warning("boom")

def fun2():
	fun1()

def fun3():
	fun2()

fun3()
return "you''ve been warned"
'
	LANGUAGE plpython3u;

SELECT nested_warning();

/* AttributeError at toplevel used to give segfaults with the traceback
*/
CREATE FUNCTION toplevel_attribute_error() RETURNS void AS
$$
plpy.nonexistent
$$ LANGUAGE plpython3u;

SELECT toplevel_attribute_error();

/* Calling PL/Python functions from SQL and vice versa should not lose context.
 */
CREATE OR REPLACE FUNCTION python_traceback() RETURNS void AS $$
def first():
  second()

def second():
  third()

def third():
  plpy.execute("select sql_error()")

first()
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION sql_error() RETURNS void AS $$
begin
  select 1/0;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION python_from_sql_error() RETURNS void AS $$
begin
  select python_traceback();
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sql_from_python_error() RETURNS void AS $$
plpy.execute("select sql_error()")
$$ LANGUAGE plpython3u;

SELECT python_traceback();
SELECT sql_error();
SELECT python_from_sql_error();
SELECT sql_from_python_error();

/* check catching specific types of exceptions
 */
CREATE TABLE specific (
    i integer PRIMARY KEY
);

CREATE FUNCTION specific_exception(i integer) RETURNS void AS
$$
from plpy import spiexceptions
try:
    plpy.execute("insert into specific values (%s)" % (i or "NULL"));
except spiexceptions.NotNullViolation as e:
    plpy.notice("Violated the NOT NULL constraint, sqlstate %s" % e.sqlstate)
except spiexceptions.UniqueViolation as e:
    plpy.notice("Violated the UNIQUE constraint, sqlstate %s" % e.sqlstate)
$$ LANGUAGE plpython3u;

SELECT specific_exception(2);
SELECT specific_exception(NULL);
SELECT specific_exception(2);

/* SPI errors in PL/Python functions should preserve the SQLSTATE value
 */
CREATE FUNCTION python_unique_violation() RETURNS void AS $$
plpy.execute("insert into specific values (1)")
plpy.execute("insert into specific values (1)")
$$ LANGUAGE plpython3u;

CREATE FUNCTION catch_python_unique_violation() RETURNS text AS $$
begin
    begin
        perform python_unique_violation();
    exception when unique_violation then
        return 'ok';
    end;
    return 'not reached';
end;
$$ language plpgsql;

SELECT catch_python_unique_violation();

/* manually starting subtransactions - a bad idea
 */
CREATE FUNCTION manual_subxact() RETURNS void AS $$
plpy.execute("savepoint save")
plpy.execute("create table foo(x integer)")
plpy.execute("rollback to save")
$$ LANGUAGE plpython3u;

SELECT manual_subxact();

/* same for prepared plans
 */
CREATE FUNCTION manual_subxact_prepared() RETURNS void AS $$
save = plpy.prepare("savepoint save")
rollback = plpy.prepare("rollback to save")
plpy.execute(save)
plpy.execute("create table foo(x integer)")
plpy.execute(rollback)
$$ LANGUAGE plpython3u;

SELECT manual_subxact_prepared();

/* raising plpy.spiexception.* from python code should preserve sqlstate
 */
CREATE FUNCTION plpy_raise_spiexception() RETURNS void AS $$
raise plpy.spiexceptions.DivisionByZero()
$$ LANGUAGE plpython3u;

DO $$
BEGIN
	SELECT plpy_raise_spiexception();
EXCEPTION WHEN division_by_zero THEN
	-- NOOP
END
$$ LANGUAGE plpgsql;

/* setting a custom sqlstate should be handled
 */
CREATE FUNCTION plpy_raise_spiexception_override() RETURNS void AS $$
exc = plpy.spiexceptions.DivisionByZero()
exc.sqlstate = 'SILLY'
raise exc
$$ LANGUAGE plpython3u;

DO $$
BEGIN
	SELECT plpy_raise_spiexception_override();
EXCEPTION WHEN SQLSTATE 'SILLY' THEN
	-- NOOP
END
$$ LANGUAGE plpgsql;

/* test the context stack trace for nested execution levels
 */
CREATE FUNCTION notice_innerfunc() RETURNS int AS $$
plpy.execute("DO LANGUAGE plpython3u $x$ plpy.notice('inside DO') $x$")
return 1
$$ LANGUAGE plpython3u;

CREATE FUNCTION notice_outerfunc() RETURNS int AS $$
plpy.execute("SELECT notice_innerfunc()")
return 1
$$ LANGUAGE plpython3u;

\set SHOW_CONTEXT always

SELECT notice_outerfunc();

/* test error logged with an underlying exception that includes a detail
 * string (bug #18070).
 */
CREATE FUNCTION python_error_detail() RETURNS SETOF text AS $$
  plan = plpy.prepare("SELECT to_date('xy', 'DD') d")
  for row in plpy.cursor(plan):
    yield row['d']
$$ LANGUAGE plpython3u;

SELECT python_error_detail();
