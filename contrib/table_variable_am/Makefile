# contrib/table_variable_am/Makefile

MODULE_big = table_variable_am
OBJS = \
	$(WIN32RES) \
	tv_heapam.o \
	tv_heapam_visibility.o \
	table_variable_am.o

PGFILEDESC = "Table Variable AM"

EXTENSION = table_variable_am
DATA = table_variable_am--1.0.sql


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/table_variable_am
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

