#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(readingdb, 0.6.0, stevedh@eecs.berkeley.edu)
AC_CONFIG_HEADER([c6/config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AM_INIT_AUTOMAKE([-Wall])
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_RANLIB
dnl AC_PROG_PYTHON
dnl AC_PROG_SWIG

# PKG_CHECK_MODULES([CHECK], [check >= 0.9.4]) 
# Checks for libraries.

AC_ARG_ENABLE([compression],
        AS_HELP_STRING([--disable-compression], [Disable database compression]))
AS_IF([test "x$enable_compression" != "xno"], AC_DEFINE([USE_COMPRESSION], [], [Use compressed database]))

AC_ARG_ENABLE([compression-log],
        AS_HELP_STRING([--enable-compression-log], [Write a log for each write of compression statistics]))
AS_IF([test "x$enable_compression_log" = "xyes"], AC_DEFINE([WRITE_COMPRESSION_LOG], [], [Write compression log]))

dnl user to run as
AC_MSG_CHECKING(user to run as)
AC_ARG_WITH([username],
        [AC_HELP_STRING([--with-username], [User to run readingdb database as (readingdb)])],
        READINGDB_USER=$withval,
        READINGDB_USER=readingdb)
AC_MSG_RESULT($READINGDB_USER)
AC_DEFINE_UNQUOTED([READINGDB_USER], ["$READINGDB_USER"], [User to run as])

dnl find datadir
AC_MSG_CHECKING(default data directory)
AC_ARG_WITH([datadir],
        [AC_HELP_STRING([--with-datadir], [Default data directory (/var/lib/readingdb)])],
        DATA_DIR=$withval,
        DATA_DIR=/var/lib/readingdb)
AC_MSG_RESULT($DATA_DIR)
AC_DEFINE_UNQUOTED([DATA_DIR], ["$DATA_DIR"], [Default data directory])


# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h endian.h libkern/OSByteOrder.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memset])

# bdb
AC_SEARCH_LIBS(db_strerror, [db], , AC_MSG_ERROR([ERROR: can't link against berkeley db]))
dnl AC_MSG_CHECKING(for acceptable berkeley db version)
dnl # there's got to be a better way to check library versions...  
dnl # we require >= 4.8.28 since (a) they changed the on-disk format then
dnl # and I don't want to deal with the old one, and (b) pretty sure
dnl # that's when they added compression.
dnl AC_COMPILE_IFELSE(AC_LANG_PROGRAM([#include <db.h>], [
dnl # if DB_VERSION_MAJOR >= 4
dnl #  if DB_VERSION_MINOR >= 8
dnl #   if DB_VERSION_PATCH >= 28
dnl #   else
dnl #    error
dnl #   endif
dnl #  else
dnl #   error
dnl #  endif
dnl # else
dnl #  error
dnl # endif
dnl ]), AC_MSG_RESULT([yes]), AC_MSG_ERROR([Wrong bdb version: >= 4.8.28 required]))

# assume they have pthreads for now...
AC_SEARCH_LIBS(pthread_create, [pthread], ,AC_MSG_ERROR([ERROR: can't find pthreds]))
AC_SEARCH_LIBS(compress2, [z], , AC_MSG_ERROR([ERROR: can't find zlib]))

# check for protoc-c compiler
AC_PATH_PROG(PROTOC, protoc-c)
if test "x$PROTOC" = x; then 
  echo "ERROR: cannot find protobuf-c, required"
fi
# check for proto-c libraries
AC_CHECK_HEADERS(google/protobuf-c/protobuf-c.h)
AC_SEARCH_LIBS(protobuf_c_message_pack, [protobuf-c], , AC_MSG_ERROR([ERROR: can't link against protobuf-c library]))

AC_CONFIG_FILES([Makefile c6/Makefile c6/pbuf/Makefile])
AC_CONFIG_SUBDIRS([c6/hashtable/])
AC_OUTPUT

