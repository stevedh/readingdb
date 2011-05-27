#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(readingdb, 6.0, stevedh@eecs.berkeley.edu)
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AM_INIT_AUTOMAKE([-Wall])
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_RANLIB

# PKG_CHECK_MODULES([CHECK], [check >= 0.9.4]) 
# Checks for libraries.

AC_ARG_ENABLE([compression],
        AS_HELP_STRING([--disable-compression], [Disable database compression]))
AS_IF([test "x$enable_compression" != "xno"], AC_DEFINE([USE_COMPRESSION], [], [Use compressed database]))

AC_ARG_ENABLE([compression-log],
        AS_HELP_STRING([--enable-compression-log], [Write a log for each write of compression statistics]))
AS_IF([test "x$enable_compression_log" = "xyes"], AC_DEFINE([WRITE_COMPRESSION_LOG], [], [Write compression log]))

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memset])

# bdb
AC_SEARCH_LIBS(db_strerror, [db], , AC_MSG_ERROR([ERROR: can't link against berkeley db]))
AC_MSG_CHECKING(for acceptable berkeley db version)
# there's got to be a better way to check library versions...  
# we require >= 4.8.28 since (a) they changed the on-disk format then
# and I don't want to deal with the old one, and (b) pretty sure
# that's when they added compression.
AC_COMPILE_IFELSE(AC_LANG_PROGRAM([#include <db.h>], [
# if DB_VERSION_MAJOR >= 4
#  if DB_VERSION_MINOR >= 8
#   if DB_VERSION_PATCH >= 28
#   else
#    error
#   endif
#  else
#   error
#  endif
# else
#  error
# endif
]), AC_MSG_RESULT([yes]), AC_MSG_ERROR([Wrong bdb version: >= 4.8.28 required]))

# assume they have pthreads for now...

# check for protoc-c compiler
AC_PATH_PROG(PROTOC, protoc-c)
if test "x$PROTOC" = x; then 
  echo "ERROR: cannot find protobuf-c, required"
fi
# check for proto-c libraries
AC_CHECK_HEADERS(google/protobuf-c/protobuf-c.h)
AC_SEARCH_LIBS(protobuf_c_message_pack, [protobuf-c], , AC_MSG_ERROR([ERROR: can't link against protobuf-c library]))

AC_CONFIG_FILES([Makefile pbuf/Makefile])
AC_CONFIG_SUBDIRS([hashtable/])
AC_OUTPUT
