#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(hashtable, 0.1, stevedh@eecs.berkeley.edu)
AC_CONFIG_SRCDIR([src/hashtable.c])
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AM_INIT_AUTOMAKE([-Wall])
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_RANLIB

PKG_CHECK_MODULES([CHECK], [check >= 0.9.4]) 
# Checks for libraries.

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

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 tests/Makefile])
AC_OUTPUT
