#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

LIBCUTIL_VSN="401b4bfb2d04f84a3cf38a0beca2545847c38cb0"   # libcutil master May 3, 2013

set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

BASEDIR="$PWD"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

# Changed "make" to $MAKE

case "$1" in
    clean)
        rm -rf libcutil
        ;;

    test)
        (cd libcutil/build && $MAKE test)

        ;;

    get-deps)
        if [ ! -d libcutil ]; then
            git clone git://github.com/leo-project/libcutil.git
            (cd libcutil && git checkout $LIBCUTIL_VSN)
        fi
        ;;

    *)

        if [ ! -d libcutil ]; then
            git clone git://github.com/leo-project/libcutil.git
            (cd libcutil && git checkout $LIBCUTIL_VSN)
        fi
        cd libcutil
        if [ ! -d build ]; then
            mkdir build
        fi
        (cd build && cmake .. && $MAKE)

        ;;
esac
