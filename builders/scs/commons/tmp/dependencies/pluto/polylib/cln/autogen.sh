#!/bin/sh
libtoolize -c --force
aclocal -I m4
autoheader
automake -a -c --foreign
autoconf
