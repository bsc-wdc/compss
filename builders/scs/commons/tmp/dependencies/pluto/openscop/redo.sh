#!/bin/sh

set -e

make maintainer-clean
./autogen.sh
./configure --prefix="$HOME"/usr --with-gmp=system --with-gmp-prefix=/usr
make
