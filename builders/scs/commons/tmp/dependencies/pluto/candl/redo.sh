#!/bin/sh
make maintainer-clean
#./get_submodules.sh
./autogen.sh
./configure  \
            --prefix=$HOME/usr \
            --with-osl=system \
            --with-osl-prefix=$HOME/usr \
            --with-piplib=system \
            --with-piplib-prefix=$HOME/usr 

make
