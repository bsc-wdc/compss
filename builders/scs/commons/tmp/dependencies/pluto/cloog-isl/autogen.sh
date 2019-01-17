#!/bin/sh

set -e

# (Re)Generate autotools files
autoreconf -vi

# (Re)Generate autotools files for ISL if it exists
if test -f isl/autogen.sh; then
  (cd isl; ./autogen.sh)
fi
