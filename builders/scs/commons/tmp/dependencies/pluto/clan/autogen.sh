#!/bin/sh

set -e

# (Re)Generate autotools files
autoreconf -vi

# (Re)Generate autotools files for OSL if it exists
if test -f osl/autogen.sh; then
  (cd osl; ./autogen.sh)
fi
