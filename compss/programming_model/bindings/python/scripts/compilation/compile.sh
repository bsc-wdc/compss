#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

###################################
## Compilation with mypyc script ##
###################################

mypyc --ignore-missing-imports --exclude 'pycompss\/((tests\/)|(dds\/)|(util\/objects\/replace.py)|(api\/local.py)|(api\/commons\/private_tasks.py)|(api\/software.py)|(functions\/data_tasks.py)|(streams\/)|(interactive.py)|(util\/interactive\/events.py)||(util\/interactive\/state.py)|(__main__.py))$' ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] Mypy compile failed with exit value: $ev"
  exit $ev
fi

# Copy the main pycompss compiled module one folder up to be found using
# python3 -m pycompss. Alternative would be to force use only python3 or
# define the path in PYTHONPATH (which can be dangerous).
cp *__mypyc* ..

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0
