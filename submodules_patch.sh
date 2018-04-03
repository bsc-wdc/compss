#!/bin/bash -e

  # PLUTO
  echo "* Patching PLUTO submodule"
  (cd dependencies/pluto; ./submodules_get.sh)

  # PyCOMPSs AutoParallel
  echo "* Patching AutoParallel submodule"
  rm -f compss/programming_model/bindings/python/src/pycompss/api/parallel.py
  rm -rf compss/programming_model/bindings/python/src/pycompss/api/tests
  rm -rf compss/programming_model/bindings/python/src/pycompss/util/translators

  cp dependencies/autoparallel/pycompss/api/parallel.py compss/programming_model/bindings/python/src/pycompss/api/
  cp -r dependencies/autoparallel/pycompss/api/tests/ compss/programming_model/bindings/python/src/pycompss/api/
  cp -r dependencies/autoparallel/pycompss/util/translators/ compss/programming_model/bindings/python/src/pycompss/util/

  echo "DONE"
  exit 0

