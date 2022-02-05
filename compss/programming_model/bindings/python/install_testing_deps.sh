#!/bin/bash

echo "Installing required dependencies to run the unittests..."

# General
python2 -m pip install nose --user
python3 -m pip install nose --user
python2 -m pip install coverage --user
python3 -m pip install coverage --user
python3 -m pip install pycodestyle --user
python3 -m pip install mpi4py --user
python3 -m pip install numpy dill guppy3 memory_profiler matplotlib decorator --user
python3 -m pip install jupyter pytest nbval pytest-cov pytest-notebook ipyparallel jupyter_nbextensions_configurator jupyterlab --user
python3 -m ipykernel install --user

# Jupyter specific
python2 -m pip install nbval --user
python3 -m pip install nbval --user
python2 -m pip install pytest --user
python3 -m pip install pytest --user
python2 -m pip install pytest-cov --user
python3 -m pip install pytest-cov --user

# DDS specific
python2 -m pip install spacy --user
python3 -m pip install spacy --user
python3 -m spacy download en_core_web_sm

echo "------------------------------------------------"
echo "| REMINDER: Install PyCOMPSs completely before |"
echo "|           running the integration tests.     |"
echo "------------------------------------------------"
