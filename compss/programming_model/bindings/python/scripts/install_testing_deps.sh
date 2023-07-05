#!/bin/bash

echo "Installing required dependencies to run the unittests..."

# Testing, analysis, etc.
python3 -m pip install coverage --user
python3 -m pip install pycodestyle pydocstyle flake8 pep8-naming --user
python3 -m pip install pylint bandit prospector py --user

# General
python3 -m pip install mpi4py --user
python3 -m pip install numpy dill guppy3 memory_profiler matplotlib decorator --user
python3 -m pip install jupyter pytest nbval pytest-cov pytest-notebook ipyparallel jupyter_nbextensions_configurator jupyterlab --user
python3 -m ipykernel install --user

# Jupyter specific
python3 -m pip install pytest nbval pytest-cov pytest-html-profiling pytest-metadata pytest-profiling pytest-subprocess pytest-sugar --user

# DDS specific
python3 -m pip install spacy --user
python3 -m spacy download en_core_web_sm

echo "------------------------------------------------"
echo "| REMINDER: Install PyCOMPSs completely before |"
echo "|           running the integration tests.     |"
echo "------------------------------------------------"
