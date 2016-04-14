#!/bin/sh

app_dir=$1
py_path=$2
export PYCOMPSS_HOME=`dirname $0`/../../bindings/python
export PYTHONPATH=$app_dir:$py_path:$PYCOMPSS_HOME
shift 2

python $PYCOMPSS_HOME/pycompss/worker/worker.py $*

if [ $? -eq 0 ]; then
	exit 0
else
	echo 1>&2 "Task execution failed"
	exit 7
fi

