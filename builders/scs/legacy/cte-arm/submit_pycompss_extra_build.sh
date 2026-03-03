#!/bin/bash
#PJM -L elapse=02:00:00
#PJM -L "node=1"
#PJM -S
echo "Installing COMPSs in $TARGET"
source venv.sh
cd COMPSs/Bindings/python

python_version=3

./install "$TARGET"/Bindings/python false python${python_version}
if [ $? -ne 0 ]; then
	echo "ERROR installing extra PyCOMPSs"
	exit 1
fi
echo "Extra PyCOMPSs INSTALLED!"
