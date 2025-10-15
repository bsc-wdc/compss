#!/bin/bash
#PJM -L elapse=02:00:00
#PJM -L "node=1"
#PJM -S
echo "Installing COMPSs in $TARGET"
source env.sh
cd COMPSs

./install -A $TARGET cte-arm.cfg
if [ $? -ne 0 ]; then
	echo "ERROR installing COMPSs"
	exit 1
fi
echo "COMPSs INSTALLED!"
