#!/bin/bash

echo "Running mpi_run.sh"
echo "Following modules will be loaded:"
echo "$MODULES_TO_LOAD"

ssh -o StrictHostKeyChecking=no $(hostname) /bin/bash <<EOF
export COMPSS_MPIRUN_TYPE=$COMPSS_MPIRUN_TYPE
module try-load ${MODULES_TO_LOAD//:/ }
$@

EOF
