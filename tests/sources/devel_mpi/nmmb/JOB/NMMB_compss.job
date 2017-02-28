#!/bin/bash

  # MUST BE FIXED -------------------
  ulimit -s unlimited
  
  export PATH=$PATH:/gpfs/projects/bsc19/bsc19533/NMMB-BSC/nmmb-bsc-ctm-v2.0/MODEL/exe
  export UMO_PATH=/gpfs/projects/bsc19/bsc19533/NMMB-BSC/nmmb-bsc-ctm-v2.0
  export FIX=$UMO_PATH/PREPROC/FIXED
  export VRB=$UMO_PATH/PREPROC/VARIABLE
  export OUTPUT=$UMO_PATH/PREPROC/output
  export UMO_ROOT=$UMO_PATH/JOB
  export SRCDIR=$UMO_PATH/MODEL
  export UMO_OUT=$UMO_PATH/OUTPUT/CURRENT_RUN
  export POST_CARBONO=$UMO_PATH/POSTPROC
  export GRB=$UMO_PATH/DATA/INITIAL
  export DATMOD=$UMO_PATH/DATA/STATIC
  export CHEMIC=
  export STE=
  export OUTNMMB=$UMO_PATH/OUTPUT
  export OUTGCHEM=
  export PREMEGAN=
  export TMP=/tmp
  
  export FNL=$GRB
  export GFS=$GRB
  # END MUST BE FIXED ----------------
  
  scriptDir=$(pwd)/$(dirname $0)
  propertiesFile=${scriptDir}/nmmb_compss.properties

  enqueue_compss \
    --exec_time=10 \
    --num_nodes=2 \
    --job_dependency=None \
    --tasks_per_node=16 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --classpath=${scriptDir}/../nmmb.jar \
    --network=infiniband \
    --log_level=debug \
    --tracing=false \
    --graph=true \
    nmmb.Nmmb ${propertiesFile}

