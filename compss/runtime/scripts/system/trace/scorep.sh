#!/bin/bash -e

module use /lustre/home/cherold/sw/modules
module load scorep_python/io scorep_preload/git
# Set path to the application - this even better because the submission is independent of the current working directory
export SCOREP_PROFILING_ENABLE_CORE_FILES=1
#export SCOREP_FILTERING_FILE=/PATH/TO/pycompss.flt # lustre/home/cherold/pycompss.flt
export SCOREP_ENABLE_PROFILING=false
export SCOREP_ENABLE_TRACING=true
export SCOREP_TOTAL_MEMORY=500MB
export SCOREP_SUBSTRATE_PLUGINS=vampir_groups_writer