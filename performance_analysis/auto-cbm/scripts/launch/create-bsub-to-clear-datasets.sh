#!/bin/bash

# AS FIRST PARAMETER: jobDependencyID
# REMOVES ALL datasets USED BY ALL APPS.


JOBID=$1

echo -e \ "\
#!/bin/bash \n\
# \n\
#BSUB -J COMPSs -w 'ended($1)' \n\
#BSUB -cwd . \n\
#BSUB -oo cbm-finished.out \n\
#BSUB -eo cbm-finished.err \n\
#BSUB -n 2 \n\
#BSUB -R \"span[ptile=1]\" \n\
#BSUB -W \"00:1\" \n\
\n\
rm -rf cbm*/*.log \n\
rm -rf matmul*/*.log \n\
rm -rf matmul*/A\.* \n\
rm -rf matmul*/B\.* \n\
rm -rf matmul*/C\.* \n\
rm -rf cbm*/dummy* \n\
"
