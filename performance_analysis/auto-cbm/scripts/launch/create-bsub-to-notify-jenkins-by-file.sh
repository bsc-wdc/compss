#!/bin/bash

# AS FIRST PARAMETER: jobDependencyID
# CREATE A FILE NAMED cbm-finished, USING BSUB. POLLING WILL CHECK FOR THIS FILE TO KNOW WHEN 
# CBM HAS FINISHED.


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
touch cbm-finished \n\
"
