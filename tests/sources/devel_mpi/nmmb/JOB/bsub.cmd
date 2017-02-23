#!/bin/bash
#BSUB -n 64  
#BSUB -o ./logs/nmmb_%J.out
#BSUB -e ./logs/nmmb_%J.err
#BSUB -cwd . 
#BSUB -J NMMB-BSC 
#BSUB -W 00:10

export OMPI_MCA_coll_hcoll_enable=0
export OMPI_MCA_mtl=^mxm

time ./NMMBrrtm_RUN.sh  

exit
