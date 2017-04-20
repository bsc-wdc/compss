#!/bin/bash
#BSUB -n 16 
#BSUB -x 
#BSUB -o ./mpi_outputs/matmul_mpi_%J.out
#BSUB -e ./mpi_outputs/matmul_mpi_%J.err
#BSUB -cwd . 
#BSUB -J MATMUL_MPI
#BSUB -W 00:10

echo "RUNNING FOR PROCS=${MPI_PROCS} ; BSIZE=$BSIZE"

AIN="/scratch/tmp/A.in"
BIN="/scratch/tmp/B.in"
COUT="/scratch/tmp/C.out"

echo "- Remove previous files"
rm -f $AIN $BIN $COUT

echo "- Generate new files"
for (( i=1; i<=$BSIZE; i++ )); do
  aux=$(printf "%03d.%05d" $(( $RANDOM % 1000 )) $(( $RANDOM % 100000)))
  echo "$aux" >> $AIN

  aux=$(printf "%03d.%05d" $(( $RANDOM % 1000 )) $(( $RANDOM % 100000)))
  echo "$aux" >> $BIN

  echo "0.0" >> $COUT
done

echo "- Launch MPI Matmul"
START=$(date +%s.%N)
time mpirun -np ${MPI_PROCS} --bind-to core ./bin/matmul $BSIZE $AIN $BIN $COUT
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)
echo "[EXECUTION TIME] $DIFF"

echo "Clean files"
rm -f $AIN $BIN $COUT

exit

