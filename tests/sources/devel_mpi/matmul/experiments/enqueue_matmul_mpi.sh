#!/bin/bash

  MSIZE=$1
  BSIZE=$2
  NUM_PROCS=$3
  NUM_REQ=$4
  EXEC_TIME=$5
  JOB_DEP=$6

  SIZE=$((MSIZE*BSIZE))

  cat > mpi_matmul_${MSIZE}_${BSIZE}_${NUM_PROCS}.cmd << EOT
#!/bin/bash
#BSUB -n ${NUM_REQ}
#BSUB -x
#BSUB -o ./matmul_mpi_%J.out
#BSUB -e ./matmul_mpi_%J.err
#BSUB -cwd .
EOT

  if [ "$JOB_DEP" != "None" ]; then
    cat >> mpi_matmul_${MSIZE}_${BSIZE}_${NUM_PROCS}.cmd << EOT
#BSUB -J MATMUL_MPI -w 'ended($JOB_DEP)'
EOT
  else 
    cat >> mpi_matmul_${MSIZE}_${BSIZE}_${NUM_PROCS}.cmd << EOT
#BSUB -J MATMUL_MPI
EOT
  fi

  cat >> mpi_matmul_${MSIZE}_${BSIZE}_${NUM_PROCS}.cmd << EOT
#BSUB -W 00:$EXEC_TIME

echo "RUNNING FOR PROCS=${NUM_PROCS} ; MSIZE=$MSIZE ; BSIZE=$BSIZE ; SIZE=$SIZE"

AIN="/scratch/tmp/A.in"
BIN="/scratch/tmp/B.in"
COUT="/scratch/tmp/C.out"

echo "- Remove previous files"
rm -f \$AIN \$BIN \$COUT

echo "- Generate new files"
for (( i=1; i<=$SIZE; i++ )); do
  aux1=\$RANDOM\$RANDOM\$RANDOM
  aux2=\$(printf "%01d.%016d" \$((\$RANDOM%10)) \$aux1)
  echo "\$aux2" >> \$AIN

  aux1=\$RANDOM\$RANDOM\$RANDOM
  aux2=\$(printf "%01d.%016d" \$((\$RANDOM%10)) \$aux1)
  echo "\$aux2" >> \$BIN

  echo "0.0" >> \$COUT
done

echo "- Launch MPI Matmul"
START=\$(date +%s.%N)
time mpirun -np ${NUM_PROCS} -report-bindings ./bin/matmul $SIZE \$AIN \$BIN \$COUT
END=\$(date +%s.%N)
DIFF=\$(echo "\$END - \$START" | bc)
echo "[EXECUTION TIME] \$DIFF"

echo "Clean files"
rm -f \$AIN \$BIN \$COUT

exit
EOT

  echo "Submitting MPI job"
  bsub < mpi_matmul_${MSIZE}_${BSIZE}_${NUM_PROCS}.cmd

