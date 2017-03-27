#/bin/bash

  export BLAST_BINARY=/gpfs/home/bsc19/bsc19533/blast/binary/blastall

  debug=false
  database=/gpfs/home/bsc19/bsc19533/blast/databases/swissprot/swissprot
  query=/gpfs/home/bsc19/bsc19533/blast/sequences/sargasso_test.fasta
  nFrags=8
  tmpDir=/scratch/tmp/
  outputFile=/gpfs/home/bsc19/bsc19533/blast/output.txt

  enqueue_compss \
    --exec_time=20 \
    --num_nodes=2 \
    --gpus_per_node=2 \
    --tasks_per_node=8 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --network=infiniband \
    --log_level=off \
    --classpath=/gpfs/home/bsc19/bsc19533/blast/blast_new.jar \
    blast.Blast $debug $database $query $nFrags $tmpDir $outputFile

