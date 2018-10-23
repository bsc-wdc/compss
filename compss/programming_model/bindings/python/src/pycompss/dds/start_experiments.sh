!/bin/bash

  # Define script variables

  scriptDir=$(pwd)/$(dirname $0)
  execFile=${scriptDir}/examples.py
  appClasspath=${scriptDir}/
  appPythonpath=${scriptDir}/

for size in 512 256;
do

  size_dir=$scriptDir/experiments/$size
  mkdir -p $size_dir
  cd $size_dir

  file_name=/gpfs/projects/bsc19/COMPSs_DATASETS/wordcount/data/line_separated/$size.txt

  for chunk_size in 5048576;
  do
    chunk_dir=$size_dir/$chunk_size
    mkdir -p $chunk_dir
    cd $chunk_dir

    for i in 9 5 2;
    do
      node_dir="$chunk_dir/$i"
      mkdir -p $node_dir
      cd $node_dir

      # Retrieve arguments
      numNodes=$i
      executionTime="$((25-$i))"
      tracing=false

      comma="enqueue_compss \
                --job_dependency=None \
                --num_nodes=$numNodes \
                --exec_time=$executionTime \
                --tracing=$tracing \
                --graph=false \
                --classpath=$appClasspath \
                --pythonpath=$appPythonpath \
                --lang=python \
                --qos=debug \
                --worker_in_master_cpus=0 \
                $execFile $file_name results.txt $chunk_size"
      echo "$comma" > test.txt
      $comma
      sleep 2
    done
  done
done
