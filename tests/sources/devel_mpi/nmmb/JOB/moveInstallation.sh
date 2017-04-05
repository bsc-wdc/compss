#!/bin/bash -e
 
  moveTo=$1

  scriptDir=$(pwd)
  variable=${scriptDir}/../PREPROC/VARIABLE
  postproc=${scriptDir}/../POSTPROC/

  cd $variable
  rm -f compile_read_paul_source.sh
  ln -s compile_read_paul_source_${moveTo}.sh compile_read_paul_source.sh

  cd $postproc
  rm -f pre_postproc_auth.sh
  ln -s pre_postproc_auth_${moveTo}.sh pre_postproc_auth.sh

  echo "DONE"

