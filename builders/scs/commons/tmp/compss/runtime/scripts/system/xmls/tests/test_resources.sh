#!/bin/bash

test_empty() {
  # Test the empty resources.xml creation

  # Import resources generation
  # shellcheck source=../generate_resources.sh
  # shellcheck disable=SC1091
  source ../generate_resources.sh

  # Define test files
  resources_gen="resources_test_empty.xml"
  resources_exp="resources_exp_empty.xml"

  # Dump file
  init "${resources_gen}"
  add_header
  add_footer

  # Compare to expected result
  ev=$(diff "${resources_gen}" "${resources_exp}")
  ev2=$?
  rm -f "${resources_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: Empty resources is different"
    exit 1
  else
    echo "[OK] test_empty passed"
  fi
}

test_simple() {
  # Test the simple resources.xml creation

  # Import resources generation
  # shellcheck source=../generate_resources.sh
  # shellcheck disable=SC1091
  source ../generate_resources.sh

  # Define test files
  resources_gen="resources_test_simple.xml"
  resources_exp="resources_exp_simple.xml"

  # Dump file
  init "${resources_gen}"
  add_header
  add_shared_disk "gpfs"
  add_compute_node "localhost" "4" "0" "" "" "43001" "43002" "" ""
  add_footer

  # Compare to expected result
  ev=$(diff "${resources_gen}" "${resources_exp}")
  ev2=$?
  rm -f "${resources_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: Simple resources is different"
    exit 1
  else
    echo "[OK] test_simple passed"
  fi
}

test_cloud() {
  # Test the cloud resources.xml creation

  # Import resources generation
  # shellcheck source=../generate_resources.sh
  # shellcheck disable=SC1091
  source ../generate_resources.sh

  # Define test files
  resources_gen="resources_test_cloud.xml"
  resources_exp="resources_exp_cloud.xml"

  # Dump file
  init "${resources_gen}"
  add_header
  add_shared_disks " gpfs gpfs2"
  add_compute_node "localhost" "4" "1" "1" "8192" "43001" "43002" "srun" "gpfs=/gpfs/ gpfs2=/.statelite/gpfs/"
  add_cloud "CP" "" "/path/to/conn.jar" "myConnector.MyConnector" "Image1" "gpfs=/gpfs/ gpfs2=/.statelite/gpfs/" "10" "" "" "" "small:1:0:0::1:0.85 medium:4:2:0:16000:1:1.25"
  add_footer

  # Compare to expected result
  ev=$(diff "${resources_gen}" "${resources_exp}")
  ev2=$?
  rm -f "${resources_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: cloud resources is different"
    exit 1
  else
    echo "[OK] test_cloud passed"
  fi
}


#
# MAIN
#

test_empty
test_simple
test_cloud

