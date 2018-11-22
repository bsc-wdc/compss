#!/bin/bash

test_empty() {
  # Test the empty project.xml creation

  # Import project generation
  # shellcheck source=../generate_project.sh
  # shellcheck disable=SC1091
  source ../generate_project.sh

  # Define test files
  project_gen="project_test_empty.xml"
  project_exp="project_exp_empty.xml"

  # Dump file
  init "${project_gen}"
  add_header
  add_footer

  # Compare to expected result
  ev=$(diff "${project_gen}" "${project_exp}")
  ev2=$?
  rm -f "${project_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: Empty project is different"
    exit 1
  else
    echo "[OK] test_empty passed"
  fi
}

test_simple() {
  # Test the simple project.xml creation

  # Import project generation
  # shellcheck source=../generate_project.sh
  # shellcheck disable=SC1091
  source ../generate_project.sh

  # Define test files
  project_gen="project_test_simple.xml"
  project_exp="project_exp_simple.xml"

  # Dump file
  init "${project_gen}"
  add_header
  add_master_node
  add_compute_node "localhost" "/opt/COMPSs/" "/tmp/COMPSsWorker" "" "" "" "" "" ""
  add_footer

  # Compare to expected result
  ev=$(diff "${project_gen}" "${project_exp}")
  ev2=$?
  rm -f "${project_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: Simple project is different"
    exit 1
  else
    echo "[OK] test_simple passed"
  fi
}

test_cloud() {
  # Test the cloud project.xml creation

  # Import project generation
  # shellcheck source=../generate_project.sh
  # shellcheck disable=SC1091
  source ../generate_project.sh

  # Define test files
  project_gen="project_test_cloud.xml"
  project_exp="project_exp_cloud.xml"

  # Dump file
  init "${project_gen}"
  add_header
  add_master_node "gpfs=/gpfs/ gpfs2=/.statelite/gpfs/"
  add_compute_node "localhost" "/opt/COMPSs/" "/tmp/COMPSsWorker" "user" "/tmp/" "/usr/lib/" "/opt/COMPSs/Runtime/compss-engine.jar" "/opt/COMPSs/Bindings/python/2" "2"
  add_cloud 0 1 3 "CP" "prop1=value1 prop2=/my/value/2" "COMPSsImage" "/opt/COMPSs/" "/tmp/COMPSsWorker" "" "" "" "/opt/COMPSs/Runtime/compss-engine.jar" "" "" "small medium big"
  add_footer

  # Compare to expected result
  ev=$(diff "${project_gen}" "${project_exp}")
  ev2=$?
  rm -f "${project_gen}"
  if [ -n "$ev" ] || [ $ev2 -ne 0 ]; then
    echo "ERROR: cloud project is different"
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

