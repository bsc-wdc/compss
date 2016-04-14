#!/bin/bash
  cd $(dirname $0)
  
  echo "Installing compss-framework in bscgrid06"
  scp install_rpm.sh root@bscgrid06:.
  ssh root@bscgrid06 "sh install_rpm.sh x86_64; rm install_rpm.sh"

