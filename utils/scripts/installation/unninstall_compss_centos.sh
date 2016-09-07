#!/bin/bash -e

  # Remove compss
  yum remove -y compss-*

  # Purge compss

  # Clean all
  yum clean all

  # Update DB
  yum update -y

  # Exit with status from last command
  exit

