#!/bin/bash -e

  # Remove compss
  apt-get -y --force-Yes remove compss-*

  # Purge compss
  apt-get -y --force-Yes purge compss-*

  # Clean all
  apt-get -y --force-Yes autoremove
  apt-get clean

  # Update DB
  apt-get update

  # Exit with status from last command
  exit

