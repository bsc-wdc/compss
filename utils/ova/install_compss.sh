#!/bin/bash -e
  
  TRUNK="$HOME/trunk/"
  BUILDERS="$TRUNK/builders"

  cd "$BUILDERS"
  sudo -E ./buildlocal

