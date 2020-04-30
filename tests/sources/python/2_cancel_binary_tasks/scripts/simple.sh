#!/bin/bash -e

  clean() {
    echo "Handle kill signal"
    # All ok
    exit 0
  }

  trap clean SIGTERM

  echo "Hello World!"
  echo "Waiting for signal..."
  sleep 10s
  echo "Waiting for signal..."
  sleep 10s
  echo "Waiting for signal..."
  sleep 10s
  echo "Waiting for signal..."
  sleep 10s
  echo "Waiting for signal..."
  sleep 10s
  echo "Waiting for signal..."
  sleep 10s

  # If we reach this point the signal has not been caught
  echo "Signal not caught"
  exit 1
