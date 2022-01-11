#!/bin/bash -e

pycompss environment change cluster_env
pycompss app remove matmul
pycompss environment remove --force cluster_env