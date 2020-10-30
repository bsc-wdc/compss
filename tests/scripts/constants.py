#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os

############################################
# CONSTANTS
############################################

DEFAULT_SKIP = True
DEFAULT_NUM_RETRIES = 3
DEFAULT_FAIL_FAST = False
DEFAULT_SC_FAMILIES = [
                       "simple",
                       "apps/java",
                       "apps/python/examples",
                       "apps/c"]
DEFAULT_SC_IGNORED = [".target", "target", ".idea", ".settings", ".git", "geneDetection"]
DEFAULT_FAMILIES = [
                    "agents",
                    "autoparallel",
                    "c",
                    "cloud",
                    "gat",
                    "java",
                    "performance",
                    "pscos",
                    "python",
                    "tools",
                    "fault_tolerance"]
DEFAULT_IGNORED = [".target", "target", ".idea", ".settings", ".git"]
DEFAULT_TESTS = []

DEFAULT_CFG_FILE = "NIO.cfg"
DEFAULT_SC_CFG_FILE = "MN.cfg"
DEFAULT_CFG_EXTENSION = ".cfg"
DEFAULT_COMPSS_HOME = "/opt/COMPSs/"
DEFAULT_REL_COMPSS_LOG_DIR = ".COMPSs"
DEFAULT_REL_TARGET_TESTS_DIR = "tests_execution_sandbox"
DEFAULT_COMM = "es.bsc.compss.nio.master.NIOAdaptor"
DEFAULT_EXECUTION_ENVS = ["python2", "python3", "python2_mpi", "python3_mpi"]
DEFAULT_SC_EXECUTION_ENVS = ["shared_disk", "local_disk"]
DEFAULT_COMPSS_MODULE = "COMPSs/Trunk"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(SCRIPT_DIR, "../sources/local")
TESTS_SC_DIR = os.path.join(SCRIPT_DIR, "../sources/sc")
REMOTE_SCRIPTS_REL_PATH = "remote_sc/"
CONFIGURATIONS_DIR = os.path.join(SCRIPT_DIR, "../configurations")
RUNCOMPSS_REL_PATH = "Runtime/scripts/user/runcompss"
ENQUEUE_COMPSS_REL_PATH = "Runtime/scripts/user/enqueue_compss"
CLEAN_PROCS_REL_PATH = "Runtime/scripts/utils/compss_clean_procs"
JACOCO_LIB_REL_PATH ="Tools/jacoco/lib/"
