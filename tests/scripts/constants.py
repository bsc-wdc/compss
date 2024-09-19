#!/usr/bin/env python3

# Imports
import os
import sys

############################################
# CONSTANTS
############################################

DEFAULT_SKIP = True
DEFAULT_NUM_RETRIES = 3
DEFAULT_FAIL_FAST = False
DEFAULT_SC_FAMILIES = ["simple", "apps/java", "apps/python/examples", "apps/c"]
DEFAULT_SC_IGNORED = [
    ".target",
    "target",
    ".idea",
    ".settings",
    ".git",
    "geneDetection",
]
DEFAULT_FAMILIES = [
    "agents",
    "agents_transfers",
    "c",
    "cloud",
    "java",
    "performance",
    "pscos",
    "python",
    "tools",
    "fault_tolerance",
    "gos",
]
DEFAULT_CLI_FAMILIES = ["environment", "runapps", "jupyter"]
DEFAULT_IGNORED = [
    ".target",
    "target",
    ".idea",
    ".settings",
    ".git",
    "gat",
    "gos_batch",
]
DEFAULT_TESTS = []

if sys.platform == "darwin":
    DEFAULT_CFG_FILE = "NIO_mac.cfg"
else:
    DEFAULT_CFG_FILE = "NIO.cfg"
DEFAULT_SC_CFG_FILE = "MN.cfg"
DEFAULT_CFG_EXTENSION = ".cfg"
DEFAULT_COMPSS_HOME = "/opt/COMPSs/"
DEFAULT_REL_COMPSS_LOG_DIR = ".COMPSs"
DEFAULT_REL_TARGET_TESTS_DIR = "tests_execution_sandbox"
DEFAULT_COMM = "es.bsc.compss.nio.master.NIOAdaptor"
DEFAULT_EXECUTION_ENVS = ["python3"]
DEFAULT_SC_EXECUTION_ENVS = ["shared_disk", "local_disk"]
DEFAULT_COMPSS_MODULE = "COMPSs/Trunk"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(SCRIPT_DIR, "../sources/local")
TESTS_CLI_DIR = os.path.join(SCRIPT_DIR, "../sources/cli")
TESTS_SC_DIR = os.path.join(SCRIPT_DIR, "../sources/sc")
REMOTE_SCRIPTS_REL_PATH = "remote_sc/"
CONFIGURATIONS_DIR = os.path.join(SCRIPT_DIR, "../configurations")
PYCOMPSS_SRC_DIR = os.path.join(
    SCRIPT_DIR, "../../compss/programming_model/bindings/python/src"
)
RUNCOMPSS_REL_PATH = "Runtime/scripts/user/runcompss"
ENQUEUE_COMPSS_REL_PATH = "Runtime/scripts/user/enqueue_compss"
CLEAN_PROCS_REL_PATH = "Runtime/scripts/utils/compss_clean_procs"
JACOCO_LIB_REL_PATH = "Tools/jacoco/lib/"
