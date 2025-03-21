#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
Main installation module.

This file contains the code to install the COMPSs' python binding and its
associated C/C++ modules.
"""

import os
import sys
from setuptools import setup
from setuptools import Extension


GCC_DEBUG_FLAGS = [
    "-Wall",
    "-Wextra",
    "-pedantic",
    "-O2",
    "-Wshadow",
    "-Wformat=2",
    "-Wfloat-equal",
    "-Wconversion",
    "-Wlogical-op",
    "-Wcast-qual",
    "-Wcast-align",
    "-D_GLIBCXX_DEBUG",
    "-D_GLIBCXX_DEBUG_PEDANTIC",
    "-D_FORTIFY_SOURCE=2",
    "-fsanitize=address",
    "-fstack-protector",
]

TARGET_OS = os.environ["TARGET_OS"]
JAVA_HOME = os.environ["JAVA_HOME"]
BINDING_DIR = os.environ["BINDING_DIR"]

if TARGET_OS == "Linux":
    INCLUDE_JDK = os.path.join(JAVA_HOME, "include", "linux")
    OS_EXTRA_COMPILE_COMPSS = ["-fPIC", "-std=c++11"]
elif TARGET_OS == "Darwin":
    INCLUDE_JDK = os.path.join(JAVA_HOME, "include", "darwin")
    OS_EXTRA_COMPILE_COMPSS = ["-fPIC", "-DGTEST_USE_OWN_TR1_TUPLE=1"]
else:
    INCLUDE_JDK = None
    OS_EXTRA_COMPILE_COMPSS = None
    print(f"ERROR: Unsupported OS {TARGET_OS} (Supported Linux/Darwin)")
    sys.exit(1)

# Bindings common extension
COMPSS_MODULE_EXT = Extension(
    "compss",
    include_dirs=[
        os.path.join(BINDING_DIR, "bindings-common", "src"),
        os.path.join(BINDING_DIR, "bindings-common", "include"),
        os.path.join(JAVA_HOME, "include"),
        INCLUDE_JDK,
    ],
    library_dirs=[os.path.join(BINDING_DIR, "bindings-common", "lib")],
    libraries=["bindings_common"],
    extra_compile_args=OS_EXTRA_COMPILE_COMPSS,
    sources=["src/pycompss/ext/compssmodule.cc"],
)

# Thread affinity extension
PROCESS_AFFINITY_EXT = Extension(
    "process_affinity",
    include_dirs=["src/pycompss/ext"],
    extra_compile_args=["-std=c++11"],
    # extra_compile_args=["-fPIC %s" % (" ".join(GCC_DEBUG_FLAGS.split("\n")))],
    sources=["src/pycompss/ext/process_affinity.cc"],
)

# DLB affinity extension
DLB_HOME = os.environ.get("DLB_HOME", None)
DLB_AFFINITY_EXT = None
if DLB_HOME is not None:
    DLB_AFFINITY_EXT = Extension(
        "dlb_affinity",
        include_dirs=[os.path.join(DLB_HOME, "include")],
        library_dirs=[os.path.join(DLB_HOME, "lib"), os.path.join(DLB_HOME, "lib64")],
        libraries=["dlb"],
        extra_compile_args=["-std=c++11"],
        # extra_compile_args=["-fPIC %s" % (" ".join(GCC_DEBUG_FLAGS.split("\n")))],
        sources=["src/pycompss/ext/dlb_affinity.c"],
    )

# EAR extension
EAR_HOME = os.environ.get("EAR_INSTALL_PATH", None)
EAR_EXT = None
if EAR_HOME is not None:
    EAR_EXT = Extension(
        "ear",
        include_dirs=[os.path.join(EAR_HOME, "include")],
        library_dirs=[os.path.join(EAR_HOME, "lib")],
        libraries=["earld_dummy"],
        extra_compile_args=["-std=c++11"],
        # extra_compile_args=["-fPIC %s" % (" ".join(GCC_DEBUG_FLAGS.split("\n")))],
        sources=["src/pycompss/ext/ear.c"],
    )

if TARGET_OS == "Linux":
    if DLB_HOME is None:
        OS_MODULES = [COMPSS_MODULE_EXT, PROCESS_AFFINITY_EXT]
    else:
        OS_MODULES = [COMPSS_MODULE_EXT, PROCESS_AFFINITY_EXT, DLB_AFFINITY_EXT]
    if EAR_HOME is not None:
        OS_MODULES += [EAR_EXT]
elif TARGET_OS == "Darwin":
    OS_MODULES = [COMPSS_MODULE_EXT]
else:
    # Unreachable code: will exit in previous if statement.
    OS_MODULES = None
    print(f"ERROR: Unsupported OS {TARGET_OS} (Supported Linux/Darwin)")
    sys.exit(1)


def main():
    """Adds extensions to pyproject.toml definition."""
    # This uses pyproject.toml metadata
    setup(ext_modules=OS_MODULES)


if __name__ == "__main__":
    main()
