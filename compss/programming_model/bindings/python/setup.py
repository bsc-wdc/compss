#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import re
import sys
import pathlib
from setuptools import setup, Extension

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
if TARGET_OS == "Linux":
    INCLUDE_JDK = os.path.join(os.environ["JAVA_HOME"], "include", "linux")
    OS_EXTRA_COMPILE_COMPSS = ["-fPIC", "-std=c++11"]
elif TARGET_OS == "Darwin":
    INCLUDE_JDK = os.path.join(os.environ["JAVA_HOME"], "include", "darwin")
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
        "../bindings-common/src",
        "../bindings-common/include",
        os.path.join(os.environ["JAVA_HOME"], "include"),
        INCLUDE_JDK,
    ],
    library_dirs=["../bindings-common/lib"],
    libraries=["bindings_common"],
    extra_compile_args=OS_EXTRA_COMPILE_COMPSS,
    sources=["src/ext/compssmodule.cc"],
)

# Thread affinity extension
PROCESS_AFFINITY_EXT = Extension(
    "process_affinity",
    include_dirs=["src/ext"],
    extra_compile_args=["-std=c++11"],
    # extra_compile_args=["-fPIC %s" % (" ".join(GCC_DEBUG_FLAGS.split("\n")))],
    sources=["src/ext/process_affinity.cc"],
)

# dlb affinity extension
DLB_HOME = os.environ.get("DLB_HOME", None)
DLB_AFFINITY_EXT = None
if DLB_HOME is not None:
    DLB_AFFINITY_EXT = Extension(
        "dlb_affinity",
        include_dirs=[os.path.join(DLB_HOME, "include")],
        library_dirs=[os.path.join(DLB_HOME, "lib"), os.path.join(DLB_HOME, "lib64")],
        libraries=["dlb"],
        extra_compile_args=["-std=c++11"],
        sources=["src/ext/dlb_affinity.c"],
    )


# Helper method to find packages
def find_packages(path="./src"):
    """Find packages within the given path.

    :param path: Source path.
    :return: List of packages.
    """
    ret = []
    for root, _, files in os.walk(path, followlinks=True):
        if "__init__.py" in files:
            # Erase src header from package name
            pkg_name = root[6:]
            # Replace / by .
            pkg_name = pkg_name.replace("/", ".")
            # Erase non UTF characters
            pkg_name = re.sub("^[^A-z0-9_]+", "", pkg_name)
            # Add package to list
            ret.append(pkg_name)
    return ret


if TARGET_OS == "Linux":
    if DLB_HOME is None:
        OS_MODULES = [COMPSS_MODULE_EXT, PROCESS_AFFINITY_EXT]
    else:
        OS_MODULES = [COMPSS_MODULE_EXT, PROCESS_AFFINITY_EXT, DLB_AFFINITY_EXT]
elif TARGET_OS == "Darwin":
    OS_MODULES = [COMPSS_MODULE_EXT]
else:
    # Unreachable code: will exit in previous if statement.
    OS_MODULES = None
    print(f"ERROR: Unsupported OS {TARGET_OS} (Supported Linux/Darwin)")
    sys.exit(1)


HERE = pathlib.Path(__file__).parent.resolve()
# Get the long description from the README file
LONG_DESCRIPTION = (HERE / "README.md").read_text(encoding="utf-8")
AUTHOR = "Workflows and Distributed Computing Group (WDC) - Barcelona Supercomputing Center (BSC)"

# Setup
setup(
    # Metadata
    name="pycompss",
    version="3.0.rc2208",
    description="Python Binding for COMP Superscalar Runtime",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://compss.bsc.es",
    author=AUTHOR,
    author_email="support-compss@bsc.es",
    project_urls={
        "Bug Reports": "https://github.com/bsc-wdc/compss/issues",
        "Source": "https://github.com/bsc-wdc/compss",
    },
    # License
    license="Apache 2.0",
    license_files=["LICENSE.txt"],
    # Other
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Distributed Computing",
    ],
    # Build
    package_dir={"pycompss": "src/pycompss"},
    packages=[""] + find_packages(),
    python_requires=">=3.6",
    install_requires=[],
    package_data={
        "": [
            "log/logging_off.json",
            "log/logging_info.json",
            "log/logging_debug.json",
            "log/logging_worker_debug.json",
            "log/logging_worker_off.json",
            "log/logging_mpi_worker_info.json",
            "log/logging_mpi_worker_debug.json",
            "log/logging_mpi_worker_off.json",
            "log/logging_gat_worker_info.json",
            "log/logging_gat_worker_debug.json",
            "log/logging_gat_worker_off.json",
            "README.md",
            "LICENSE.txt",
        ]
    },
    ext_modules=OS_MODULES,
    # entry_points={"console_scripts": ["pycompss_binding = pycompss.__main__:main"]},
)
