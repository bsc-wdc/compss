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
PyCOMPSs Util - Process - Preloader.

This file centralizes the library preloading functions.
It helps to import in parallel all indicated libraries.
"""
import logging
import os
import pkgutil
from concurrent.futures import ThreadPoolExecutor
from pycompss.util.typing_helper import typing


PRELOAD_PYTHON_LIBRARIES_EVNAME = "PRELOAD_PYTHON_LIBRARIES"


def preimports() -> bool:
    """Check if imports have to be preloaded.

    True if the PRELOAD_PYTHON_LIBRARIES_EVNAME environment variable is
    defined.
    :return: If imports have to be preloaded.
    """
    return PRELOAD_PYTHON_LIBRARIES_EVNAME in os.environ


def __default_imports() -> typing.List[str]:
    """Retrieve the default imports list.

    :return: The default libraries to be preimported.
    """
    default_imports = [
        "pickle",
        "dill",
        "numpy",
        "pycompss.api.commons.decorator",
        "pycompss.api.commons.error_msgs",
        "pycompss.api.commons.implementation_types",
        "pycompss.api.commons.private_tasks",
        "pycompss.api.commons.data_type",
        "pycompss.api.commons.constants",
        "pycompss.api.api",
        "pycompss.api.task",
        "pycompss.api.constraint",
        "pycompss.api.implement",
        "pycompss.api.mpi",
        "pycompss.api.multinode",
        "pycompss.api.on_failure",
        "pycompss.api.parameter",
        "pycompss.api.prolog",
        "pycompss.api.epilog",
        "pycompss.api.reduction",
        "pycompss.runtime.management.classes",
        "pycompss.runtime.management.COMPSs",
        "pycompss.runtime.management.direction",
        "pycompss.runtime.management.object_tracker",
        "pycompss.runtime.management.synchronization",
        "pycompss.runtime.management.link.direct",
        "pycompss.runtime.management.link.messages",
        "pycompss.runtime.management.link.separate",
        "pycompss.runtime.mpi.keys",
        "pycompss.runtime.mpi",
        "pycompss.runtime.start.initialization",
        "pycompss.runtime.start",
        "pycompss.runtime.task.arguments",
        "pycompss.runtime.task.commons",
        "pycompss.runtime.task.features",
        "pycompss.runtime.task.keys",
        "pycompss.runtime.task.master",
        "pycompss.runtime.task.parameter",
        "pycompss.runtime.task.shared_args",
        "pycompss.runtime.task.worker",
        "pycompss.runtime.task.definitions.arguments",
        "pycompss.runtime.task.definitions.constraints",
        "pycompss.runtime.task.definitions.core_element",
        "pycompss.runtime.task.definitions.function",
        "pycompss.runtime.task.definitions",
        "pycompss.runtime.task.wrappers.psco_stream",
        "pycompss.runtime.task.wrappers",
        "pycompss.runtime.task",
        "pycompss.runtime.binding",
        "pycompss.runtime.commons",
        "pycompss.runtime",
        "pycompss.util.environment.configuration",
        "pycompss.util.jvm.parser",
        "pycompss.util.logger.helpers",
        "pycompss.util.logger.level",
        "pycompss.util.logger.remittent",
        "pycompss.util.objects.properties",
        # "pycompss.util.objects.replace",
        "pycompss.util.objects.sizer",
        "pycompss.util.objects.util",
        "pycompss.util.process.manager",
        "pycompss.util.serialization.extended_support",
        "pycompss.util.serialization.serializer",
        "pycompss.util.std.redirects",
        "pycompss.util.storages.persistent",
        "pycompss.util.supercomputer.scs",
        "pycompss.util.tracing.types_events_master",
        "pycompss.util.tracing.types_events_worker",
        "pycompss.util.tracing.helpers",
        "pycompss.util.warning.modules",
        "pycompss.util.arguments",
        "pycompss.util.context",
        "pycompss.util.exceptions",
        "pycompss.util.location",
        "pycompss.util.typing_helper",
        "pycompss.worker.commons.executor",
        "pycompss.worker.commons.worker",
        "pycompss.worker.piper.cache.classes",
        "pycompss.worker.piper.cache.manager",
        "pycompss.worker.piper.cache.profiler",
        "pycompss.worker.piper.cache.setup",
        "pycompss.worker.piper.cache.tracker",
        "pycompss.worker.piper.commons.constants",
        "pycompss.worker.piper.commons.executor",
        "pycompss.worker.piper.commons.utils",
        "pycompss.worker.piper.commons.utils_logger",
        "pycompss.worker.piper.piper_worker",
        "pycompss",
    ]
    return default_imports


def __load_import(library: str) -> None:
    """Import the given library as string.

    :param library: Library name to be imported.
    :return: None
    """
    try:
        __import__(library)
    except Exception as e:
        if __debug__:
            print(f"WARNING: Pre-load import {library} failed: {e}")


def preload_imports(
    logger: logging.Logger, header: str, subheader: str
) -> None:
    """Resolve imports provided by an environment variable.

    This resolving is performed in parallel using threading, so that the
    imports are done in the main worker process and inherited by the
    executor processes to avoid that each of them has to do them
    sequentially.

    :param logger: Logger.
    :param header: Header to be shown in the logger messages.
    :param subheader: Subheader to be shown in the logger messages.
    :return: None
    """
    imports = os.environ[PRELOAD_PYTHON_LIBRARIES_EVNAME]
    show_memory = False
    if __debug__:
        try:
            import psutil

            process = psutil.Process(os.getpid())
            used_memory = process.memory_info().rss / (1024 * 1024)
            logger.debug(
                "%s%s - Memory used before imports: %s",
                header,
                subheader,
                str(used_memory),
            )
            show_memory = True
        except ImportError:
            logger.debug(
                "%s%s - Could not calculate used memory."
                "Install psutil if you want to check it.",
                header,
                subheader,
                str(),
            )
    # Get the library names
    to_be_imported = __default_imports()
    if imports == "ALL":
        # If the variable contains ALL, will import all possible packages
        # Read all installed packages
        for module_info in pkgutil.iter_modules():
            name = module_info.name
            if isinstance(name, str):
                if (
                    not name.startswith("lib")
                    and "mpi" not in name
                    and name not in ["setup", "__init__"]
                ):
                    to_be_imported.append(name.strip())
    elif ";" in imports:
        # If the variable specifies explicitly a semicolon separated list of
        # packages to be pre imported.
        for name in imports.split(";"):
            if name:
                to_be_imported.append(name.strip())
    else:
        # Read the whole file
        with open(imports) as f:
            lines = f.readlines()
        # Filter comments and any line that does not contain the import word
        full_imports = []
        for line in lines:
            if "import" in line and not line.startswith("#"):
                full_imports.append(line)
        for i in full_imports:
            lib = i.split(" ")[1].strip()
            to_be_imported.append(lib)
    if __debug__:
        logger.debug(
            "%s%s - Libraries pre-imported: %s",
            header,
            subheader,
            str(len(to_be_imported)),
        )
        if show_memory:
            used_memory_after = process.memory_info().rss / (1024 * 1024)
            logger.debug(
                "%s%s - Memory used after imports: %s",
                header,
                subheader,
                str(used_memory_after),
            )
            amount = used_memory_after - used_memory
            logger.debug(
                "%s%s - Memory increase: %s", header, subheader, str(amount)
            )
    # Import the libraries using the max amount of cores
    pool = ThreadPoolExecutor()
    pool.map(__load_import, to_be_imported)
    pool.shutdown(wait=True)
