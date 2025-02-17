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
PyCOMPSs Worker - Piper - Cache Profiler.

This file contains the cache object profiler functions.
"""

import json
from pycompss.util.typing_helper import typing


def add_profiler_get_put(
    profiler_dict: typing.Dict[
        str, typing.Dict[str, typing.Dict[str, typing.Dict[str, int]]]
    ],
    function: str,
    parameter: str,
    filename: str,
    parameter_type: str,
) -> None:
    """Add get and put entry to the profiler.

    :param profiler_dict: Profiling dictionary.
    :param function: Function to include.
    :param parameter: Function parameter to include.
    :param filename: File name associated to the parameter.
    :param parameter_type: Parameter type.
    :return: None.
    """
    if function not in profiler_dict:
        profiler_dict[function] = {}
    if parameter not in profiler_dict[function]:
        profiler_dict[function][parameter] = {}
    if filename not in profiler_dict[function][parameter]:
        profiler_dict[function][parameter][filename] = {"PUT": 0, "GET": 0}
    profiler_dict[function][parameter][filename][parameter_type] += 1


def add_profiler_get_struct(
    profiler_get_struct: list, function: str, parameter: str, filename: str
) -> None:
    """Add info to profiling struct entry.

    :param profiler_get_struct: Profiling struct.
    :param function: Function to include
    :param parameter: Function parameter to include.
    :param filename: File name associated to the parameter.
    :return: None.
    """
    if (
        function not in profiler_get_struct[2]
        and parameter not in profiler_get_struct[1]
    ):
        profiler_get_struct[0].append(filename)
        profiler_get_struct[1].append(parameter)
        profiler_get_struct[2].append(function)


def profiler_print_message(
    profiler_dict: typing.Dict[
        str, typing.Dict[str, typing.Dict[str, typing.Dict[str, int]]]
    ],
    profiler_get_struct: typing.List[typing.List[str]],
    analysis_dir: str,
) -> None:
    r"""Export profiling information to json.

    for function in profiler_dict:
        f.write('\t' + "FUNCTION: " + str(function))
        logger.debug('\t' + "FUNCTION: " + str(function))
        for parameter in profiler_dict[function]:
            f.write('\t' + '\t' + '\t' + "PARAMETER: " + str(parameter))
            logger.debug('\t' + '\t' + '\t' + "PARAMETER: " + str(parameter))
            for filename in profiler_dict[function][parameter]:
                _put = profiler_dict[function][parameter][filename]['PUT']
                _get = profiler_dict[function][parameter][filename]['GET']
                f.write('\t' + '\t' + '\t' + '\t' + "FILENAME: " + filename +
                        '\t' + " PUT " + str(_put) + " GET " + str(_get))
                logger.debug('\t' + '\t' + '\t' + '\t' + "FILENAME: " +
                             filename + '\t' + " PUT " +
                             str(_put) + " GET " + str(_get))
    f.write("")
    logger.debug("")
    logger.debug("PROFILER GETS")
    for i in range(len(profiler_get_struct[0])):
        logger.debug('\t' + "FILENAME: " + profiler_get_struct[0][i]
                          + ". PARAMETER: " + profiler_get_struct[1][i]
                          + ". FUNCTION: " + profiler_get_struct[2][i])

    :param profiler_dict: Profiling dictionary.
    :param profiler_get_struct: Profiling struct.
    :param analysis_dir: Analysis directory.
    :return: None.
    """
    f_d_type = typing.Dict[  # noqa # pylint: disable=unused-variable
        str,
        typing.Dict[
            str,
            typing.Dict[str, typing.Union[str, int, bool, typing.List[str]]],
        ],
    ]
    final_dict = {}  # type: f_d_type
    for function in profiler_dict:
        final_dict[function] = {}
        for parameter in profiler_dict[function]:
            total_get = 0
            total_put = 0
            is_used = []
            filenames = profiler_dict[function][parameter]
            final_dict[function][parameter] = {}
            for filename in filenames:
                puts = filenames[filename]["PUT"]
                if puts > 0:
                    try:
                        index = profiler_get_struct[0].index(filename)
                        is_used.append(
                            profiler_get_struct[2][index]
                            + "#"
                            + profiler_get_struct[1][index]
                        )
                    except ValueError:
                        pass
                total_put += puts
                total_get += filenames[filename]["GET"]
            final_dict[function][parameter]["GET"] = total_get
            final_dict[function][parameter]["PUT"] = total_put

            if len(is_used) > 0:
                final_dict[function][parameter]["USED"] = is_used
            elif total_get > 0:
                final_dict[function][parameter]["USED"] = [
                    function + "#" + parameter
                ]
            else:
                final_dict[function][parameter]["USED"] = []

    with open(
        analysis_dir + "/cache_profiler.json", "a", encoding="utf-8"
    ) as json_file:
        json.dump(final_dict, json_file)
