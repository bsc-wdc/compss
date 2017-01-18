#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: fconejer

PyCOMPSs Utils - JVM Configuration Parser
=========================================
    This file contains all methods required to parse the jvm options file.
"""

# import logging
# logger = logging.getLogger(__name__)

def convertToDict(jvm_opt_file):
    """
    JVM parameter file converter to dictionary.
    :param jvm_opt_file: JVM parameters file
    :return: Dictionary with the parameters specified on the file.
    """
    # logger.debug("Parsing JVM options file: %s" % jvm_opt_file)
    opts = {}
    with open(jvm_opt_file) as fp:
        for line in fp:
            line = line.strip()
            if line:
                if(line.startswith("-XX:")):
                    # These parameters have no value
                    key = line.split(":")[1].replace('\n','')
                    opts[key] = True
                else:
                    key = line.split("=")[0]
                    value = line.split("=")[1].replace('\n','')
                    value = value.strip()
                    if not value:
                        value = None
                    opts[key] = value
    return opts