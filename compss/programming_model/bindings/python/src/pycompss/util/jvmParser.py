"""
@author: fconejer

PyCOMPSs Utils - JVM Configuration Parser
=========================================
    This file contains all methods required to parse the jvm options file.
"""

import logging

logger = logging.getLogger(__name__)

def convertToDict(jvm_opt_file):
    logger.debug("Parsing JVM options file: %s" % jvm_opt_file)
    opts = {}
    with open(jvm_opt_file) as fp:
        for line in fp:
            line = line.strip()
            if line:
                if(line.startswith("-XX:")):
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