#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

class ExecutionCharacteristics(object):

    def __init__(self,
                 generation_nodes,
                 generation_cpus_per_node,
                 run_nodes,
                 run_cpus_per_node,
                 amount_executions):
        self.generation_nodes = generation_nodes
        self.generation_cpus_per_node = generation_cpus_per_node
        self.run_nodes = run_nodes
        self.run_cpus_per_node = run_cpus_per_node
        self.amount_executions = amount_executions
