#
#  Copyright 2018 Barcelona Supercomputing Center (www.bsc.es)
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
Partitions should be loaded with data in the last step only. Thus, generator
objects are necessary. Inside 'task' functions we will call their 'generate'
method in order to retrieve the partition. These partitions can previously be
loaded on master and sent to workers, or read from files on worker nodes.
"""


class BaseDataGenerator(object):
    """
    Everyone implements this.
    """
    def generate_data(self):
        raise NotImplementedError

