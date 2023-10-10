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
from pycompss_cli.core.local.actions import LocalActions
from pycompss_cli.core.docker.actions import DockerActions
from pycompss_cli.core.remote.actions import RemoteActions
from pycompss_cli.core.unicore.actions import UnicoreActions
from pycompss_cli.core import utils
from copy import deepcopy
import os
from pathlib import Path
import json

class ActionsDispatcher(object):
    def __init__(self) -> None:
        super().__init__()
        self.home_path = str(Path.home())

    def run_action(self, arguments):
        self.__ensure_default_env()

        if 'env' in arguments and arguments.env:
            env_type = arguments.env
            env_conf = None
        else:
            envs_path = self.home_path + '/.COMPSs/envs'
            if not os.path.isdir(envs_path) or len(list(os.walk(envs_path))) == 1:
                print("There are no environments created. Try using `pycompss init`")
                exit(1)

            if arguments.action == 'environment':
                if arguments.environment and arguments.environment.startswith('r'):
                    self.__delete_envs(arguments.env_id, arguments)

            env_id = arguments.env_id if arguments.env_id else None
                
            env_conf = utils.get_current_env_conf(env_id=env_id)
            env_type = env_conf['env']

        self.__actions_cmd = self.__getactions_cmd(env_type, arguments, env_conf)

        action_name = utils.get_object_method_by_name(self.__actions_cmd, arguments.action)
        action_func = getattr(self.__actions_cmd, action_name)
        action_func()

    def __delete_envs(self, envs_ids, arguments):
        for env_id in envs_ids:
            env_type = self.__get_env_type_from_name(env_id)
            if env_type is None:
                print("ERROR: There's no environment named " + env_id)
                continue
            if env_id == 'default':
                print('ERROR: `default` environment is required and cannot be deleted')
                continue
            env_conf = utils.get_env_conf_by_name(env_id)
            env_arguments = deepcopy(arguments)
            env_arguments.env_id = env_id
            action_cmd = self.__getactions_cmd(env_type, env_arguments, env_conf=env_conf)
            action_cmd.env_remove()
        exit(0)

    def __get_env_type_from_name(self, env_name):
        envs_path = self.home_path + '/.COMPSs/envs'
        env_dir_tree = list(os.walk(envs_path))
        envs_names = env_dir_tree[0][1]
        env_dir_tree = env_dir_tree[1:]
        for i in range(len(env_dir_tree)):
            if env_name == envs_names[i]:
                return json.load(open(env_dir_tree[i][0] + '/env.json'))['env']
        return None

    def __getactions_cmd(self, env_type, arguments, env_conf=None):
        debug = arguments.debug

        if env_type == "local":
            return LocalActions(arguments, debug, env_conf)
        elif env_type == "docker":
            return DockerActions(arguments, debug, env_conf)
        elif env_type == "remote":
            return RemoteActions(arguments, debug, env_conf)
        elif env_type == "unicore":
            return UnicoreActions(arguments, debug, env_conf)
        else:
            raise NotImplementedError(f"Environment `{env_type}` not implemented")

    
    def __ensure_default_env(self):
        default_env = self.home_path + '/.COMPSs/envs/default'
        if not os.path.isdir(default_env):
            os.makedirs(default_env)
            open(default_env + '/current', 'a').close()
            with open(default_env + '/env.json', 'w') as def_env:
                json.dump({ 'env': 'local', 'name': 'default' }, def_env)
            with open(default_env + '/modules.sh', 'w') as mod_file:
                mod_file.write('module load COMPSS')
