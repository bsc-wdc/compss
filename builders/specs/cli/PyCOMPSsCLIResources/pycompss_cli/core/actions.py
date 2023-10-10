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
"""
This file contains the actions supported by pycompss-cli.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""
from abc import ABC, abstractmethod
import json
import os
import shutil
from pathlib import Path
from glob import glob
from pycompss_cli.core import utils
from uuid import uuid4 as uuid

class Actions(ABC):

    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__()
        self.arguments = arguments
        self.debug = debug
        self.env_conf = env_conf
        self.home_path = str(Path.home())
        
        if self.env_conf:
            self.env_conf['env_path'] = self.home_path + '/.COMPSs/envs/' + self.env_conf['name']

        if 'name' in self.arguments and self.arguments.name == 'unique uuid':
            self.arguments.name = ''.join(str(uuid()).split('-')[:2])

    
    def env_add_conf(self, extra_conf):
        current_env, env_conf_path = utils.get_current_env_conf(env_id=self.arguments.name, return_path=True)
        new_conf =  {**current_env, **extra_conf}
        with open(env_conf_path, 'w') as f:
            json.dump(new_conf, f)


    @abstractmethod
    def init(self):
        if not self.arguments.env:
            self.arguments.func()
            exit(0)

        del self.arguments.func

        if os.path.isdir(self.home_path + '/.COMPSs/envs'):
            envs = os.listdir(self.home_path + '/.COMPSs/envs')

            if self.arguments.name in envs:
                print("ERROR: There's already another environment named " + self.arguments.name)
                exit(1)

        env_path = self.home_path + '/.COMPSs/envs/' + self.arguments.name
        self.arguments.env_path = env_path

        os.makedirs(env_path)

        with open(env_path + '/env.json', 'w') as env_conf:
            json.dump(vars(self.arguments), env_conf)
        self.env_conf = vars(self.arguments)

        if self.arguments.config:
            shutil.copy2(self.arguments.config, env_path)
            
        print('Environment created ID:', self.arguments.name)

    @abstractmethod
    def exec(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def job(self):
        if not self.arguments.job:
            self.arguments.func()
            exit(0)

    def job_submit(self):
        if not self.arguments.rest_args:
            print(self.arguments.enqueue_args)
            exit(0)

    @abstractmethod
    def app(self):
        raise NotImplementedError("Wrong Environment! Try switching to a `remote` environment")

    @abstractmethod
    def jupyter(self):
        pass

    @abstractmethod
    def monitor(self):
        pass

    @abstractmethod
    def gengraph(self):
        pass

    @abstractmethod
    def gentrace(self):
        pass

    @abstractmethod
    def components(self):
        pass
    
    def environment(self):
        action_name = 'list'

        if self.arguments.environment:
            action_name = self.arguments.environment

        action_name = utils.get_object_method_by_name(self, 'env_' + action_name, include_in_name=True)
        getattr(self, action_name)()

    def env_list(self):
        envs_path = self.home_path + '/.COMPSs/envs'
        envs_files = os.listdir(envs_path)

        env_info = []
        for env_name in envs_files:
            env_type = json.load(open(envs_path + f'/{env_name}/env.json'))['env']
            env_current = '*' if 'current' in os.listdir(envs_path + f'/{env_name}') else ''
            env_info.append([env_name, env_type, env_current])
        
        col_names = ['ID', 'Type', 'Active']
        utils.table_print(col_names, env_info)


    def env_change(self, env_id=None):
        env_id = self.arguments.env_id if env_id is None else env_id
        if not os.path.isdir(self.home_path + '/.COMPSs/envs/' + env_id):
            print("ERROR: There's no environment named " + env_id)
            exit(1)

        current_files = glob(self.home_path + '/.COMPSs/envs/*/current')
        if current_files:
            os.remove(current_files[0])
        env_dir_name = self.home_path + '/.COMPSs/envs/' + env_id
        open(env_dir_name + '/current', 'a').close()
        print(f'Environment `{env_id}` is now active')

    @abstractmethod
    def env_remove(self, eid=None):
        self.env_change(env_id='default')
        env_id = self.arguments.env_id if eid is None else eid

        env_dir_name = self.home_path + '/.COMPSs/envs/' + env_id
        
        print(f'Deleting environment `{env_id}`...')
        shutil.rmtree(env_dir_name)
