"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""
from abc import ABC, abstractmethod
import json
import os
import shutil
from pathlib import Path
from glob import glob
from pycompss_player.core import utils

class Actions(ABC):

    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__()
        self.env_conf = env_conf
        self.arguments = arguments
        self.debug = debug
        self.home_path = str(Path.home())
        
    
    def add_env_config(self, extra_conf):
        current_env, env_conf_path = utils.get_current_env(return_path=True)
        new_conf =  {**current_env, **extra_conf}
        with open(env_conf_path, 'w') as f:
            json.dump(new_conf, f)


    @abstractmethod
    def init(self):
        # print('ABS INIT')

        if os.path.isdir(self.home_path + '/.COMPSs/envs'):
            envs = os.listdir(self.home_path + '/.COMPSs/envs')

            if self.arguments.name in envs:
                print("ERROR: There's already another environment named " + self.arguments.name)
                exit(1)

        env_dir_name = self.home_path + '/.COMPSs/envs/' + self.arguments.name

        os.makedirs(env_dir_name)
        # print("MAKE DIRS", env_dir_name)

        with open(env_dir_name + '/env.json', 'w') as env_conf:
            json.dump(vars(self.arguments), env_conf)

        if self.arguments.config:
            shutil.copy2(self.arguments.config, env_dir_name)

        for current_file in glob(self.home_path + '/.COMPSs/envs/*/current'):
            os.remove(current_file)

        open(env_dir_name + '/current', 'a').close()

        # print(self.arguments.name)

    @abstractmethod
    def exec(self):
        pass

    @abstractmethod
    def run(self):
        pass

    # @abstractmethod
    # def job(self):
    #     pass

    # @abstractmethod
    # def jupyter(self):
    #     pass

    # @abstractmethod
    # def monitor(self):
    #     pass

    # @abstractmethod
    # def gengraph(self):
    #     pass

    # @abstractmethod
    # def component(self):
    #     pass

    def environment(self):
        # print('ABS ENV')
        action_name = 'list'

        if self.arguments.environment:
            action_name = self.arguments.environment

        action_name = utils.get_object_method_by_name(self, '__env_' + action_name, include_in_name=True)
        getattr(self, action_name)()

    def __env_list(self):
        env_dir_tree = list(os.walk(self.home_path + '/.COMPSs/envs'))
        envs_names = env_dir_tree[0][1]
        env_dir_tree = env_dir_tree[1:]

        env_info = []
        for i in range(len(env_dir_tree)):
            env_type = json.load(open(env_dir_tree[i][0] + '/env.json'))['env']
            env_current = '*' if 'current' in env_dir_tree[i][2] else ''
            env_info.append([envs_names[i], env_type, env_current])
        
        col_names = ['ID', 'Type', 'Active']
        utils.table_print(col_names, env_info)


    def __env_change(self):
        if not os.path.isdir(self.home_path + '/.COMPSs/envs/' + self.arguments.env_id):
            print("ERROR: There's no environment named " + self.arguments.env_id)
            exit(1)

        current_file = glob(self.home_path + '/.COMPSs/envs/*/current')[0]
        os.remove(current_file)
        env_dir_name = self.home_path + '/.COMPSs/envs/' + self.arguments.env_id
        open(env_dir_name + '/current', 'a').close()
        print('Environment', self.arguments.env_id, 'is now active')

    def __env_remove(self):
        if not os.path.isdir(self.home_path + '/.COMPSs/envs/' + self.arguments.env_id):
            print("ERROR: There's no environment named " + self.arguments.env_id)
            exit(1)

        env_dir_name = self.home_path + '/.COMPSs/envs/' + self.arguments.env_id
        shutil.rmtree(env_dir_name)

        other_envs = list(os.walk(self.home_path + '/.COMPSs/envs'))
        if len(other_envs) > 1:
            open(other_envs[1][0] + '/current', 'a').close()