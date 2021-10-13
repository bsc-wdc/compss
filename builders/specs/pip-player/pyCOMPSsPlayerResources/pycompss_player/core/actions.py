"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""
from abc import ABC, abstractmethod
import json
import os
import shutil
from pathlib import Path

class Actions(ABC):

    def __init__(self, arguments, debug=False) -> None:
        super().__init__()
        self.arguments = arguments
        self.debug = debug
    
    @abstractmethod
    def init(self):
        print('ABS INIT')

        home_path = str(Path.home())

        if os.path.isdir(home_path + '/.COMPSs/envs'):
            envs = os.listdir(home_path + '/.COMPSs/envs')

            if self.arguments.name in envs:
                print("ERROR: Ther's already another environment named " + self.arguments.name)
                exit(1)

        env_dir_name = home_path + '/.COMPSs/envs/' + self.arguments.name

        os.makedirs(env_dir_name)
        print("MAKE DIRS", env_dir_name)        

        with open(env_dir_name + '/env.json', 'w') as env_conf:
            json.dump(vars(self.arguments), env_conf)

        if self.arguments.config:
            shutil.copy2(self.arguments.config, env_dir_name)

        print("Environment created successfully, setting as default")

    @abstractmethod
    def exec(self):
        print('ABS EXEC')
        pass

    @abstractmethod
    def run(self):
        print('ABS RUN')
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

    @abstractmethod
    def environment(self):
        print('ABS ENV')
        pass