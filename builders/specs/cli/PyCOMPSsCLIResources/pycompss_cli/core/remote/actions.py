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

from pycompss_cli.core.remote.cmd import remote_app_deploy, remote_app_remove, remote_deploy_compss
from pycompss_cli.core.remote.cmd import remote_submit_job
from pycompss_cli.core.remote.cmd import remote_list_job
from pycompss_cli.core.remote.cmd import remote_cancel_job
from pycompss_cli.core.remote.cmd import remote_run_app
from pycompss_cli.core.remote.cmd import remote_exec_app
from pycompss_cli.core.remote.cmd import remote_list_apps
from pycompss_cli.core.remote.cmd import remote_get_home
from pycompss_cli.core.remote.cmd import remote_env_remove
from pycompss_cli.core.remote.cmd import remote_download_file
from pycompss_cli.core.actions import Actions
from pycompss_cli.core import utils
from pycompss_cli.core.remote.interactive_sc import core, defaults
import os, json, time, datetime, traceback, re
from collections import defaultdict

class RemoteActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

        self.apps = None
        self.past_jobs = defaultdict(dict)
        if self.env_conf and os.path.isfile(self.env_conf['env_path'] + '/jobs.json'):
            with open(self.env_conf['env_path'] + '/jobs.json') as f:
                self.past_jobs = defaultdict(dict, json.load(f))

    def get_apps(self, env_id=None):
        if self.apps is not None:
            return self.apps

        if env_id is not None:
            if 'remote_home' not in self.env_conf:
                return []
            return remote_list_apps(env_id, self.env_conf['login'], self.env_conf['remote_home'])
        
        self.apps = remote_list_apps(self.env_conf['name'], self.env_conf['login'], self.env_conf['remote_home'])
        return self.apps

    def init(self):
        """ Deploys COMPSs infrastructure on remote environment

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        super().init()

        if self.arguments.modules is None:
            self.arguments.modules = ['COMPSs']
        elif len(self.arguments.modules) == 1 and os.path.isfile(self.arguments.modules[0]):
            self.arguments.modules = self.arguments.modules[0]
        
        print('Deploying environment...')

        env_id = self.arguments.name
        envars = []
        if re.search(r'mn\d\.bsc\.es', self.env_conf['login']) is not None:
            envars.append('COMPSS_PYTHON_VERSION=3.7.4')
        
        try:
            remote_deploy_compss(env_id, self.arguments.login, self.arguments.modules, envars=envars)

            remote_home_path = remote_get_home(self.arguments.login)

            self.env_add_conf({'remote_home': remote_home_path})
        except:
            traceback.print_exc()
            
            print("ERROR: Remote({}) deployment failed".format(env_id))
            self.env_remove(env_id)

    def run(self):
        app_name = self.arguments.app_name
        if not app_name:
            print(f"ERROR: Application ID argument (-app) is required for executing runcompss in remote")
            exit(1)

        if app_name not in self.get_apps():
            print(f"ERROR: Application `{app_name}` not found")
            exit(1)

        app_args = self.arguments.rest_args
        command = "runcompss " + ' '.join(app_args)
        login_info = self.env_conf['login']
        env_id = self.env_conf['name']
        remote_dir = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'
        env_name = self.env_conf['name']
        modules = self.__get_modules()

        print(remote_run_app(remote_dir, login_info, env_name, command, modules))

    def __get_modules(self):
        with open(self.env_conf['env_path'] + '/modules.sh', 'r') as mod_file:
            mods = mod_file.read().strip().split('\n')
            mods = [re.sub(r'#.*', '', mod).strip() for mod in mods]
            return mods

    def job(self):
        super().job()

        action_name = self.arguments.job
        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        super().job_submit()

        app_name = self.arguments.app_name
        
        if not app_name:
            print(f"ERROR: Application ID argument (-app) is required for executing runcompss in remote")
            exit(1)

        if app_name not in self.get_apps():
            print(f"ERROR: Application {app_name} not found")
            exit(1)

        login_info = self.env_conf['login']
        env_id = self.env_conf['name']
        remote_dir = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'

        if len(self.arguments.rest_args) == 1 and os.path.isfile(self.arguments.rest_args[0]):
            with open(self.arguments.rest_args[0], 'r') as args_file:
                app_args = args_file.read().strip().replace('\n', ' ')
        else:
            app_args = " ".join(self.arguments.rest_args)

        app_args = app_args.replace('{COMPS_APP_PATH}', remote_dir)

        app_args = f'--pythonpath={remote_dir} ' + app_args
        app_args = f'--classpath={remote_dir} ' + app_args
        app_args = f'--appdir={remote_dir} ' + app_args

        modules = self.__get_modules()
        env_vars = [item for sublist in self.arguments.env_var for item in sublist]
        if 'COMPSS_PYTHON_VERSION' not in ''.join(env_vars):
            env_vars = ['COMPSS_PYTHON_VERSION=3.7.4'] + env_vars
        
        job_id = remote_submit_job(login_info, remote_dir, app_args, modules, envars=env_vars, debug=self.debug)

        self.past_jobs[job_id] = {
            'app_name': app_name,
            'env_vars': '; '.join(env_vars) if env_vars else 'None',
            'enqueue_args': app_args,
            'timestamp': str(datetime.datetime.utcnow())[:-7] + ' UTC'
        }

        with open(self.env_conf['env_path'] + '/jobs.json', 'w') as f:
            json.dump(self.past_jobs, f)

    def job_history(self):
        job_id = self.arguments.job_id
        if job_id:
            app_jobs = self.past_jobs[job_id]
            print('\tApp name:', app_jobs['app_name'])
            print('\tSubmit time:', app_jobs['timestamp'])
            print('\tEnvironment Variables:', app_jobs['env_vars'])
            print('\tEnqueue Args:', app_jobs['enqueue_args'])
            print()
        else:
            col_names = ['JobIDs', 'AppName', 'Status']
            rows = []
            for job_id, app_job in self.past_jobs.items():
                if 'status' not in app_job:
                    job_status = self.job_status(job_id)
                else:
                    job_status = app_job['status']
                    if 'COMPLETED' not in job_status:
                        job_status = self.job_status(job_id)
                if job_status is None:
                    job_status = 'Unknown'
                rows.append([job_id, app_job['app_name'], f'\t{job_status}'])
            utils.table_print(col_names, rows)

    def job_list(self):
        login_info = self.env_conf['login']
        modules = self.__get_modules()
        remote_list_job(login_info, modules)

    def job_cancel(self):
        login_info = self.env_conf['login']
        jobid = self.arguments.job_id
        modules = self.__get_modules()
        remote_cancel_job(login_info, jobid, modules)

    def job_status(self, jid=None):
        login_info = self.env_conf['login']
        job_id = self.arguments.job_id if jid is None else jid
        modules = self.__get_modules()
        scripts_path = self.env_conf['remote_home'] + '/.COMPSs/job_scripts'
        job_status = core.job_status(scripts_path, job_id, login_info, modules, debug=self.debug)
        if job_status == 'ERROR' and job_id in self.past_jobs:
            app_name = self.past_jobs[job_id]['app_name']
            env_id = self.env_conf['name']
            app_path = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'
            cmd = f'grep -iF "error" {app_path}/compss-{job_id}.err'
            status = 'ERROR' if remote_exec_app(login_info, cmd) else 'SUCCESS'
            job_status = 'COMPLETED:' + status
        
        if jid is None:
            print(job_status)

        if job_id in self.past_jobs:
            self.past_jobs[job_id]['status'] = job_status
            with open(self.env_conf['env_path'] + '/jobs.json', 'w') as f:
                json.dump(self.past_jobs, f)

        return job_status

    def app(self):
        if not self.arguments.app:
            self.arguments.func()
            exit(1)

        action_name = self.arguments.app
        action_name = utils.get_object_method_by_name(self, 'app_' + action_name, include_in_name=True)
        getattr(self, action_name)()

    def app_deploy(self):
        if self.arguments.source_dir == 'current directory':
            self.arguments.source_dir = os.getcwd()
        else:
            if not os.path.isdir(self.arguments.source_dir):
                print(f"ERROR: Local source directory {self.arguments.source_dir} does not exist")
                exit(1)

        app_name = self.arguments.app_name
        if app_name in self.get_apps() and not self.arguments.overwrite:
            print(f'ERROR: There is already another application named `{app_name}`. Use `--overwrite` to overwrite it.')
            exit(1)

        env_id = self.env_conf['name']
        app_dir = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'

        remote_app_deploy(app_dir, self.env_conf['login'], self.arguments.source_dir,
                          self.arguments.destination_dir, self.arguments.env_file)


    def app_remove(self):
        app_names = self.arguments.app_name
        for app_name in app_names:
            if app_name in self.get_apps():
                login_info = self.env_conf['login']
                env_id = self.env_conf['name']
                app_dir = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'
                remote_app_remove(login_info, app_dir)
                print(f'Application `{app_name}` removed successfully')
            else:
                print(f'ERROR: Application `{app_name}` not found')
                exit(1)

    def app_list(self):
        apps = self.get_apps()

        if not apps:
            print('INFO: There are no applications binded to this environment yet')
            print('       Try deploying an application first with `pycompss app deploy`')
            exit(1)

        utils.table_print(['Name'], [[a] for a in apps])

    def exec(self):
        login_info = self.env_conf['login']
        command = ' '.join(self.arguments.exec_cmd)
        print(remote_exec_app(login_info, command))

    def env_remove(self, eid=None):
        env_id = self.arguments.env_id if eid is None else eid
        env_apps = self.get_apps(env_id=env_id)
        if len(env_apps) > 0:
            print('WARNING: There are still applications binded to this environment')
            answer = 'y'
            if not self.arguments.force:
                answer = input('Do you want to delete this environment and all the applications? (y/N) ')
            if answer.lower() == 'y' or answer == 'yes':
                login_info = self.env_conf['login']
                remote_env_remove(login_info, env_id, env_apps)
                super().env_remove(eid=eid)
        else:
            super().env_remove(eid=eid)

    def components(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def gengraph(self):
        dot_path = self.arguments.dot_file
        
        command = "compss_gengraph " + dot_path
        remote_exec_app(self.env_conf['login'], command)

    def gentrace(self):
        modules = self.__get_modules()

        trace_ls = f"ls {self.arguments.trace_dir}"
        trace_ls = remote_exec_app(self.env_conf['login'], trace_ls, self.debug)
        if not '.prv.gz' in trace_ls:
            gentrace_cmd = f"compss_gentrace {self.arguments.trace_dir} "
            gentrace_cmd += ' '.join(self.arguments.rest_args)

            commands = [
                *modules,
                gentrace_cmd
            ]

            command = ';'.join(commands)

            print('Generating trace...')
            remote_exec_app(self.env_conf['login'], command, self.debug)
        else:
            print('Trace already exists')

        if self.arguments.download_dir:
            compress_cmd = f"cd {self.arguments.trace_dir};gzip trace.prv"
            remote_exec_app(self.env_conf['login'], compress_cmd, self.debug)
            
            print('Downloading trace...')
            remote_download_file(self.env_conf['login'], self.arguments.trace_dir, self.arguments.download_dir, self.debug)

        


    def monitor(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def jupyter(self):
        app_name = self.arguments.app_name
        if not app_name:
            print(f"ERROR: Application ID argument (-app) is required for starting jupyter on remote environment")
            exit(1)

        if app_name not in self.get_apps():
            print(f"ERROR: Application `{app_name}` not found")
            exit(1)
    

        login_info = self.env_conf['login']
        env_id = self.env_conf['name']
        remote_dir = self.env_conf['remote_home'] + f'/.COMPSsApps/{env_id}/{app_name}'
        job_name = app_name + '-PyCOMPSsInteractive'
        app_args = ['--jupyter_notebook='+remote_dir, 
                    '--job_name='+job_name, 
                    '--lang=python',
                    f'--master_working_dir={remote_dir}',
                    f'--pythonpath={remote_dir}']
        app_args += self.arguments.rest_args

        port = '8888'
        for arg in app_args:
            if '--port=' in arg:
                port = arg.split('=')[1]
                app_args.remove(arg)
                break

        app_args = ' '.join(app_args)
        modules = self.__get_modules()
        envars = []
        if re.search(r'mn\d\.bsc\.es', self.env_conf['login']) is not None:
            envars.append('COMPSS_PYTHON_VERSION=3.7.4')

        job_id = remote_submit_job(login_info, remote_dir, app_args, modules, envars, debug=self.debug)

        scripts_path = self.env_conf['remote_home'] + '/.COMPSs/job_scripts'

        print('Waiting for jupyter to start...')
        jupyter_job_status = defaults.NOT_RUNNING_KEYWORD
        try:
            while jupyter_job_status != 'RUNNING':
                jupyter_job_status = core.job_status(scripts_path, job_id, login_info, modules, debug=self.debug)
        except:
            print('ERROR while waiting for jupyter to start')
            core.cancel_job(scripts_path, [job_id], login_info, modules, debug=self.debug)
            exit(1)
        else:
            print('Jupyter started')

            print('Connecting to jupyter server...')
            time.sleep(5)

            core.connect_job(scripts_path, job_id, login_info, modules, remote_dir, port_forward=port, web_browser=None, debug=self.debug)
            
            core.cancel_job(scripts_path, [job_id], login_info, modules, debug=self.debug)
