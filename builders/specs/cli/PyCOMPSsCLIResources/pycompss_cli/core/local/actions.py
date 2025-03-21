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
"""
This file contains the actions supported by pycompss-cli.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from collections import defaultdict
import datetime
import json
import shutil
import traceback
from typing import List
from pycompss_cli.models.app import App
from pycompss_cli.core.local.cmd import local_deploy_compss
from pycompss_cli.core.local.cmd import local_run_app
from pycompss_cli.core.local.cmd import local_exec_app
from pycompss_cli.core.local.cmd import local_jupyter
from pycompss_cli.core.local.cmd import local_submit_job
from pycompss_cli.core.local.cmd import local_job_list
from pycompss_cli.core.local.cmd import local_cancel_job
from pycompss_cli.core.local.cmd import local_job_status
from pycompss_cli.core.local.cmd import local_app_deploy
from pycompss_cli.core.local.cmd import local_inspect
from pycompss_cli.core.actions import Actions
import pycompss_cli.core.utils as utils
import os, sys


class LocalActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)
        path = os.path.abspath(__file__)
        self.local_job_scripts_dir = os.path.dirname(path) + '/../remote/job_scripts'

        self.apps = None

        self.past_jobs = defaultdict(list)
        if self.env_conf and os.path.isfile(self.env_conf['env_path'] + '/jobs.json'):
            with open(self.env_conf['env_path'] + '/jobs.json') as f:
                self.past_jobs = defaultdict(list, json.load(f))

    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure local env

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """

   
        if self.arguments.working_dir == 'current directory':
            self.arguments.working_dir = os.getcwd()

        try:
            local_deploy_compss(self.arguments.working_dir)
        except:
            traceback.print_exc()
            print("ERROR: Local deployment failed")
            self.env_remove(self.arguments.name)


    def run(self):
        super().run()
        """ Run the given command in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        app_args = self.arguments.rest_args

        commands = [
            "runcompss " + ' '.join(app_args)
        ]

        if 'working_dir' in self.env_conf:
            commands.insert(0, 'cd ' + self.env_conf['working_dir'])

        if self.debug:
            print("Running...")
            print("\t- Local command: ", ' '.join(commands))
                
        local_run_app(commands)

    def jupyter(self):
        if utils.check_exit_code('jupyter') == 127:
            print('ERROR: Jupyter not found!')
            exit(1)

        working_dir = '.'
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']

        lab_or_notebook = 'lab' if self.arguments.lab else 'notebook'

        local_jupyter(working_dir, lab_or_notebook, ' '.join(self.arguments.rest_args))

    def exec(self):
        command = ' '.join(self.arguments.exec_cmd)
        local_exec_app(command)

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
        for app in self.get_apps():
            if app.name == app_name:
                print(f'ERROR: There is already another application named `{app_name}`')
                exit(1)

        env_id = self.env_conf['name']
        app_dir = os.path.expanduser('~') + f'/.COMPSsApps/{env_id}/{app_name}'
        os.makedirs(app_dir, exist_ok=True)

        local_app_deploy(self.arguments.source_dir, app_dir, self.arguments.destination_dir)


    def app_remove(self):
        app_names = self.arguments.app_name
        for app_name in app_names:
            apps_name = list(filter(lambda a: a.name == app_name, self.get_apps()))
            if len(apps_name) > 0:
                app: App = apps_name[0]
                env_id = self.env_conf['name']
                app_dir = os.path.expanduser('~') + f'/.COMPSsApps/{env_id}/{app_name}'
                shutil.rmtree(app_dir)
                if app.remote_dir:
                    shutil.rmtree(app.remote_dir)
                print(f'Application `{app_name}` removed successfully')
            else:
                print(f'ERROR: Application `{app_name}` not found')
                exit(1)

    def app_list(self):
        apps = self.get_apps()

        if not apps:
            print('INFO: There are no applications binded to this environment yet')
            print('       Try deploying an application with `pycompss app deploy`')
            exit(1)

        app_data = []
        env_id = self.env_conf['name']
        for app in apps:
            app_dir = os.path.expanduser('~') + f'/.COMPSsApps/{env_id}/{app.name}'
            dest_dir = app.remote_dir if app.remote_dir else app_dir
            app_data.append([ app.name, dest_dir ])

        utils.table_print(['Name', 'Location'], app_data)

    def env_remove(self, env_id=None):
        super().env_remove(eid=env_id)

    def job(self):
        super().job()

        action_name = self.arguments.job
        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        super().job_submit()

        app_name = self.arguments.app_name

        working_dir = os.getcwd()
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']

        for arg in reversed(self.arguments.rest_args):
            if os.path.isabs(arg):
                file_path = arg
            else:
                file_path = working_dir + '/' + arg
            if os.path.isfile(file_path):
                app_name = arg
                break

        app_args = ' '.join(self.arguments.rest_args)
        env_vars = [item for sublist in self.arguments.env_var for item in sublist]
        app_args = f'--pythonpath={working_dir} ' + app_args
        app_args = f'--classpath={working_dir} ' + app_args
        app_args = f'--appdir={working_dir} ' + app_args

        if self.arguments.verbose:
            print('\t-\tenvars:', env_vars)
            print('\t-\tenqueue_compss:', app_args)

        job_id = local_submit_job(app_args, env_vars)

        self.past_jobs[job_id] = {
                'app_name': app_name if app_name else 'unknown',
                'env_vars': '; '.join(env_vars) if env_vars else 'Empty',
                'enqueue_args': app_args,
                'timestamp': str(datetime.datetime.utcnow())[:-7] + ' UTC',
                'path': working_dir
            }

        with open(self.env_conf['env_path'] + '/jobs.json', 'w') as f:
            json.dump(self.past_jobs, f)

    def job_history(self):
        job_id = self.arguments.job_id
        if job_id:
            app_jobs = self.past_jobs[job_id]
            print('\tApp name:', app_jobs['app_name'])
            print('\tApp path:', app_jobs['path'])
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
        list_jobs = local_job_list(self.local_job_scripts_dir)
        print(list_jobs)
        return list_jobs

    def job_status(self, jid=None):
        job_id = self.arguments.job_id if jid is None else jid
        job_status = local_job_status(self.local_job_scripts_dir, job_id)
        if job_status == 'ERROR' and job_id in self.past_jobs:
            app_path = self.past_jobs[job_id]['path']
            cmd = f'grep -iF "error" {app_path}/compss-{job_id}.err'
            status = 'ERROR' if local_exec_app(cmd) else 'SUCCESS'
            job_status = 'COMPLETED:' + status

        if jid is None:
            print(job_status)

        self.past_jobs[job_id]['status'] = job_status
        with open(self.env_conf['env_path'] + '/jobs.json', 'w') as f:
            json.dump(self.past_jobs, f)

        return job_status

    def job_cancel(self):
        jobid = self.arguments.job_id
        res = local_cancel_job(self.local_job_scripts_dir, jobid)
        print(res)

    def monitor(self):
        if self.arguments.option == 'start':
            local_exec_app('/etc/init.d/compss-monitor start')
            local_exec_app('firefox http://localhost:8080/compss-monitor &')
        elif self.arguments.option == 'stop':
            local_exec_app('/etc/init.d/compss-monitor stop')
            
    def components(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def gengraph(self):
        command = "compss_gengraph " + self.arguments.dot_file
        local_exec_app(command)

    def gentrace(self):
        command = f"compss_gentrace {self.arguments.trace_dir} "
        command += ' '.join(self.arguments.rest_args)
        local_exec_app(command)
        if self.arguments.download_dir:
            local_exec_app(f'cp {self.arguments.trace_dir}/* {self.arguments.download_dir}/')

    def get_apps(self, env_id=None) -> List[App]:
        if self.apps is not None:
            return self.apps

        app_dir = os.path.expanduser('~') + f'/.COMPSsApps/'

        if not os.path.isdir(app_dir):
            return []
            
        if env_id is not None:
            app_dir += str(env_id)
        else:
            app_dir += self.env_conf['name']

        apps: List[App] = []
        for app_dir_name in os.listdir(app_dir):
            app_dir_path = app_dir + '/' + app_dir_name
            if os.path.isfile(app_dir_path + '/.compss'):
                with open(app_dir_path + '/.compss', 'r') as dest_dir_file:
                    dest_dir = dest_dir_file.read().strip()
                apps.append(App(app_dir_name, dest_dir))
            else:
                apps.append(App(app_dir_name))
        self.apps = apps
        return apps

    def inspect(self):
        # Code that inspects the RO-Crate
        local_inspect(self.arguments.ro_crate)



