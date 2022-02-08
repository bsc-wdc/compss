"""
This file contains the actions supported by pycompss-cli.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from collections import defaultdict
import datetime
import json
import traceback
from pycompss_cli.core.local.cmd import local_deploy_compss
from pycompss_cli.core.local.cmd import local_run_app
from pycompss_cli.core.local.cmd import local_exec_app
from pycompss_cli.core.local.cmd import local_jupyter
from pycompss_cli.core.local.cmd import local_submit_job
from pycompss_cli.core.local.cmd import local_job_list
from pycompss_cli.core.local.cmd import local_cancel_job
from pycompss_cli.core.local.cmd import local_job_status
from pycompss_cli.core.actions import Actions
import pycompss_cli.core.utils as utils
import os, sys


class LocalActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)
        path = os.path.abspath(__file__)
        self.local_job_scripts_dir = os.path.dirname(path) + '/../remote/job_scripts'

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
        if self.debug:
            print("Running...")
            print("Parameters:")
            print("\t- Application: " + self.arguments.application)
            print("\t- Arguments: " + str(self.arguments.argument))

        app_args = self.arguments.rest_args

        commands = [
            "runcompss " + ' '.join(app_args)
        ]

        if 'working_dir' in self.env_conf:
            commands.insert(0, 'cd ' + self.env_conf['working_dir'])
                
        local_run_app(commands)

    def jupyter(self):
        if utils.check_exit_code('jupyter') == 127:
            print('ERROR: Jupyter not found!')
            exit(1)

        working_dir = '.'
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']

        jupyter_args = self.arguments.rest_args
        local_jupyter(working_dir, ' '.join(jupyter_args))

    def exec(self):
        command = ' '.join(self.arguments.exec_cmd)
        local_exec_app(command)

    def app(self):
        print("ERROR: Wrong Environment! Try switching to a `cluster` environment")
        exit(1)

    def env_remove(self, env_id=None):
        super().env_remove(eid=env_id)

    def job(self):
        super().job()

        action_name = self.arguments.job
        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        super().job_submit()

        app_name = None

        working_dir = os.getcwd()
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']

        for arg in reversed(self.arguments.rest_args):
            file_path = working_dir + '/' + arg
            if os.path.isfile(file_path):
                app_name = arg
                break

        if app_name is None:
            print('WARNING: No application found in the working directory!')

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
                'app_name': app_name if app_name else 'error',
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
