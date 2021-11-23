"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_player.core.remote.cmd import remote_app_deploy, remote_app_remove, remote_deploy_compss
from pycompss_player.core.remote.cmd import remote_submit_job
from pycompss_player.core.remote.cmd import remote_list_job
from pycompss_player.core.remote.cmd import remote_cancel_job
from pycompss_player.core.remote.cmd import remote_run_app
from pycompss_player.core.remote.cmd import remote_exec_app
from pycompss_player.core.actions import Actions
from pycompss_player.core import utils
from pycompss_player.core.remote.interactive_sc import core, defaults
import os, json, time
from collections import defaultdict

class RemoteActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

        self.apps = {}
        if self.env_conf and os.path.isfile(self.env_conf['env_path'] + '/apps.json'):
            with open(self.env_conf['env_path'] + '/apps.json') as f:
                self.apps = json.load(f)

        self.jobs = defaultdict(list)
        if self.env_conf and os.path.isfile(self.env_conf['env_path'] + '/jobs.json'):
            with open(self.env_conf['env_path'] + '/jobs.json') as f:
                self.jobs = defaultdict(list, json.load(f))

    def init(self):
        """ Deploys COMPSs infrastructure remote|cluster env

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        super().init()

        if self.arguments.modules is None:
            self.arguments.modules = ['COMPSs']
        elif len(self.arguments.modules) == 1 and os.path.isfile(self.arguments.modules[0]):
            self.arguments.modules = self.arguments.modules[0]
        elif 'COMPSs' not in [m[:len('COMPSs')] for m in self.arguments.modules]:
            self.arguments.modules.append('COMPSs')

        # Check if there's already another env in the cluster


        remote_deploy_compss(self.arguments.name, self.arguments.login, self.arguments.modules)

    def run(self):
        app_name = self.arguments.app_name
        if not app_name:
            print(f"ERROR: Application ID argument (-app) is required for executing runcompss in cluster")
            exit(1)

        if app_name not in self.apps:
            print(f"ERROR: Application `{app_name}` not found")
            exit(1)

        app_args = self.arguments.rest_args
        command = "runcompss " + ' '.join(app_args)
        login_info = self.env_conf['login']
        user = login_info.split('@')[0]
        remote_dir = self.apps[app_name]['remote_dir'].replace('~', f'/home/{user[:5]}/{user}')
        env_name = self.env_conf['name']

        print(remote_run_app(remote_dir, login_info, env_name, command))

    def job(self):
        action_name = 'list'

        if self.arguments.job:
            action_name = self.arguments.job

        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        app_name = self.arguments.app_name

        if app_name not in self.apps:
            print(f"ERROR: Application {app_name} not found")
            exit(1)

        login_info = self.env_conf['login']
        app_args = " ".join(self.arguments.rest_args)
        remote_dir = self.apps[app_name]['remote_dir']
        env_id = self.env_conf['name']
        job_id = remote_submit_job(env_id, login_info, remote_dir, app_args)

        self.jobs[job_id] = {
            'app_name': app_name,
            'enqueue_args': app_args
        }

        with open(self.env_conf['env_path'] + '/jobs.json', 'w') as f:
            json.dump(self.jobs, f)

    def job_history(self):
        job_id = self.arguments.job_id
        app_jobs = self.jobs[job_id]
        utils.table_print(['App Name', 'Enqueue Args'], [[app_jobs['app_name'], '    ' + app_jobs['enqueue_args']]])

    def job_list(self):
        login_info = self.env_conf['login']
        remote_list_job(login_info)

    def job_cancel(self):
        login_info = self.env_conf['login']
        jobid = self.arguments.job_id
        remote_cancel_job(login_info, jobid)

    def app(self):
        action_name = 'list'

        if self.arguments.app:
            action_name = self.arguments.app

        action_name = utils.get_object_method_by_name(self, 'app_' + action_name, include_in_name=True)
        getattr(self, action_name)()

    def app_deploy(self):
        if self.arguments.local_dir == 'current directory':
            self.arguments.local_dir = os.getcwd()

        app_name = self.arguments.app_name
        env_id = self.env_conf['name']

        if not self.arguments.remote_dir:
            self.arguments.remote_dir = f'~/.COMPSsApps/{env_id}/{app_name}'

        remote_app_deploy(self.env_conf['name'], self.env_conf['login'], self.arguments.local_dir, self.arguments.remote_dir)

        self.apps[app_name] = {
            'remote_dir': self.arguments.remote_dir
        }

        with open(self.env_conf['env_path'] + '/apps.json', 'w') as f:
            json.dump(self.apps, f)

    def app_remove(self):
        app_names = self.arguments.app_name
        for app_name in app_names:
            if app_name in self.apps:
                login_info = self.env_conf['login']
                remote_work_dir = self.apps[app_name]['remote_dir']
                remote_app_remove(login_info, remote_work_dir)

                if len(self.apps) == 1:
                    os.remove(self.env_conf['env_path'] + '/apps.json')
                    if os.path.isfile(self.env_conf['env_path'] + '/removed'):
                        self.env_remove()
                else:
                    del self.apps[app_name]
                    with open(self.env_conf['env_path'] + '/apps.json', 'w') as f:
                        json.dump(self.apps, f)
            else:
                print('ERROR: Application not found')
                exit(1)

    def app_list(self):
        if not self.apps:
            print('ERROR: There are no applications binded to this environment')
            print('       Try deploying an application to the cluster')
            exit(1)

        login_info = self.env_conf['login']
        apps_dirs = [app['remote_dir'] for app in self.apps.values() ]
        # apps_size = remote_app_size(login_info, apps_dirs)
        
        with open(self.env_conf['env_path'] + '/apps.json') as f:
            apps = json.load(f)
            utils.table_print(['Name'], [[k] for k in apps.keys()])

    def exec(self):
        login_info = self.env_conf['login']
        command = ' '.join(self.arguments.exec_cmd)
        remote_exec_app(login_info, command)

    def environment(self):
        super().environment()

    def env_remove(self):
        if os.path.isfile(self.env_conf['env_path'] + '/apps.json'):
            open(self.env_conf['env_path'] + '/removed', 'a').close()
            print('WARNING: There are still applications binded to this environment')
            print('         When all the apps are removed the environment will be also removed')
        else:
            if self.arguments.action == 'environment':
                super().env_remove()
            else:
                super().remove_current_env()

    def component(self):
        pass

    def gengraph(self):
        pass

    def monitor(self):
        pass

    def jupyter(self):

        app_name = self.arguments.app_name
        if not app_name:
            print(f"ERROR: Application ID argument (-app) is required for starting jupyter on cluster")
            exit(1)

        if app_name not in self.apps:
            print(f"ERROR: Application `{app_name}` not found")
            exit(1)
        
        login_info = self.env_conf['login']
        app_args = self.arguments.rest_args
        user = login_info.split('@')[0]
        remote_dir = self.apps[app_name]['remote_dir'].replace('~', f'/home/{user[:5]}/{user}')
        job_name = app_name + '-PyCOMPSsInteractive'
        app_args += ['--jupyter_notebook='+remote_dir, '--job_name='+job_name, '--lang=python', f'--master_working_dir={remote_dir}', '--tracing=false']
        app_args = ' '.join(app_args)
        env_id = self.env_conf['name']

        job_id = remote_submit_job(env_id, login_info, remote_dir, app_args, envars=['pythonversion=3-jupyter'])

        modules_path = f'source ~/.COMPSs/envs/{env_id}/modules.sh'

        compss_path = core._check_remote_compss(login_info, modules_path)

        scripts_path = core._infer_scripts_path(compss_path)

        print('Waiting for jupyter to start...')
        jupyter_job_status = defaults.NOT_RUNNING_KEYWORD
        while jupyter_job_status != 'RUNNING':
            jupyter_job_status = core.job_status(scripts_path, job_id, login_info, modules_path)

        print('Connecting to jupyter server...')
        time.sleep(5)

        core.connect_job(scripts_path, job_id, login_info, modules_path, web_browser=None)
        
        core.cancel_job(scripts_path, [job_id], login_info, modules_path)