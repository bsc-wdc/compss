
import re
from shutil import copyfile
import tempfile
import traceback
from pycompss_cli.core.actions import Actions
from pycompss_cli.core.unicore import UNICORE_URL_ENVAR
from pycompss_cli.core.unicore import UNICORE_USER_ENVAR
from pycompss_cli.core.unicore import UNICORE_PASSWORD_ENVAR
from pycompss_cli.core.unicore import UNICORE_TOKEN_ENVAR
from pycompss_cli.core import utils

import os

try:
    import pyunicore.client as uc_client
    import pyunicore.credentials as uc_credentials
    from pyunicore.client import PathFile
    from pyunicore.credentials import AuthenticationFailedException
except ImportError:
    print("Error: PyUNICORE not installed. Please install it with 'pip install pyunicore'")
    exit(1)

class UnicoreActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

        if env_conf is not None:
            if 'unicore_token' in env_conf:
                if UNICORE_TOKEN_ENVAR in os.environ:
                    env_conf['unicore_token'] = os.environ[UNICORE_TOKEN_ENVAR]
                    self.env_add_conf(env_conf)
                self.credential = uc_credentials.OIDCToken(env_conf['unicore_token'])
            else:
                if UNICORE_USER_ENVAR in os.environ and \
                    UNICORE_PASSWORD_ENVAR in os.environ:
                    env_conf['unicore_user'] = os.environ[UNICORE_USER_ENVAR]
                    env_conf['unicore_password'] = os.environ[UNICORE_PASSWORD_ENVAR]
                    self.env_add_conf(env_conf)
                user, password = env_conf['unicore_user'], env_conf['unicore_password']
                self.credential = uc_credentials.UsernamePassword(user, password)
            if 'unicore_base_url' in env_conf:
                try:
                    transport = uc_client.Transport(self.credential, oidc=False)
                    self.client = uc_client.Client(transport, env_conf['unicore_base_url'])
                    self.home_storage = uc_client.Storage(transport, env_conf['unicore_base_url'] + '/storages/HOME')
                    self.apps = self.unicore_list_apps()
                except AuthenticationFailedException:
                    print('ERROR: Authentication Failed')

    def __print_credential_errors(self):
        print("Error: You must provide the `base_url` and `user`,`password` or `token` parameters or set the following environment variables:")
        print(f"\t{UNICORE_URL_ENVAR}")
        print(f"\t{UNICORE_TOKEN_ENVAR}")
        print(f"\t{UNICORE_USER_ENVAR}")
        print(f"\t{UNICORE_USER_ENVAR}")
        print(f"\t{UNICORE_PASSWORD_ENVAR}")


    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure on unicore env

        :returns: None
        """

        try:
            if self.arguments.base_url is None:
                base_url = UNICORE_URL_ENVAR
            else:
                base_url = self.arguments.base_url

            if self.arguments.user is None and \
                self.arguments.password is None and \
                self.arguments.token is None:
                if UNICORE_TOKEN_ENVAR in os.environ:
                    token = os.environ[UNICORE_TOKEN_ENVAR].strip()
                    self.credential = uc_credentials.OIDCToken(token)
                elif UNICORE_USER_ENVAR in os.environ and \
                     UNICORE_PASSWORD_ENVAR in os.environ:
                    user = os.environ[UNICORE_USER_ENVAR]
                    password = os.environ[UNICORE_PASSWORD_ENVAR]
                    self.credential = uc_credentials.UsernamePassword(user, password)
                else:
                    self.__print_credential_errors()
                    raise
            elif self.arguments.token and \
                self.arguments.user is None and \
                self.arguments.password is None:
                self.credential = uc_credentials.OIDCToken(self.arguments.token.strip())
            elif self.arguments.user and self.arguments.password and \
                self.arguments.token is None:
                self.credential = uc_credentials.UsernamePassword(
                    self.arguments.user,
                    self.arguments.password
                )
            else:
                print('ERROR: CHOOSE EITHER USER/PASSWORD OR TOKEN AUTHENTICATION.')
                raise
        
            if base_url == UNICORE_URL_ENVAR and \
                UNICORE_URL_ENVAR not in os.environ:
                self.__print_credential_errors()
                raise

            env_conf = {
                'unicore_base_url': base_url,
            }

            if isinstance(self.credential, uc_credentials.UsernamePassword):
                print('Using USER/PASSWORD for authenticating')
                env_conf['unicore_user'] = self.credential.username
                env_conf['unicore_password'] = self.credential.password
            else:
                print('Using TOKEN for authenticating')
                env_conf['unicore_token'] = self.credential.token

            self.env_add_conf(env_conf)

            print('Deploying environment...')

            transport = uc_client.Transport(self.credential, oidc=False)
            self.client = uc_client.Client(transport, base_url)
            self.home_storage = uc_client.Storage(transport, base_url + '/storages/HOME')
            self.home_storage.mkdir(self.env_conf['name'])

            if self.arguments.modules is not None and \
                os.path.isfile(self.arguments.modules[0]):
                module_file = self.arguments.modules[0]
                copyfile(module_file, os.path.expanduser(f"~/.COMPSs/envs/{self.arguments.name}/modules.sh'"))
                self.home_storage.upload(module_file, f'{self.env_conf["name"]}/modules.sh')
                
            print('Environment deployed')
        except:
            traceback.print_exc()
            print("ERROR: Unicore deployment failed")
            self.env_remove(env_id=self.arguments.name)


    def get_apps(self):
        if self.apps is not None:
            return self.apps
        
        self.apps = self.unicore_list_apps()
        return self.apps


    def unicore_list_apps(self):
        apps_path = self.home_storage.listdir(self.env_conf['name']).keys()
        return [p[len(self.env_conf['name'])+1:-1] for p in apps_path]


    def app(self):
        if not self.arguments.app:
            self.arguments.func()
            exit(1)

        action_name = self.arguments.app
        action_name = utils.get_object_method_by_name(self, 'app_' + action_name, include_in_name=True)
        getattr(self, action_name)()

    
    def app_list(self):
        apps = self.get_apps()

        if not apps:
            print('INFO: There are no applications binded to this environment yet')
            print('       Try deploying an application first with `pycompss app deploy`')
            exit(1)

        utils.table_print(['Name'], [[a] for a in apps])

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
        app_dir = f'{env_id}/{app_name}/'

        files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(self.arguments.source_dir) for f in filenames]

        self.home_storage.mkdir(app_dir)
        print('Transferring files:')
        for f in files:
            dst_file = f[len(self.arguments.source_dir):]
            print(f'\t{dst_file}')
            self.home_storage.upload(f, app_dir + dst_file)

    def app_remove(self):
        app_names = self.arguments.app_name
        for app_name in app_names:
            if app_name in self.get_apps():
                self.home_storage.rmdir(f'{self.env_conf["name"]}/{app_name}/')
                print(f'Application `{app_name}` removed successfully')
            else:
                print(f'ERROR: Application `{app_name}` not found')
                exit(1)

    def components(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def env_remove(self, env_id=None):
        if hasattr(self, 'home_storage'):
            self.home_storage.rmdir(self.env_conf['name'])
        super().env_remove(eid=env_id)


    def exec(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def gengraph(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def gentrace(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

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

        env_id = self.env_conf['name']

        env_mod_file: PathFile  = self.home_storage.stat(f'{env_id}/modules.sh')
        env_mod_file.properties
        commands = [
            f'source $HOME/{env_id}/modules.sh',
            f'cd $HOME/{env_id}/{app_name}/'
        ]
        app_args = " ".join(self.arguments.rest_args)

        app_dir = f'$HOME/{env_id}/{app_name}/'

        app_args = f'--pythonpath={app_dir} ' + app_args
        app_args = f'--classpath={app_dir} ' + app_args
        app_args = f'--appdir={app_dir} ' + app_args
        app_args = f'--worker_working_dir={app_dir} ' + app_args
        
        app_args = 'enqueue_compss ' + app_args

        commands.append(app_args)
        command = ';'.join(commands)
        print(command)

        my_job = {
            'Executable': command,
            'Job type': 'on_login_node'
        }

        job = self.client.new_job(job_description=my_job, inputs=[])

        job.poll()

        work_dir = job.working_dir
        stdout = work_dir.stat("/stdout")
        stderr = work_dir.stat("/stderr")
        content = stdout.raw().read()
        content_err = stderr.raw().read()
        print(content_err)
        print(content)



    def jupyter(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def monitor(self):
        print('ERROR: Not Implemented Yet')
        exit(1)

    def run(self):
        print('ERROR: Not Implemented Yet')
        exit(1)