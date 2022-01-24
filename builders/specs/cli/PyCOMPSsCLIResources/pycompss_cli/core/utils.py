import json
from glob import glob
from pathlib import Path
import subprocess
import time, os

def get_object_method_by_name(obj, method_name, include_in_name=False):
    for class_method_name in dir(obj):
        if not '__' in class_method_name and callable(getattr(obj, class_method_name)):
            if class_method_name.startswith(method_name) or (include_in_name and method_name in class_method_name):
                return class_method_name

def table_print(col_names, data):
    max_len = max(map(len, [item for sublist in data for item in sublist if item is not None])) + 1
    row_format ="{:>{l}}" * (len(col_names) + 1)
    print(row_format.format("", l=max_len, *col_names))
    for row in data:
        print(row_format.format('-', l=max_len, *row))

def get_current_env_conf(return_path=False):
    home_path = str(Path.home())
    current_env = glob(home_path + '/.COMPSs/envs/*/current')[0].replace('current', 'env.json')
    with open(current_env, 'r') as env:
        if return_path:
            return json.load(env), current_env
        return json.load(env)

def get_env_conf_by_name(env_name):
    home_path = str(Path.home())
    env_path = home_path + '/.COMPSs/envs/' + env_name + '/env.json'
    with open(env_path, 'r') as env:
        return json.load(env)

def ssh_run_commands(login_info, commands, **kwargs):
    cmd = ' ; '.join(commands)
    res = subprocess.run(f"ssh {login_info} '{cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    return res.stdout.decode()

def check_exit_code(command):
    return subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode

def poll_directory(directory, sleep_time=0.1):
    files = []
    while True:
        new_query = os.listdir(directory)
        if new_query != files:
            files = new_query
            yield set(files)
        time.sleep(sleep_time)