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
import json
from glob import glob
from pathlib import Path
import subprocess
import os


def get_object_method_by_name(obj, method_name, include_in_name=False):
    for class_method_name in dir(obj):
        if not '__' in class_method_name and callable(getattr(obj, class_method_name)):
            if class_method_name.startswith(method_name) or (include_in_name and method_name in class_method_name):
                return class_method_name

def table_print(col_names, data):
    print_table(data, header=col_names)

def get_current_env_conf(env_id=None, return_path=False):
    home_path = str(Path.home())
    if env_id:
        current_env = glob(home_path + '/.COMPSs/envs/' + env_id + '/env.json')[0]
    else:
        current_file = glob(home_path + '/.COMPSs/envs/*/current')
        if len(current_file) > 0:
            current_file = current_file[0]
        else:
            current_file = home_path + '/.COMPSs/envs/default/current'
            with open(current_file, 'w') as env:
                pass
        current_env = current_file.replace('current', 'env.json')
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
    cmd = ' ; '.join(filter(len, commands))
    res = subprocess.run(f"ssh {login_info} '{cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    return res.stdout.decode(), res.stderr.decode()

def check_exit_code(command):
    return subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode


def print_table(items, header=None, wrap=True, wrap_style="wrap", row_line=False, fix_col_width=False):
    ''' Prints a matrix of data as a human readable table. Matrix
    should be a list of lists containing any type of values that can
    be converted into text strings.
    Two different column adjustment methods are supported through
    the *wrap_style* argument:
    
       wrap: it will wrap values to fit max_col_width (by extending cell height)
       cut: it will strip values to max_col_width
    If the *wrap* argument is set to False, column widths are set to fit all
    values in each column.
    This code is free software. Updates can be found at
    https://gist.github.com/jhcepas/5884168
    
    '''

    max_col_width = os.get_terminal_size().columns
        
    if fix_col_width:
        c2maxw = dict([(i, max_col_width) for i in range(len(items[0]))])
        wrap = True
    elif not wrap:
        c2maxw = dict([(i, max([len(str(e[i])) for e in items])) for i in range(len(items[0]))])
    else:
        c2maxw = dict([(i, min(max_col_width, max([len(str(e[i])) for e in items])))
                        for i in range(len(items[0]))])
    if header:
        current_item = -1
        row = header
        if wrap and not fix_col_width:
            for col, maxw in c2maxw.items():
                c2maxw[col] = max(maxw, len(header[col]))
                if wrap:
                    c2maxw[col] = min(c2maxw[col], max_col_width)
    else:
        current_item = 0
        row = items[current_item]
    while row:
        is_extra = False
        values = []
        extra_line = [""]*len(row)
        for col, val in enumerate(row):
            cwidth = c2maxw[col]
            wrap_width = cwidth
            val = str(val)
            try:
                newline_i = val.index("\n")
            except ValueError:
                pass
            else:
                wrap_width = min(newline_i+1, wrap_width)
                val = val.replace("\n", " ", 1)
            if wrap and len(val) > wrap_width:
                if wrap_style == "cut":
                    val = val[:wrap_width-1]+"+"
                elif wrap_style == "wrap":
                    extra_line[col] = val[wrap_width:]
                    val = val[:wrap_width]
            val = val.ljust(cwidth)
            values.append(val)
        print(' | '.join(values))
        if not set(extra_line) - set(['']):
            if header and current_item == -1:
                print(' | '.join(['='*c2maxw[col] for col in range(len(row)) ]))
            current_item += 1
            try:
                row = items[current_item]
            except IndexError:
                row = None
        else:
            row = extra_line
            is_extra = True
 
        if row_line and not is_extra and not (header and current_item == 0):
            if row:
                print(' | '.join(['-'*c2maxw[col] for col in range(len(row)) ]))
            else:
                print(' | '.join(['='*c2maxw[col] for col in range(len(extra_line)) ]))
