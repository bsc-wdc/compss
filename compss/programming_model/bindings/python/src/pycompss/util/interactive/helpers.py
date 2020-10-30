#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Util - Interactive Mode Helpers
========================================
    Provides auxiliary methods for the interactive mode.
"""

import os
import inspect

# Debug mode: Changed to true from interactive.py if specified by the user
# when starting the runtime. Enables the explicit prints.
DEBUG = False


SEPARATORS = {  # for user defined lines in the entire/global scope
              'globals_separator': "### GLOBALS ###",
                # for user defined classes
              'classes_separator': '### CLASSES ###',
                # for user defined functions (that are not decorated)
              'functions_separator': "### FUNCTIONS ###",
                # for user defined tasks
              'tasks_separator': "### TASKS ###"}


PREFIXES = ("@implement", "@constraint", "@decaf", "@mpi",
            "@ompss", "@binary", "@opencl")

# ################################################################# #
# ################# MAIN FUNCTION ################################# #
# ################################################################# #


def update_tasks_code_file(f, file_path):
    # type: (..., str) -> None
    """ Main interactive helper function.

    Analyses the user code that has been executed and parses it looking for:
        - imports
        - tasks
        - functions
    Builds a file where the necessary contents for the worker are.
    Also updates the old code with the new if functions or tasks are redefined.

    :param f: New task function.
    :param file_path: File where the code is stored.
    :return: None
    """
    if not os.path.exists(file_path):
        _create_tasks_code_file(file_path)

    if DEBUG:
        print("Task definition detected.")

    # Intercept the code
    imports = _get_ipython_imports()      # [import\n, import\n, ...]
    global_code = _get_ipython_globals()  # [var\n, var\n, ...]
    classes_code = _get_classes()         # {'name': str(line\nline\n...)}
    functions_code = _get_functions()     # {'name': str(line\nline\n...)}
    task_code = _get_task_code(f)         # {'name': str(line\nline\n...)}
    old_code = _get_old_code(file_path)   # old_code structure:
    # {'imports':[import\n, import\n, ...],
    #  'tasks':{'name':str(line\nline\n...),
    #  'name':str(line\nline\n...), ...}}

    # Look for new/modified pieces of code and compares the existing code with
    # the new additions.
    new_imports = _update_imports(imports, old_code['imports'])
    new_globals = _update_globals(global_code, old_code['globals'])
    new_classes = _update_classes(classes_code, old_code['classes'])
    # Check that there are no functions with the same name as a newly defined
    # tasks
    for k in task_code.keys():
        functions_code.pop(k, None)
        old_code['functions'].pop(k, None)
    # Continue with comparisons
    new_functions = _update_functions(functions_code, old_code['functions'])
    new_tasks = _update_tasks(task_code, old_code['tasks'])

    # Update the file where the code is stored.
    _update_code_file(new_imports,
                      new_globals,
                      new_classes,
                      new_functions,
                      new_tasks,
                      file_path)


# ###################################################################
# ############### AUXILIAR METHODS ##################################
# ###################################################################

# CODE INTERCEPTION FUNCTIONS

def _create_tasks_code_file(file_path):
    # type: (str) -> None
    """ Creates a file where to store the user code.

    :param file_path: File location and name.
    :return: None
    """
    user_code_file = open(file_path, 'a')
    user_code_file.write('\n')
    user_code_file.write(SEPARATORS['globals_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(SEPARATORS['classes_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(SEPARATORS['functions_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(SEPARATORS['tasks_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.close()


def _get_raw_code():
    # type: () -> list
    """ Retrieve the raw code from interactive session.

    :return: the list of the blocks defined by the user that are currently
             loaded in globals.
    """
    import IPython  # noqa
    ipython = IPython.get_ipython()
    raw_code = ipython.user_ns['In']
    return raw_code


def _get_ipython_imports():
    # type: () -> list
    """ Finds the user imports.

    :return: A list of imports: [import\n, import\n, ...].
    """
    raw_code = _get_raw_code()
    imports = []
    for i in raw_code:
        # Each i can have more than one line (jupyter-notebook block)
        # We only get the lines that start with from or import and do not
        # have blank spaces before.
        lines = i.split('\n')
        for line in lines:
            if line.startswith("from") or line.startswith("import"):
                imports.append(line + '\n')
    return imports


def _get_ipython_globals():
    # type: () -> dict
    """ Finds the user global variables.

    WARNING: Assignations using any of the master api calls will be ignored
    in order to avoid the worker to try to call the runtime.

    WARNING2: We will consider only global variables that must be seen by the
    workers if the variable name is defined in capital letters.
    Please, take caution with the modification on the global
    variables since they can lead to raise conditions in the user
    code execution.

    :return: A list of lines: [var\n, var\n, ...].
    """
    api_calls = ['compss_open',
                 'compss_delete_file',
                 'compss_wait_on_file',
                 'compss_delete_object',
                 'compss_wait_on']

    raw_code = _get_raw_code()
    glob_lines = {}
    glob_name = ''
    for i in raw_code:
        # Each i can have more than one line (jupyter-notebook block)
        # We only get the lines that start with from or import and do not
        # have blank spaces before.
        lines = i.split('\n')
        found_one = False
        for line in lines:
            # if the line starts without spaces and is a variable assignation
            if not (line.startswith(' ') or line.startswith('\t')) and \
                    _is_variable_assignation(line):
                line_parts = line.split()
                glob_name = line_parts[0]
                if not glob_name.isupper():
                    # It is an assignation where the variable name is not in
                    # capital letters
                    found_one = False
                elif any(call in line_parts[2:] for call in api_calls):
                    # It is an assignation that does not contain a master api
                    # call
                    found_one = False
                else:
                    glob_lines[glob_name] = line.strip()
                    found_one = True
                continue
            # if the next line/s start with space or tab belong also to the
            # global variable
            if found_one and (line.startswith(' ') or line.startswith('\t')):
                # It is a multiple lines global variable definition
                glob_lines[glob_name] += line.strip()
            else:
                found_one = False
    return glob_lines


def _is_variable_assignation(line):
    # type: (str) -> bool
    """ Check if the line is a variable assignation.

    This function is used to check if a line of code represents a variable
    assignation:
    * if contains a '=' (assignation) and does not start with import, nor @,
      nor def, nor class.
    * then it is ==> is a global variable assignation.

    :param line: Line to parse
    :return: <Boolean>
    """
    if '=' in line:
        parts = line.split()
        if not (line.startswith("from") or
                line.startswith("import") or
                line.startswith("@") or
                line.startswith("def") or
                line.startswith("class")) \
                and len(parts) >= 3 and parts[1] == '=':
            # It is actually an assignation
            return True
        else:
            # It is an import/function/decorator/class definition
            return False
    else:
        # Not an assignation if does not contain '='
        return False


def _get_classes():
    # type: () -> dict
    """ Finds the user defined classes in the code.

    Output dictionary: {'name': str(line\nline\n...)}

    :return: A dictionary with the user classes code:
    """

    raw_code = _get_raw_code()
    classes = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for classes in the block
        class_found = False
        for line in lines:
            class_name = ''
            if line.startswith('class'):
                # Class header: find name and include it in the functions dict
                # split and remove empty spaces
                header = [name for name in line.split(" ") if name]
                # the name may be followed by the parameters parenthesis
                class_name = header[1].split("(")[0].strip()
                # create an entry in the functions dict
                classes[class_name] = [line + '\n']
                class_found = True
            elif (line.startswith("  ") or
                  (line.startswith("\t")) or
                  (line.startswith('\n')) or
                  (line == '')) and class_found:
                # class body found: append
                classes[class_name].append(line + '\n')
            else:
                class_found = False
    # Plain classes content (from {key: [line, line,...]} to {key: line\nline})
    for k, v in list(classes.items()):
        # Collapse all lines into a single one
        classes[k] = ''.join(v).strip()
    return classes


def _get_functions():
    # type: () -> dict
    """ Finds the user defined functions in the code.

    Output dictionary: {'name': str(line\nline\n...)}

    :return: A dictionary with the user functions code
    """
    raw_code = _get_raw_code()
    functions = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for functions in the block
        is_task = False
        is_function = False
        function_found = False
        func_name = ''
        decorators = ''
        for line in lines:
            if line.startswith('@task'):
                # The following function detected will be a task --> ignore
                is_task = True
            if line.startswith("@") and \
                    not any(map(line.startswith, PREFIXES)) and \
                    not is_task:
                # It is a function preceded by a decorator
                is_function = True
                is_task = False
            if line.startswith("def") and not is_task:
                # A function which is not a task has been defined --> capture
                # with is_function boolean
                # Restore the is_task boolean to control if another task is
                # defined in the same block.
                is_function = True
                is_task = False
            if is_function:
                if line.startswith("@"):
                    decorators += line + '\n'
                if line.startswith("def"):
                    # Function header: find name and include it in the
                    # functions dict. Split and remove empty spaces
                    header = [name for name in line.split(" ") if name]
                    # the name may be followed by the parameters parenthesis
                    func_name = header[1].split("(")[0].strip()
                    # create an entry in the functions dict
                    functions[func_name] = [decorators + line + '\n']
                    decorators = ''
                    function_found = True
                elif (line.startswith("  ") or
                      (line.startswith("\t")) or
                      (line.startswith('\n')) or
                      (line == '')) and function_found:
                    # Function body: append
                    functions[func_name].append(line + '\n')
                else:
                    function_found = False
    # Plain functions content:
    # from {key: [line, line,...]} to {key: line\nline}
    for k, v in list(functions.items()):
        functions[k] = ''.join(v).strip()  # Collapse all lines into one
    return functions


def _get_task_code(f):
    # type: (...) -> dict
    """ Finds the task code.

    :param f: Task function
    :return: A dictionary with the task code: {'name': str(line\nline\n...)}
    """
    try:
        task_code = inspect.getsource(f)
    except TypeError:
        # This is a numba jit declared task
        task_code = inspect.getsource(f.py_func)
    if task_code.startswith((' ', '\t')):
        return {}
    else:
        name = ''
        lines = task_code.split('\n')
        for line in lines:
            # Ignore the decorator stack
            if line.strip().startswith('def'):
                name = line.replace('(', ' (').split(' ')[1].strip()
                break  # Just need the first
        return {name: task_code}


def _clean(lines_list):
    # type: (list) -> list
    """ Removes the blank lines from a list of strings.

    * _get_old_code auxiliary method - Clean imports list.

    :param lines_list: List of strings.
    :return: The list without '\n' strings.
    """
    result = []
    if len(lines_list) == 1 and lines_list[0].strip() == '':
        # If the lines_list only contains a single line jump remove it
        return result
    else:
        # If it is longer, remove all single \n appearances
        for line in lines_list:
            if line.strip() != '':
                result.append(line)
        return result


def _get_old_code(file_path):
    # type: (str) -> dict
    """ Retrieve the old code from a file.

    :param file_path: The file where the code is located.
    :return: A dictionary with the imports and existing tasks.
    """
    # Read the entire file
    code_file = open(file_path, 'r')
    contents = code_file.readlines()
    code_file.close()

    # Separate imports from tasks
    file_imports = []
    file_globals = []
    file_classes = []
    file_functions = []
    file_tasks = []
    found_glob_separator = False
    found_class_separator = False
    found_func_separator = False
    found_task_separator = False
    for line in contents:
        if line == SEPARATORS['globals_separator'] + '\n':
            found_glob_separator = True
        elif line == SEPARATORS['classes_separator'] + '\n':
            found_class_separator = True
        elif line == SEPARATORS['functions_separator'] + '\n':
            found_func_separator = True
        elif line == SEPARATORS['tasks_separator'] + '\n':
            found_task_separator = True
        else:
            if not found_glob_separator and \
                    not found_class_separator and \
                    not found_func_separator and \
                    not found_task_separator:
                file_imports.append(line)
            elif found_glob_separator and \
                    not found_class_separator and \
                    not found_func_separator and \
                    not found_task_separator:
                file_globals.append(line)
            elif found_glob_separator and \
                    found_class_separator and \
                    not found_func_separator and \
                    not found_task_separator:
                file_classes.append(line)
            elif found_glob_separator and \
                    found_class_separator and \
                    found_func_separator and \
                    not found_task_separator:
                file_functions.append(line)
            else:
                file_tasks.append(line)

    file_imports = _clean(file_imports)
    file_globals = _clean(file_globals)
    # No need to clean the following
    # file_classes = _clean(file_classes)
    # file_functions = _clean(file_functions)
    # file_tasks = _clean(file_tasks)

    # Process globals
    globs = {}
    if len(file_globals) != 0:
        # Collapse all lines into a single one
        collapsed = ''.join(file_globals).strip()
        scattered = collapsed.split('\n')
        # Add classes to dictionary by class name:
        for g in scattered:
            glob_code = g.strip()
            glob_name = g.split()[0].strip()
            globs[glob_name] = glob_code
    file_globals = globs

    # Process classes
    classes = {}
    # Collapse all lines into a single one
    collapsed = ''.join(file_classes).strip()
    # Then split by "class" and filter the empty results, then iterate
    # concatenating "class" to all results.
    cls = [('class ' + class_line) for class_line in
           [name for name in collapsed.split('class ') if name]]
    # Add classes to dictionary by class name:
    for c in cls:
        class_code = c.strip()
        class_name = c.replace('(', ' (').split(' ')[1].strip()
        classes[class_name] = class_code

    # Process functions
    functions = {}
    # Clean empty lines
    clean_functions = [f_line for f_line in file_functions if f_line]
    # Iterate over the lines splitting by the ones that start with def
    funcs = []
    f = ''
    for line in clean_functions:
        if line.startswith('def'):
            if f:
                funcs.append(f)
            f = line
        else:
            f += line
    # Add functions to dictionary by function name:
    for f in funcs:
        func_code = f.strip()
        func_name = f.replace('(', ' (').split(' ')[1].strip()
        functions[func_name] = func_code

    # Process tasks
    tasks = {}
    # Collapse all lines into a single one
    collapsed = ''.join(file_tasks).strip()
    # Then split by "@" and filter the empty results, then iterate
    # concatenating "@" to all results.
    tsks = [('@' + deco_line) for deco_line in
            [deco for deco in collapsed.split('@') if deco]]
    # Take into account that other decorators my be over @task, so it is
    # necessary to collapse the function stack
    tasks_list = []
    tsk = ""
    for t in tsks:
        if any(map(t.startswith, PREFIXES)):
            tsk += t
        if t.startswith("@task"):
            tsk += t
            tasks_list.append(tsk)
            tsk = ""
        elif not any(map(t.startswith, PREFIXES)):
            # If a decorator over the function is provided, it will
            # have to be included in the last task
            tasks_list[-1] += t
    # Add functions to dictionary by function name:
    for t in tasks_list:
        # Example: '@task(returns=int)\ndef mytask(v):\n    return v+1'
        task_code = t.strip()
        task_header = t.split('\ndef')[1]
        task_name = task_header.replace('(', ' (').split(' ')[1].strip()
        tasks[task_name] = task_code

    old = {'imports': file_imports,
           'globals': file_globals,
           'classes': classes,
           'functions': functions,
           'tasks': tasks}
    return old


# #######################
# CODE UPDATE FUNCTIONS #
# #######################

def _update_imports(new_imports, old_imports):
    # type: (list, list) -> list
    """ Update imports.

    Compare the old imports against the new ones and returns the old imports
    with the new imports that did not existed previously.

    :param new_imports: All new imports.
    :param old_imports: All old imports.
    :return: A list of imports as strings.
    """
    not_in_imports = []
    for i in new_imports:
        already = False
        for j in old_imports:
            if i == j:
                already = True
        if not already:
            not_in_imports.append(i)
    # Merge the minimum imports
    imports = old_imports + not_in_imports
    return imports


def _update_globals(new_globals, old_globals):
    # type: (dict, dict) -> dict
    """ Update global variables.

    Compare the old globals against the new ones and returns the old globals
    with the new globals that did not existed previously.

    :param new_globals: All new globals.
    :param old_globals: All old globals.
    :return: A list of globals as strings.
    """
    if len(old_globals) == 0:
        return new_globals
    else:
        for global_name in list(new_globals.keys()):
            if DEBUG and global_name in old_globals and \
                    (not new_globals[global_name] == old_globals[global_name]):
                print("WARNING! Global variable " + global_name +
                      " has been redefined (the previous will be deprecated).")
            old_globals[global_name] = new_globals[global_name]
        return old_globals


def _update_classes(new_classes, old_classes):
    # type: (dict, dict) -> dict
    """ Update classes.

    Compare the old classes against the new ones. This function is essential
    due to the fact that a jupyter-notebook user may rewrite a function and
    the latest version is the one that needs to be kept.

    :param new_classes: dictionary containing all classes (last version).
    :param old_classes: dictionary containing the existing classes.
    :return: dictionary with the merging result (keeping all classes and
             updating the old ones).
    """
    if len(old_classes) == 0:
        return new_classes
    else:
        for class_name in list(new_classes.keys()):
            if DEBUG and class_name in old_classes and \
                    (not new_classes[class_name] == old_classes[class_name]):
                __show_redefinition_warning__("Class", class_name)
            old_classes[class_name] = new_classes[class_name]
        return old_classes


def _update_functions(new_functions, old_functions):
    # type: (dict, dict) -> dict
    """ Update functions.

    Compare the old functions against the new ones. This function is essential
    due to the fact that a jupyter-notebook user may rewrite a function and
    the latest version is the one that needs to be kept.

    :param new_functions: dictionary containing all functions (last version).
    :param old_functions: dictionary containing the existing functions.
    :return: dictionary with the merging result (keeping all functions and
             updating the old ones).
    """

    if len(old_functions) == 0:
        return new_functions
    else:
        for function_name in list(new_functions.keys()):
            if DEBUG and function_name in old_functions and\
                    (not new_functions[function_name] == old_functions[function_name]):
                __show_redefinition_warning__("Function", function_name)
            old_functions[function_name] = new_functions[function_name]
        return old_functions


def _update_tasks(new_tasks, old_tasks):
    # type: (dict, dict) -> dict
    """ Update task decorated functions.

    Compare the old tasks against the new ones. This function is essential due
    to the fact that a jupyter-notebook user may rewrite a task and the latest
    version is the one that needs to be kept.

    :param new_tasks: new tasks code.
    :param old_tasks: existing tasks.
    :return: dictionary with the merging result.
    """
    if not new_tasks:
        # when new_tasks is empty, means that the update was triggered by a
        # class task. No need to update as a tasks since the class has already
        # been updated
        pass
    else:
        task_name = list(new_tasks.keys())[0]
        if DEBUG and task_name in old_tasks and\
                (not new_tasks[task_name] == old_tasks[task_name]):
            __show_redefinition_warning__("Task", task_name)
        old_tasks[task_name] = new_tasks[task_name]
    return old_tasks


def __show_redefinition_warning__(kind, name):
    # type: (str, str) -> None
    """ Shows a warning notifying the redefinition of "kind" type. """
    print("WARNING! %s %s has been redefined (the previous will be deprecated)." % (kind, name))  # noqa: E501


# #######################
# FILE UPDATE FUNCTIONS #
# #######################

def _update_code_file(new_imports, new_globals, new_classes, new_functions,
                      new_tasks, file_path):
    # type: (list, dict, dict, dict, dict, str) -> None
    """ Writes the results to the code file used by the workers.

    :param new_imports: new imports.
    :param new_globals: new global variables.
    :param new_classes: new classes.
    :param new_functions: new functions.
    :param new_tasks: new tasks.
    :param file_path: File to update.
    :return: None
    """
    code_file = open(file_path, 'w')
    # Write imports
    for i in new_imports:
        code_file.write(i)
    code_file.write('\n')
    # Write globals separator
    code_file.write(SEPARATORS['globals_separator'] + '\n')
    # Write globals
    if len(new_globals) == 0:
        code_file.write('\n')
    else:
        for _, v in list(new_globals.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write classes separator
    code_file.write(SEPARATORS['classes_separator'] + '\n')
    # Write classes
    if len(new_classes) == 0:
        code_file.write('\n')
    else:
        for _, v in list(new_classes.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write functions separator
    code_file.write(SEPARATORS['functions_separator'] + '\n')
    # Write functions
    if len(new_functions) == 0:
        code_file.write('\n')
    else:
        for _, v in list(new_functions.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write tasks separator
    code_file.write(SEPARATORS['tasks_separator'] + '\n')
    # Write tasks
    if len(new_tasks) == 0:
        code_file.write('\n')
    else:
        for _, v in list(new_tasks.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    code_file.flush()
    code_file.close()
