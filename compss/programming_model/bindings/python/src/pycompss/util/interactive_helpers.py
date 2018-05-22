#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Binding - Interactive Mode Helpers
===========================================
Provides auxiliar methods for the interactive mode.
"""

import os

separators = {'globals_separator': "### GLOBALS ###",  # for user defined lines in the entire/global scope
              'classes_separator': '### CLASSES ###',  # for user defined classes
              'functions_separator': "### FUNCTIONS ###",  # for user defined functions (that are not decorated)
              'tasks_separator': "### TASKS ###"}  # for user defined tasks


# ###################################################################
# ################# MAIN FUNCTION ###################################
# ###################################################################


def updateTasksCodeFile(f, file_path):
    """
    Main interactive helper function.
    Analyses the user code that has been executed and parses it looking for:
        - imports
        - tasks
        - functions
    Builds a file where the necessary contents for the worker are.
    Also updates the old code with the new if functions or tasks are redefined.
    :param f: new task function
    :param file_path: file where the code is stored
    """
    if not os.path.exists(file_path):
        createTasksCodeFile(file_path)

    print("Task definition detected.")

    # Intercept the code
    imports = getIPythonImports()  # [import\n, import\n, ...]
    global_code = getIPythonGlobals()  # [var\n, var\n, ...]
    classes_code = getClasses()  # {'name': str(line\nline\n...)}
    functions_code = getFunctions()  # {'name': str(line\nline\n...)}
    task_code = getTaskCode(f)  # {'name': str(line\nline\n...)}
    old_code = getOldCode(
        file_path)  # {'imports':[import\n, import\n, ...], 'tasks':{'name':str(line\nline\n...), 'name':str(line\nline\n...), ...}}

    # Look for new/modified pieces of code. Compares the existing code with the new additions.
    new_imports = updateImports(imports, old_code['imports'])
    new_globals = updateGlobals(global_code, old_code['globals'])
    new_classes = updateClasses(classes_code, old_code['classes'])
    new_functions = updateFunctions(functions_code, old_code['functions'])
    new_tasks = updateTasks(task_code, old_code['tasks'])

    # Update the file where the code is stored.
    updateCodeFile(new_imports, new_globals, new_classes, new_functions, new_tasks, file_path)


# ###################################################################
# ############### AUXILIAR METHODS ##################################
# ###################################################################

# CODE INTERCEPTION FUNCTIONS

def createTasksCodeFile(file_path):
    """
    Creates a file where to store the user code.
    :param file_path: File location and name
    """

    user_code_file = open(file_path, 'a')
    user_code_file.write('\n')
    user_code_file.write(separators['globals_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(separators['classes_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(separators['functions_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.write(separators['tasks_separator'] + "\n")
    user_code_file.write('\n')
    user_code_file.close()


def getRawCode():
    """
    Retrieve the raw code from jupyter
    :return: the list of the blocks defined by the user that are currently
             loaded in globals
    """
    # globals()['In'] # is not in this scope
    # Retrieve the self of ipython where to look
    ipython = globals()['__builtins__']['get_ipython']()
    # If you want to show the contents of the ipython object for analysis
    # file.write(str(ipython.__dict__))
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    raw_code = ipython.__dict__['user_ns']['In']
    return raw_code


def getIPythonImports():
    """
    Finds the user imports
    :return: A list of imports: [import\n, import\n, ...]
    """

    raw_code = getRawCode()
    imports = []
    for i in raw_code:
        # Each i can have more than one line (jupyter-notebook block)
        # We only get the lines that start with from or import and do not
        # have blank spaces before.
        lines = i.split('\n')
        for l in lines:
            if l.startswith("from") or l.startswith("import"):
                imports.append(l + '\n')
    return imports


def getIPythonGlobals():
    """
    Finds the user global variables
    :return: A list of lines: [var\n, var\n, ...]
    """

    raw_code = getRawCode()
    glob_lines = {}
    for i in raw_code:
        # Each i can have more than one line (jupyter-notebook block)
        # We only get the lines that start with from or import and do not
        # have blank spaces before.
        lines = i.split('\n')
        for l in lines:
            # if the line starts without spaces and is a variable assignation
            if not (l.startswith(' ') or l.startswith('\t')) and isVariableAssignation(l):
                glob_name = l.split()[0]
                glob_lines[glob_name] = l.strip()
    return glob_lines


def isVariableAssignation(line):
    """
    This function is used to check if a line of code represents a variable assignation:
    * if contains a '=' (assignation) and does not start with import, nor @, nor def, nor class
    * then it is ==> is a global variable assignation

    :param line: Line to parse
    :return: Boolean
    """

    if '=' in line:
        parts = line.split()
        if not (line.startswith("from") or
                line.startswith("import") or
                line.startswith("@") or
                line.startswith("def") or
                line.startswith("class") or '(' in line or ')' in line) \
                and len(parts) == 3 and parts[1] == '=':
            # It is actually an assignation
            return True
        else:
            # It is an import/function/decorator/class definition
            return False
    else:
        # Not an assignation if does not contain '='
        return False


def getClasses():
    """
    Finds the user defined classes in the code
    :return: A dictionary with the user classes code:
             {'name': str(line\nline\n...)}
    """

    raw_code = getRawCode()
    classes = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for classes in the block
        class_found = False
        for line in lines:
            if line.startswith('class'):
                # Class header: find name and include it in the functions dict
                # split and remove empty spaces
                header = [name for name in line.split(" ") if name]
                # the name may be followed by the parameters parenthesis
                class_name = header[1].split("(")[0].strip()
                # create an entry in the functions dict
                classes[class_name] = [line + '\n']
                class_found = True
            elif (line.startswith("  ") or (line.startswith("\t")) or (line.startswith('\n')) or (
                    line == '')) and class_found:
                # class body: append
                classes[class_name].append(line + '\n')
            else:
                class_found = False
    # Plain classes content (from {key: [line, line,...]} to {key: line\nline}).
    for k, v in list(classes.items()):
        # Collapse all lines into a single one
        classes[k] = ''.join(v).strip()
    return classes


def getFunctions():
    """
    Finds the user defined functions in the code
    :return: A dictionary with the user functions code:
             {'name': str(line\nline\n...)}
    """

    raw_code = getRawCode()
    functions = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for functions in the block
        is_task = False
        is_function = False
        function_found = False
        for line in lines:
            if line.startswith('@task'):
                # The followihg function detected will be a task --> ignore
                is_task = True
            if line.startswith("def") and not is_task:
                # A function which is not a task has been defined --> capture
                # with isFunction boolean
                # Restore the isTask boolean to control if another task is
                # defined in the same block.
                is_function = True
                is_task = False
            if is_function:
                if line.startswith("def"):
                    # Function header: find name and include it in the functions dict
                    # split and remove empty spaces
                    header = [name for name in line.split(" ") if name]
                    # the name may be followed by the parameters parenthesis
                    func_name = header[1].split("(")[0].strip()
                    # create an entry in the functions dict
                    functions[func_name] = [line + '\n']
                    function_found = True
                elif (line.startswith("  ") or (line.startswith("\t")) or (line.startswith('\n')) or (
                        line == '')) and function_found:
                    # Function body: append
                    functions[func_name].append(line + '\n')
                else:
                    function_found = False
    # Plain functions content (from {key: [line, line,...]} to {key: line\nline}).
    for k, v in list(functions.items()):
        functions[k] = ''.join(v).strip()  # Collapse all lines into a single one
    return functions


def getTaskCode(f):
    """
    Finds the task code
    :param f: Task function
    :return: A dictionary with the task code: {'name': str(line\nline\n...)}
    """

    import inspect
    task_code = inspect.getsource(f)  # .strip()
    if task_code.startswith((' ', '\t')):
        return {}
    else:
        name = ''
        lines = task_code.split('\n')
        for line in lines:
            if line.strip().startswith('def'):
                name = line.replace('(', ' (').split(' ')[1].strip()
        return {name: task_code}


def clean(linesList):
    """
    Removes the blank lines from a list of strings.
    * getOldCode auxiliar method - Clean imports list.
    :param linesList: List of strings
    :return: The list without '\n' strings.
    """

    result = []
    if len(linesList) == 1 and linesList[0].strip() == '':
        # If the linesList only contains a single line jump remove it
        return result
    else:
        # If it is longer, remove all single \n appearances
        for l in linesList:
            if l.strip() != '':
                result.append(l)
        return result


def getOldCode(file_path):
    """
    Retrieve the old code from a file.
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
    fileClasses = []
    file_functions = []
    file_tasks = []
    found_glob_separator = False
    found_class_separator = False
    found_func_separator = False
    found_task_separator = False
    for line in contents:
        if line == separators['globals_separator'] + '\n':
            found_glob_separator = True
        elif line == separators['classes_separator'] + '\n':
            found_class_separator = True
        elif line == separators['functions_separator'] + '\n':
            found_func_separator = True
        elif line == separators['tasks_separator'] + '\n':
            found_task_separator = True
        else:
            if not found_glob_separator and not found_class_separator and not found_func_separator and not found_task_separator:
                file_imports.append(line)
            elif found_glob_separator and not found_class_separator and not found_func_separator and not found_task_separator:
                file_globals.append(line)
            elif found_glob_separator and found_class_separator and not found_func_separator and not found_task_separator:
                fileClasses.append(line)
            elif found_glob_separator and found_class_separator and found_func_separator and not found_task_separator:
                file_functions.append(line)
            else:
                file_tasks.append(line)

    file_imports = clean(file_imports)
    file_globals = clean(file_globals)
    # fileClasses = clean(fileClasses)
    # fileFunctions = clean(fileFunctions)
    # fileTasks = clean(fileTasks)

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
    collapsed = ''.join(fileClasses).strip()
    # Then split by "class" and filter the empty results, then iterate
    # concatenating "class" to all results.
    cls = [('class ' + l) for l in [name for name in collapsed.split('class ') if name]]
    # Add classes to dictionary by class name:
    for c in cls:
        class_code = c.strip()
        class_name = c.replace('(', ' (').split(' ')[1].strip()
        classes[class_name] = class_code

    # Process functions
    functions = {}
    # Collapse all lines into a single one
    collapsed = ''.join(file_functions).strip()
    # Then split by "def" and filter the empty results, then iterate
    # concatenating "def" to all results.
    funcs = [('def ' + l) for l in [name for name in collapsed.split('def ') if name]]
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
    tsks = [('@' + l) for l in [deco for deco in collapsed.split('@') if deco]]
    # Take into account that other decorators my be over @task, so it is
    # necessary to collapse the function stack
    prefixes = ("@implement", "@constraint", "@decaf", "@mpi", "@ompss", "@binary", "@opencl")
    tsks_stacked = []
    tsk = ""
    for t in tsks:
        if any(map(t.startswith, prefixes)):
            tsk += t
        if t.startswith("@task"):
            tsk += t
            tsks_stacked.append(tsk)
            tsk = ""
    # Add functions to dictionary by function name:
    for t in tsks_stacked:
        # Example: '@task(returns=int)\ndef mytask(v):\n    return v+1'
        task_code = t.strip()
        task_header = t.split('\ndef')[1]
        task_name = task_header.replace('(', ' (').split(' ')[1].strip()
        tasks[task_name] = task_code

    return {'imports': file_imports, 'globals': file_globals, 'classes': classes, 'functions': functions,
            'tasks': tasks}


# CODE UPDATE FUNCTIONS

def updateImports(new_imports, old_imports):
    """
    Compare the old imports against the new ones and returns the old imports
    with the new imports that did not existed previously
    :param new_imports: All new imports
    :param old_imports: All old imports
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


def updateGlobals(new_globals, old_globals):
    """
    Compare the old globals against the new ones and returns the old globals
    with the new globals that did not existed previously
    :param new_globals: All new globals
    :param old_globals: All old globals
    :return: A list of globals as strings.
    """

    if len(old_globals) == 0:
        return new_globals
    else:
        for gName in list(new_globals.keys()):
            if gName in old_globals and (not new_globals[gName] == old_globals[gName]):
                print((
                        "WARNING! Global variable " + gName + " has been redefined with changes (the previous will be deprecated)."))
            old_globals[gName] = new_globals[gName]
        return old_globals


def updateClasses(new_classes, old_classes):
    """
    Compare the old classes against the new ones. This function is essential
    due to the fact that a jupyter-notebook user may rewrite a function and
    the latest version is the one that needs to be kept.
    :param new_classes: dictionary containing all classes (last version)
    :param old_classes: dictionary containing the existing classes.
    return: dictionary with the merging result (keeping all classes and
            updating the old ones).
    """

    if len(old_classes) == 0:
        return new_classes
    else:
        for cName in list(new_classes.keys()):
            if cName in old_classes and (not new_classes[cName] == old_classes[cName]):
                print(
                    ("WARNING! Class " + cName + " has been redefined with changes (the previous will be deprecated)."))
            old_classes[cName] = new_classes[cName]
        return old_classes


def updateFunctions(new_functions, old_functions):
    """
    Compare the old functions against the new ones. This function is essential
    due to the fact that a jupyter-notebook user may rewrite a function and
    the latest version is the one that needs to be kept.
    :param new_functions: dictionary containing all functions (last version)
    :param old_functions: dictionary containing the existing functions.
    return: dictionary with the merging result (keeping all functions and
            updating the old ones).
    """

    if len(old_functions) == 0:
        return new_functions
    else:
        for fName in list(new_functions.keys()):
            if fName in old_functions and (not new_functions[fName] == old_functions[fName]):
                print(
                    (
                            "WARNING! Function " + fName + " has been redefined with changes (the previous will be deprecated)."))
            old_functions[fName] = new_functions[fName]
        return old_functions


def updateTasks(new_tasks, old_tasks):
    """
    Compare the old tasks against the new ones. This function is essential due
    to the fact that a jupyter-notebook user may rewrite a task and the latest
    version is the one that needs to be kept.
    :param newTask: new Task code
    :param tasks: existing tasks
    return: dictionary with the merging result.
    """

    if not new_tasks:
        # when newTasks is empty, means that the update was triggered by a class
        # task. No need to update as a tasks since the class has already been updated
        pass
    else:
        new_task_name = list(new_tasks.keys())[0]
        if new_task_name in old_tasks and (not new_tasks[new_task_name] == old_tasks[new_task_name]):
            print(("WARNING! Task " + new_task_name + " has been redefined (the previous will be deprecated)."))
        old_tasks[new_task_name] = new_tasks[new_task_name]
    return old_tasks


# FILE UPDATE FUNCTIONS

def updateCodeFile(new_imports, new_globals, new_classes, new_functions, new_tasks, file_path):
    """
    Writes the results to the code file used by the workers.
    :param new_imports: new imports
    :param new_globals: new global variables
    :param new_classes: new classes
    :param new_functions: new functions
    :param new_tasks: new tasks
    :param file_path: File to update.
    :return:
    """

    code_file = open(file_path, 'w')
    # Write imports
    for i in new_imports:
        code_file.write(i)
    code_file.write('\n')
    # Write globals separator
    code_file.write(separators['globals_separator'] + '\n')
    # Write globals
    if len(new_globals) == 0:
        code_file.write('\n')
    else:
        for k, v in list(new_globals.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write classes separator
    code_file.write(separators['classes_separator'] + '\n')
    # Write classes
    if len(new_classes) == 0:
        code_file.write('\n')
    else:
        for k, v in list(new_classes.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write functions separator
    code_file.write(separators['functions_separator'] + '\n')
    # Write functions
    if len(new_functions) == 0:
        code_file.write('\n')
    else:
        for k, v in list(new_functions.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    # Write tasks separator
    code_file.write(separators['tasks_separator'] + '\n')
    # Write tasks
    if len(new_tasks) == 0:
        code_file.write('\n')
    else:
        for k, v in list(new_tasks.items()):
            for line in v:
                code_file.write(line)
            code_file.write('\n')
            code_file.write('\n')
    code_file.close()
