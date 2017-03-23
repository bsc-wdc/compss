#
#  Copyright 2002-2017 Barcelona Supercomputing Center (www.bsc.es)
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
@author: fconejer

PyCOMPSs Binding - Interactive Mode Helpers
===========================================
Provides auxiliar methods for the interactive mode.
"""

import os

separators = {'globals_separator':"### GLOBALS ###",      # for user defined lines in the entire/global scope
              'classes_separator':'### CLASSES ###',      # for user defined classes
              'functions_separator':"### FUNCTIONS ###",  # for user defined functions (that are not decorated)
              'tasks_separator':"### TASKS ###"}          # for user defined tasks

####################################################################
################## MAIN FUNCTION ###################################
####################################################################


def updateTasksCodeFile(f, filePath):
    """
    Main interactive helper function.
    Analyses the user code that has been executed and parses it looking for:
        - imports
        - tasks
        - functions
    Builds a file where the necessary contents for the worker are.
    Also updates the old code with the new if functions or tasks are redefined.
    :param f: new task function
    :param filePath: file where the code is stored
    """
    if not os.path.exists(filePath):
        createTasksCodeFile(filePath)

    print("Task definition detected.")

    # Intercept the code
    imports = getIPythonImports()     ######## [import\n, import\n, ...]
    globalCode = getIPythonGlobals()  ######## [var\n, var\n, ...]
    classesCode = getClasses()        ######## {'name': str(line\nline\n...)}
    functionsCode = getFunctions()    ######## {'name': str(line\nline\n...)}
    taskCode = getTaskCode(f)         ######## {'name': str(line\nline\n...)}
    oldCode = getOldCode(filePath)    ######## {'imports':[import\n, import\n, ...], 'tasks':{'name':str(line\nline\n...), 'name':str(line\nline\n...), ...}}

    # Look for new/modified pieces of code. Compares the existing code with the new additions.
    newImports = updateImports(imports, oldCode['imports'])
    newGlobals = updateGlobals(globalCode, oldCode['globals'])
    newClasses = updateClasses(classesCode, oldCode['classes'])
    newFunctions = updateFunctions(functionsCode, oldCode['functions'])
    newTasks = updateTasks(taskCode, oldCode['tasks'])

    # Update the file where the code is stored.
    updateCodeFile(newImports, newGlobals, newClasses, newFunctions, newTasks, filePath)




####################################################################
################ AUXILIAR METHODS ##################################
####################################################################

###  * CODE INTERCEPTION FUNCTIONS

def createTasksCodeFile(filePath):
    """
    Creates a file where to store the user code.
    :param filePath: File location and name
    """
    file = open(filePath, 'a')
    file.write('\n')
    file.write(separators['globals_separator'] + "\n")
    file.write('\n')
    file.write(separators['classes_separator'] + "\n")
    file.write('\n')
    file.write(separators['functions_separator'] + "\n")
    file.write('\n')
    file.write(separators['tasks_separator'] + "\n")
    file.write('\n')
    file.close()


def getRawCode():
    """
    Retrieve the raw code from jupyter
    :return: the list of the blocks defined by the user that are currently loaded in globals
    """
    # print globals()['In'] # is not in this scope
    ipython = globals()['__builtins__']['get_ipython']()  # retrieve the self of ipython where to look
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
        # We only get the lines that start with from or import and do not have blank spaces before.
        lines = i.split('\n')
        for l in lines:
            if l.startswith("from") or l.startswith("import"):
                imports.append(l+'\n')
    return imports


def getIPythonGlobals():
    """
    Finds the user global variables
    :return: A list of lines: [var\n, var\n, ...]
    """
    raw_code = getRawCode()
    globLines = {}
    for i in raw_code:
        # Each i can have more than one line (jupyter-notebook block)
        # We only get the lines that start with from or import and do not have blank spaces before.
        lines = i.split('\n')
        for l in lines:
            # if the line starts without spaces and is a variable assignation
            if not (l.startswith(' ') or l.startswith('\t')) and isVariableAssignation(l):
                globName = l.split()[0]
                globLines[globName] = l.strip()
    return globLines


def isVariableAssignation(line):
    '''
    This function is used to check if a line of code represents a variable assignation.
    * if contains a '=' (assignation) and does not start with import, nor @, nor def, nor class
    * then it is ==> is a global variable assignation
    :param line: Line to parse
    :return: Boolean
    '''
    if '=' in line:
        parts = line.split()
        if not (line.startswith("from") or line.startswith("import") or line.startswith("@") or line.startswith("def")
                or line.startswith("class") or '(' in line or ')' in line) and len(parts) == 3 and parts[1] == '=':
            return True  # It is actually an assignation
        else:
            return False # It is an import/function/decorator/class definition
    else:
        return False     # Not an assignation if does not contain '='


def getClasses():
    """
    Finds the user defined classes in the code
    :return: A dictionary with the user classes code: {'name': str(line\nline\n...)}
    """
    raw_code = getRawCode()
    classes = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for classes in the block
        classFound = False
        for l in lines:
            if l.startswith('class'):
                # Class header: find name and include it in the functions dict
                header = filter(None, l.split(" "))  # split and remove empty spaces
                className = header[1].split("(")[0].strip()  # the name may be followed by the parameters parenthesis
                classes[className] = [l + '\n']  # create an entry in the functions dict
                classFound = True
            elif (l.startswith("  ") or (l.startswith("\t")) or (l.startswith('\n')) or (l == '')) and classFound:
                # class body: append
                classes[className].append(l + '\n')
            else:
                classFound = False
    # Plain classes content (from {key: [line, line,...]} to {key: line\nline}).
    for k,v in classes.iteritems():
        classes[k] = ''.join(v).strip()      # Collapse all lines into a single one
    return classes


def getFunctions():
    """
    Finds the user defined functions in the code
    :return: A dictionary with the user functions code: {'name': str(line\nline\n...)}
    """
    raw_code = getRawCode()
    functions = {}
    for block in raw_code:
        lines = block.split('\n')
        # Look for functions in the block
        isTask = False
        isFunction = False
        functionFound = False
        for l in lines:
            if l.startswith('@task'):
                # The followihg function detected will be a task --> ignore
                isTask = True
            if l.startswith("def") and not isTask:
                # A function which is not a task has been defined --> capture with isFunction boolean
                # Restore the isTask boolean to control if another task is defined in the same block.
                isFunction = True
                isTask = False
            if isFunction:
                if l.startswith("def"):
                    # Function header: find name and include it in the functions dict
                    header = filter(None, l.split(" "))  # split and remove empty spaces
                    funcName = header[1].split("(")[0].strip()  # the name may be followed by the parameters parenthesis
                    functions[funcName] = [l + '\n']  # create an entry in the functions dict
                    functionFound = True
                elif (l.startswith("  ") or (l.startswith("\t")) or (l.startswith('\n')) or (l == '')) and functionFound:
                    # Function body: append
                    functions[funcName].append(l + '\n')
                else:
                    functionFound = False
    # Plain functions content (from {key: [line, line,...]} to {key: line\nline}).
    for k,v in functions.iteritems():
        functions[k] = ''.join(v).strip()      # Collapse all lines into a single one
    return functions


def getTaskCode(f):
    """
    Finds the task code
    :param f: Task function
    :return: A dictionary with the task code: {'name': str(line\nline\n...)}
    """
    import inspect
    taskCode = inspect.getsource(f).strip()
    name = ''
    lines = taskCode.split('\n')
    for line in lines:
        if line.startswith('def'):
            name = line.replace('(', ' (').split(' ')[1].strip()
    return {name:taskCode}


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


def getOldCode(filePath):
    """
    Retrieve the old code from a file.
    :param filePath: The file where the code is located.
    :return: A dictionary with the imports and existing tasks.
    """
    # Read the entire file
    file = open(filePath, 'r')
    contents = file.readlines()
    file.close()
    # Separate imports from tasks
    fileImports = []
    fileGlobals = []
    fileClasses = []
    fileFunctions = []
    fileTasks = []
    foundGlobSeparator = False
    foundClasSeparator = False
    foundFuncSeparator = False
    foundTaskSeparator = False
    for line in contents:
        if line == separators['globals_separator'] + '\n':
            foundGlobSeparator = True
        elif line == separators['classes_separator'] + '\n':
            foundClasSeparator = True
        elif line == separators['functions_separator'] + '\n':
            foundFuncSeparator = True
        elif line == separators['tasks_separator'] + '\n':
            foundTaskSeparator = True
        else:
            if not foundGlobSeparator and not foundClasSeparator and not foundFuncSeparator and not foundTaskSeparator:
                fileImports.append(line)
            elif foundGlobSeparator and not foundClasSeparator and not foundFuncSeparator and not foundTaskSeparator:
                fileGlobals.append(line)
            elif foundGlobSeparator and foundClasSeparator and not foundFuncSeparator and not foundTaskSeparator:
                fileClasses.append(line)
            elif foundGlobSeparator and foundClasSeparator and foundFuncSeparator and not foundTaskSeparator:
                fileFunctions.append(line)
            else:
                fileTasks.append(line)

    fileImports = clean(fileImports)
    fileGlobals = clean(fileGlobals)
    # fileClasses = clean(fileClasses)
    # fileFunctions = clean(fileFunctions)
    # fileTasks = clean(fileTasks)

    # Process globals
    globs = {}
    if len(fileGlobals) != 0:
        collapsed = ''.join(fileGlobals).strip()  # Collapse all lines into a single one
        scattered = collapsed.split('\n')
        # Add classes to dictionary by class name:
        for g in scattered:
            globCode = g.strip()
            globName = g.split()[0].strip()
            globs[globName] = globCode
    fileGlobals = globs

    # Process classes
    classes = {}
    collapsed = ''.join(fileClasses).strip()      # Collapse all lines into a single one
    # Then split by "class" and filter the empty results, then iterate concatenating "class" to all results.
    cls = [('class' + l) for l in filter(None, collapsed.split('class'))]
    # Add classes to dictionary by class name:
    for c in cls:
        classCode = c.strip()
        className = c.replace('(', ' (').split(' ')[1].strip()
        classes[className] = classCode

    # Process functions
    functions = {}
    collapsed = ''.join(fileFunctions).strip()      # Collapse all lines into a single one
    # Then split by "def" and filter the empty results, then iterate concatenating "def" to all results.
    funcs = [('def' + l) for l in filter(None, collapsed.split('def'))]
    # Add functions to dictionary by function name:
    for f in funcs:
        funcCode = f.strip()
        funcName = f.replace('(', ' (').split(' ')[1].strip()
        functions[funcName] = funcCode

    # Process tasks
    tasks = {}
    collapsed = ''.join(fileTasks).strip()  # Collapse all lines into a single one
    # Then split by "@" and filter the empty results, then iterate concatenating "@" to all results.
    # TODO CHECK IF MULTIPLE DECORATORS CAN BE SUPPORTED
    tsks = [('@' + l) for l in filter(None, collapsed.split('@'))]
    # Add functions to dictionary by function name:
    for t in tsks:
        taskCode = t.strip()   # Example: '@task(returns=int)\ndef mytask(v):\n    return v+1'
        taskHeader = t.split('\ndef')[1]
        taskName = taskHeader.replace('(', ' (').split(' ')[1].strip()
        tasks[taskName] = taskCode

    return {'imports': fileImports, 'globals': fileGlobals, 'classes':classes, 'functions':functions, 'tasks': tasks}


###  * CODE UPDATE FUNCTIONS

def updateImports(newImports, oldImports):
    """
    Compare the old imports against the new ones and returns the old imports with the new imports that did
    not existed previously
    :param newImports: All new imports
    :param oldImports: All old imports
    :return: A list of imports as strings.
    """
    notInImports = []
    for i in newImports:
        already = False
        for j in oldImports:
            if i == j:
                already = True
        if not already:
            notInImports.append(i)
    # Merge the minimum imports
    imports = oldImports + notInImports
    return imports


def updateGlobals(newGlobals, oldGlobals):
    """
    Compare the old globals against the new ones and returns the old globals with the new globals that did
    not existed previously
    :param newGlobals: All new globals
    :param oldGlobals: All old globals
    :return: A list of globals as strings.
    """
    if len(oldGlobals) == 0:
        return newGlobals
    else:
        for gName in newGlobals.keys():
            if oldGlobals.has_key(gName) and (not newGlobals[gName] == oldGlobals[gName]):
                print "WARNING! Global variable " + gName + " has been redefined with changes (the previous will be deprecated)."
            oldGlobals[gName] = newGlobals[gName]
        return oldGlobals


def updateClasses(newClasses, oldClasses):
    """
    Compare the old classes against the new ones. This function is essential due to the fact that a jupyter-notebook
    user may rewrite a function and the latest version is the one that needs to be kept.
    :param newClasses: dictionary containing all classes (on its last version)
    :param oldClasses: dictionary containing the existing classes.
    return: dictionary with the merging result (keeping all classes and updating the old ones).
    """
    if len(oldClasses) == 0:
        return newClasses
    else:
        for cName in newClasses.keys():
            if oldClasses.has_key(cName) and (not newClasses[cName] == oldClasses[cName]):
                print "WARNING! Class " + cName + " has been redefined with changes (the previous will be deprecated)."
            oldClasses[cName] = newClasses[cName]
        return oldClasses


def updateFunctions(newFunctions, oldFunctions):
    """
    Compare the old functions against the new ones. This function is essential due to the fact that a jupyter-notebook
    user may rewrite a function and the latest version is the one that needs to be kept.
    :param newFunctions: dictionary containing all functions (on its last version)
    :param oldFunctions: dictionary containing the existing functions.
    return: dictionary with the merging result (keeping all functions and updating the old ones).
    """
    if len(oldFunctions) == 0:
        return newFunctions
    else:
        for fName in newFunctions.keys():
            if oldFunctions.has_key(fName) and (not newFunctions[fName] == oldFunctions[fName]):
                print "WARNING! Function " + fName + " has been redefined with changes (the previous will be deprecated)."
            oldFunctions[fName] = newFunctions[fName]
        return oldFunctions


def updateTasks(newTasks, oldTasks):
    """
    Compare the old tasks against the new ones. This function is essential due to the fact that a jupyter-notebook
    user may rewrite a task and the latest version is the one that needs to be kept.
    :param newTask: new Task code
    :param tasks: existing tasks
    return: dictionary with the merging result.
    """
    newTaskName = newTasks.keys()[0]
    if oldTasks.has_key(newTaskName) and (not newTasks[newTaskName] == oldTasks[newTaskName]):
        print "WARNING! Task " + newTaskName + " has been redefined with changes (the previous will be deprecated)."
    oldTasks[newTaskName] = newTasks[newTaskName]
    return oldTasks

###  * UPDATE FUNCTIONS

def updateCodeFile(newImports, newGlobals, newClasses, newFunctions, newTasks, filePath):
    """
    Writes the results to the code file used by the workers.
    :param newImports: Imports
    :param newCode: Code
    :param filePath: File location
    """
    file = open(filePath, 'w')
    # Write imports
    for i in newImports:
        file.write(i)
    file.write('\n')
    # Write globals separator
    file.write(separators['globals_separator'] + '\n')
    # Write globals
    if len(newGlobals) == 0:
        file.write('\n')
    else:
        for k, v in newGlobals.iteritems():
            for line in v:
                file.write(line)
            file.write('\n')
            file.write('\n')
    # Write classes separator
    file.write(separators['classes_separator'] + '\n')
    # Write classes
    if len(newClasses) == 0:
        file.write('\n')
    else:
        for k, v in newClasses.iteritems():
            for line in v:
                file.write(line)
            file.write('\n')
            file.write('\n')
    # Write functions separator
    file.write(separators['functions_separator'] + '\n')
    # Write functions
    if len(newFunctions) == 0:
        file.write('\n')
    else:
        for k,v in newFunctions.iteritems():
            for line in v:
                file.write(line)
            file.write('\n')
            file.write('\n')
    # Write tasks separator
    file.write(separators['tasks_separator'] + '\n')
    # Write tasks
    if len(newTasks) == 0:
        file.write('\n')
    else:
        for k,v in newTasks.iteritems():
            for line in v:
                file.write(line)
            file.write('\n')
            file.write('\n')
    file.close()
