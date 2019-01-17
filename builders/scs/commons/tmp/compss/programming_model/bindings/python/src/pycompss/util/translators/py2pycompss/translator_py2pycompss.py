#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging
import ast

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# Translator class
#

class Py2PyCOMPSs(object):

    @staticmethod
    def translate(func, par_py_files, output, taskify_loop_level=None):
        """
        Substitutes the given parallel python files into the original
        function code and adds the required PyCOMPSs annotations. The
        result is stored in the given output file

        :param func: Python original function
        :param par_py_files: List of files containing the Python parallelization of each for block in the func_source
        :param output: PyCOMPSs file path
        :param taskify_loop_level: Loop depth to perform taskification (default None)
        :raise Py2PyCOMPSsException:
        """

        if __debug__:
            logger.debug("[Py2PyCOMPSs] Initialize translation")
            logger.debug("[Py2PyCOMPSs]  - Function: " + str(func))
            for par_f in par_py_files:
                logger.debug("[Py2PyCOMPSs]  - File: " + str(par_f))
            logger.debug("[Py2PyCOMPSs]  - Output: " + str(output))

        # Load user function code
        import astor
        func_ast = astor.code_to_ast(func)

        # Initialize output content
        output_imports = []
        task2headers = {}
        task2func_code = {}
        output_loops_code = []
        task_counter_id = 0

        # Process each par_py file
        for par_py in par_py_files:
            # Retrieve file AST
            par_py_ast = astor.code_to_ast.parse_file(par_py)

            # Process ast
            output_code = []
            task2new_name = {}
            task2original_args = {}
            task2new_args = {}
            task2ret_args = {}
            task2vars2subscripts = {}

            for statement in par_py_ast.body:
                if isinstance(statement, ast.Import):
                    from pycompss.util.translators.py2pycompss.components.code_cleaner import CodeCleaner
                    if not CodeCleaner.contains_import_statement(statement, output_imports):
                        output_imports.append(statement)
                elif isinstance(statement, ast.FunctionDef):
                    task_func_name = statement.name
                    # Update name to avoid override between par_py files
                    task_counter_id += 1
                    task_new_name = "S" + str(task_counter_id)
                    task2new_name[task_func_name] = task_new_name

                    # Update task
                    header, code, original_args, new_args, ret_args, new_vars2subscripts = Py2PyCOMPSs._process_task(
                        statement, task_new_name)

                    task2headers[task_new_name] = header
                    task2func_code[task_new_name] = code
                    task2original_args[task_new_name] = original_args
                    task2new_args[task_new_name] = new_args
                    task2ret_args[task_new_name] = ret_args
                    task2vars2subscripts[task_new_name] = new_vars2subscripts
                else:
                    # Generated CLooG code for parallel loop
                    # Check for calls to task methods and replace them. Leave the rest intact
                    rc = _RewriteCallees(task2new_name, task2original_args, task2new_args, task2ret_args,
                                         task2vars2subscripts)
                    new_statement = rc.visit(statement)
                    # Loop tasking
                    if taskify_loop_level is not None and taskify_loop_level > 0:
                        from pycompss.util.translators.py2pycompss.components.loop_taskificator import LoopTaskificator
                        lt = LoopTaskificator(taskify_loop_level, task_counter_id, task2headers, task2func_code)
                        lt_new_statement = lt.visit(new_statement)
                        task_counter_id = lt.get_final_task_counter_id()
                        task2headers = lt.get_final_task2headers()
                        task2func_code = lt.get_final_task2func_code()
                    else:
                        lt_new_statement = new_statement
                    # Store new code
                    output_code.append(lt_new_statement)

            # Store output code
            output_loops_code.append(output_code)

        # Substitute loops code on function code
        loop_index = 0
        new_body = []
        for statement in func_ast.body:
            if isinstance(statement, ast.For):
                # Check the correctness of the number of generated loops
                if loop_index >= len(output_loops_code):
                    raise Py2PyCOMPSsException(
                        "[ERROR] The number of generated parallel FORs is < than the original number of main FORs")
                # Substitute code with all parallel loop statements
                new_body.extend(output_loops_code[loop_index])
                # Add barrier
                barrier = ast.parse("compss_barrier()")
                new_body.append(barrier.body[0])
                # Mark next loop
                loop_index = loop_index + 1
            else:
                # Store the same statement to new body
                new_body.append(statement)
        func_ast.body = new_body
        # Check that we have substituted all loops
        if loop_index != len(output_loops_code):
            raise Py2PyCOMPSsException(
                "[ERROR] The number of generated parallel FORs is > than the original number of main FORs")

        # Remove the parallel decorator
        for decorator in func_ast.decorator_list:
            if isinstance(decorator, ast.Call):
                if decorator.func.id == "parallel":
                    func_ast.decorator_list.remove(decorator)

        # Debug
        # if __debug__:
        #    logger.debug("OUTPUT IMPORTS:")
        #    for oi in output_imports:
        #        logger.debug(ast.dump(oi))
        #    logger.debug("OUTPUT TASKS:")
        #    for task_name, task_code in task2func_code.items():
        #        task_header = task2headers.get(task_name)
        #        logger.debug(task_name)
        #        logger.debug(task_header)
        #        logger.debug(ast.dump(task_code))
        #    logger.debug("OUTPUT CODE:")
        #    logger.debug(ast.dump(func_ast.body))

        # Print content to PyCOMPSs file
        from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
        with open(output, 'w') as f:
            # Write header
            print("# [COMPSs Autoparallel] Begin Autogenerated code", file=f)
            # Write imports
            for oi in set(output_imports):
                print(astor.to_source(oi, pretty_source=PyCOMPSsSourceGen.long_line_ps), file=f)
            # Write default PyCOMPSs imports
            print("from pycompss.api.api import compss_barrier, compss_wait_on, compss_open", file=f)
            print("from pycompss.api.task import task", file=f)
            print("from pycompss.api.parameter import *", file=f)
            if taskify_loop_level is not None and taskify_loop_level > 0:
                print("from pycompss.util.translators.arg_utils.arg_utils import ArgUtils", file=f)
            print("", file=f)
            print("", file=f)
            # Write tasks
            from pycompss.util.translators.py2pycompss.components.code_cleaner import CodeCleaner
            for task_name in CodeCleaner.sort_task_names(task2func_code):
                task_code = task2func_code.get(task_name)
                task_header = task2headers.get(task_name)
                # Print task header if method is still a task
                if task_header is not None:
                    print(task_header, file=f)
                # Add method code
                print(astor.to_source(task_code, pretty_source=PyCOMPSsSourceGen.long_line_ps), file=f)
                print("", file=f)
            # Write function
            print(astor.to_source(func_ast, pretty_source=PyCOMPSsSourceGen.long_line_ps), file=f)
            # Write header
            print("# [COMPSs Autoparallel] End Autogenerated code", file=f)

        if __debug__:
            logger.debug("[Py2PyCOMPSs] End translation")

    @staticmethod
    def _process_task(func, new_name):
        """
        Processes the current function to obtain its task header, its
        PyCOMPSs equivalent function and the callee modification. Renames
        it with the given new_name

        :param func: AST node representing the head of the Python function
        :param new_name: New name for the Python function
        :return task_header: String representing the function task header
        :return new_func: new AST node representing the head of the function
        :return original_args: List of original arguments
        :return new_args: List of new arguments
        :return ret_args: List of return variables
        :return var2subscript: Dictionary containing the mapping of new variables to previous subscripts
        :raise Py2PyCOMPSsException:
        """

        if __debug__:
            import astor
            from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
            logger.debug("Original task definition")
            # logger.debug(ast.dump(func))
            logger.debug(astor.to_source(func, pretty_source=PyCOMPSsSourceGen.long_line_ps))

        # Rename function
        func.name = new_name

        # Rewrite subscripts by plain variables
        rs = _RewriteSubscript()
        new_func = rs.visit(func)
        var2subscript = rs.get_var_subscripts()

        # Process direction of parameters
        from pycompss.util.translators.py2pycompss.components.parameters_processor import ParametersProcessor
        in_vars, out_vars, inout_vars, return_vars = ParametersProcessor.process_parameters(new_func.body[0])
        # if __debug__:
        #     logger.debug("IN variables:")
        #     logger.debug(in_vars)
        #     logger.debug("OUT variables:")
        #     logger.debug(out_vars)
        #     logger.debug("INOUT variables:")
        #     logger.debug(inout_vars)
        #     logger.debug("RETURN variables:")
        #     logger.debug(return_vars)

        # Add non subscript variables to var2subscript
        for var in in_vars + out_vars + inout_vars + return_vars:
            if var not in var2subscript.keys():
                var_ast = ast.Name(id=var)
                var2subscript[var] = var_ast

        # Rewrite function if it has a return
        if len(return_vars) > 0:
            new_func.body[0] = ast.Return(value=new_func.body[0].value)

        # Create new function arguments
        new_args = []
        for var in in_vars + out_vars + inout_vars:
            if var not in new_args:
                var_ast = ast.Name(id=var)
                new_args.append(var_ast)
        original_args = new_func.args.args
        new_func.args.args = new_args

        # Construct task header
        from pycompss.util.translators.py2pycompss.components.header_builder import HeaderBuilder
        task_header = HeaderBuilder.build_task_header(in_vars, out_vars, inout_vars, return_vars, [], None)

        # Return task header and new function
        if __debug__:
            import astor
            from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
            logger.debug("New task definition")
            # logger.debug(ast.dump(new_func))
            # logger.debug(return_vars)
            logger.debug(astor.to_source(new_func, pretty_source=PyCOMPSsSourceGen.long_line_ps))

        return task_header, new_func, original_args, new_args, return_vars, var2subscript


#
# Class Node transformer for subscripts to plain variables
#

class _RewriteSubscript(ast.NodeTransformer):
    """
    Node Transformer class to visit all the Subscript AST nodes and change them
    by a plain variable access. The performed modifications are stored inside the
    class object so that users can retrieve them when necessary

    Attributes:
            - var_counter : Number of replaced variables
            - var2subscript : Dictionary mapping replaced variables by its original expression
    """

    def __init__(self):
        """
        Initialize Rewrite Subscript internal structures
        """

        self.var_counter = 1
        self.var2subscript = {}

    def get_next_var(self):
        """
        Returns the next variable AST node and name

        :return var_ast: New variable AST representation
        :return var_name: New variable name
        """

        # Create new var name
        var_name = "var" + str(self.var_counter)
        var_ast = ast.Name(id=var_name)

        # Increase counter for next call
        self.var_counter += 1

        # Return var object
        return var_ast, var_name

    def get_var_subscripts(self):
        """
        Returns the mapping between detected variables and its subscripts

        :return var2subscript: Map between variable names and its subscripts
        """

        return self.var2subscript

    def visit_Subscript(self, node):
        """
        Modifies the subscript node by a plain variable and internally stores the relation between
        the new variable and the old subscript

        :param node: Subscript node to process
        :return new_node: New AST representation of a plain variable
        """

        var_ast, var_name = self.get_next_var()
        self.var2subscript[var_name] = node
        return ast.copy_location(var_ast, node)


#
# Class Node transformer for tasks' callees
#

class _RewriteCallees(ast.NodeTransformer):
    """
    Node Transformer class to visit all the callees and change them

    Attributes:
            - task2new_name : Dictionary mapping the function variable original and new names
            - task2original_args : Dictionary mapping the function name to its original arguments
            - task2new_args : Dictionary mapping the function name to its new arguments
            - task2ret_vars : Dictionary mapping the function name to its return values
            - task2vars2subscripts : Dictionary mapping the function name to its vars-subscripts dictionary
    """

    def __init__(self, task2new_name, task2original_args, task2new_args, task2ret_vars, task2vars2subscripts):
        """
        Initializes _RewriteCallees internal structures

        :param task2new_name: Dictionary mapping the function variable original and new names
        :param task2original_args: Dictionary mapping the function name to its original arguments
        :param task2new_args: Dictionary mapping the function name to its new arguments
        :param task2ret_vars: Dictionary mapping the function name to its return values
        :param task2vars2subscripts: Dictionary mapping the function name to its vars-subscripts dictionary
        """

        self.task2new_name = task2new_name
        self.task2original_args = task2original_args
        self.task2new_args = task2new_args
        self.task2ret_vars = task2ret_vars
        self.task2vars2subscripts = task2vars2subscripts

    def visit_Call(self, node):
        """
        Process the call node to modify the callee with the new task_function parameters

        :param node: Call AST node
        :return new_call: New Call AST node containing the modified task call
        """

        original_name = node.func.id

        if original_name in self.task2new_name.keys():
            # It is a call to a task, we must replace it by the new callee
            if __debug__:
                import astor
                from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
                logger.debug("Original task call")
                # logger.debug(ast.dump(node))
                logger.debug(astor.to_source(node, pretty_source=PyCOMPSsSourceGen.long_line_ps))

            # Function new name
            new_name = self.task2new_name[original_name]

            # Replace function name
            node.func = ast.Name(id=new_name)

            # Map function arguments to call arguments
            import copy
            func_args = self.task2original_args[new_name]
            func_args2callee_args = {}
            for i in range(len(node.args)):
                func_arg = func_args[i].id
                callee_arg = copy.deepcopy(node.args[i])
                func_args2callee_args[func_arg] = callee_arg

            # Transform function variables to call arguments on all var2subscript
            vars2subscripts = copy.deepcopy(self.task2vars2subscripts[new_name])
            vars2new_subscripts = {}
            for var, subscript in vars2subscripts.items():
                ran = _RewriteArgNames(func_args2callee_args)
                vars2new_subscripts[var] = ran.visit(subscript)

            # if __debug__:
            #    logger.debug("Vars to subscripts:")
            #    for k, v in vars2new_subscripts.items():
            #        logger.debug(str(k) + " -> " + str(ast.dump(v)))

            # Transform all the new arguments into its subscript
            transformed_new_args = []
            new_args = self.task2new_args[new_name]
            for arg in new_args:
                transformed_new_args.append(vars2new_subscripts[arg.id])

            # if __debug__:
            #    logger.debug("New function arguments")
            #    for new_arg in transformed_new_args:
            #        logger.debug(ast.dump(new_arg))

            # Change the function args by the subscript expressions
            node.args = transformed_new_args

            # Transform all the new return variables into its subscript
            transformed_return_vars = []
            return_vars = self.task2ret_vars[new_name]
            for ret_var in return_vars:
                transformed_return_vars.append(vars2new_subscripts[ret_var])

            # if __debug__:
            #    logger.debug("New function return variables")
            #    for ret_var in transformed_return_vars:
            #        logger.debug(ast.dump(ret_var))

            # Change the function call by an assignment if there are return variables
            import copy
            copied_node = copy.deepcopy(node)
            if len(transformed_return_vars) > 0:
                if len(transformed_return_vars) == 1:
                    target = transformed_return_vars[0]
                else:
                    target = ast.Tuple(elts=transformed_return_vars)
                new_node = ast.Assign(targets=[target], value=copied_node)
            else:
                new_node = copied_node

            if __debug__:
                import astor
                from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
                logger.debug("New task call")
                # logger.debug(ast.dump(new_node))
                logger.debug(astor.to_source(new_node, pretty_source=PyCOMPSsSourceGen.long_line_ps))

            return ast.copy_location(new_node, node)
        else:
            # Generic call to a function. No modifications required
            return node


#
# Class Node transformer for Arguments Names
#

class _RewriteArgNames(ast.NodeTransformer):
    """
    Node Transformer class to visit all the Names AST nodes and change them
    by a its new arguments callee

    Attributes:
        - func_args2callee_args : Dictionary mapping the function variable names and its callee expression
    """

    def __init__(self, func_args2callee_args):
        """
        Initializes _RewriteArgNames internal structures

        :param func_args2callee_args: Dictionary mapping the function variable names and its callee expression
        """

        self.func_args2callee_args = func_args2callee_args

    def visit_Name(self, node):
        """
        Rewrites each variable node with the new variable name

        :param node: Variable AST name node
        :return new_node: AST Node representing the new name of the variable
        """

        if node.id in self.func_args2callee_args.keys():
            # Accessed variable is a function parameter
            # Modify it by its callee value
            import copy
            callee_expr = copy.deepcopy(self.func_args2callee_args[node.id])
            return ast.copy_location(callee_expr, node)
        else:
            # Accessed variable is not a parameter. Leave it intact
            return node


#
# Exception Class
#

class Py2PyCOMPSsException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on Py2PyCOMPSs.translate method.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TESTS
#

class TestPy2PyCOMPSs(unittest.TestCase):

    def test_matmul(self):
        # Base variables
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"

        # Insert function file into pythonpath
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        import importlib
        func_name = "matmul"
        test_module = importlib.import_module("pycompss.util.translators.py2pycompss.tests.test1_matmul_func")
        func = getattr(test_module, func_name)

        # Create list of parallel py codes
        src_file0 = tests_path + "/test1_matmul.src.python"
        par_py_files = [src_file0]

        # Output file
        out_file = tests_path + "/test1_matmul.out.pycompss"

        # Translate
        Py2PyCOMPSs.translate(func, par_py_files, out_file)

        # Check file content
        expected_file = tests_path + "/test1_matmul.expected.pycompss"
        try:
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(out_file, 'r') as f:
                out_content = f.read()
            self.assertEqual(out_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase file
            os.remove(out_file)

    def test_matmul_taskified(self):
        # Base variables
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"

        # Insert function file into pythonpath
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        import importlib
        func_name = "matmul"
        test_module = importlib.import_module("pycompss.util.translators.py2pycompss.tests.test2_matmul_taskified_func")
        func = getattr(test_module, func_name)

        # Create list of parallel py codes
        src_file0 = tests_path + "/test2_matmul_taskified.src.python"
        par_py_files = [src_file0]

        # Output file
        out_file = tests_path + "/test2_matmul_taskified.out.pycompss"

        # Translate
        Py2PyCOMPSs.translate(func, par_py_files, out_file, taskify_loop_level=1)

        # Check file content
        expected_file = tests_path + "/test2_matmul_taskified.expected.pycompss"
        try:
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(out_file, 'r') as f:
                out_content = f.read()
            self.assertEqual(out_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase file
            os.remove(out_file)

    def test_multiply(self):
        # Base variables
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"

        # Insert function file into pythonpath
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        import importlib
        func_name = "matmul"
        test_module = importlib.import_module("pycompss.util.translators.py2pycompss.tests.test3_multiply_func")
        func = getattr(test_module, func_name)

        # Create list of parallel py codes
        src_file0 = tests_path + "/test3_multiply.src.python"
        par_py_files = [src_file0]

        # Output file
        out_file = tests_path + "/test3_multiply.out.pycompss"

        # Translate
        Py2PyCOMPSs.translate(func, par_py_files, out_file)

        # Check file content
        expected_file = tests_path + "/test3_multiply.expected.pycompss"
        try:
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(out_file, 'r') as f:
                out_content = f.read()
            self.assertEqual(out_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase file
            os.remove(out_file)

    def test_multiply_taskified(self):
        # Base variables
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"

        # Insert function file into pythonpath
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        import importlib
        func_name = "matmul"
        test_module = importlib.import_module(
            "pycompss.util.translators.py2pycompss.tests.test4_multiply_taskified_func")
        func = getattr(test_module, func_name)

        # Create list of parallel py codes
        src_file0 = tests_path + "/test4_multiply_taskified.src.python"
        par_py_files = [src_file0]

        # Output file
        out_file = tests_path + "/test4_multiply_taskified.out.pycompss"

        # Translate
        Py2PyCOMPSs.translate(func, par_py_files, out_file, taskify_loop_level=1)

        # Check file content
        expected_file = tests_path + "/test4_multiply_taskified.expected.pycompss"
        try:
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(out_file, 'r') as f:
                out_content = f.read()
            self.assertEqual(out_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase file
            os.remove(out_file)


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
