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
# Class Node transformer for loop tasking
#

class LoopTaskificator(ast.NodeTransformer):
    """
    Node Transformer class to visit all the FOR loops and taskify them if required

    Attributes:
        - cloog_vars : List of cloog variables
        - taskify_loop_level : Depth level of for loop to taskify
        - task_counter_id : Task counter
        - task2headers : Map containing the task name and its header
        - task2func_code : Map containing the task name and its AST code representation
    """

    # Static attribute List of control flow CLooG variables
    _cloog_vars = ["lbp", "ubp", "lbv", "ubv"]

    def __init__(self, taskify_loop_level, task_counter_id, task2headers, task2func_code):
        """
        Initializes the _LoopTasking internal structures
        :param taskify_loop_level: Depth level of for loop to taskify
        :param task_counter_id: Task counter
        :param task2headers: Map containing the task names and their headers
        :param task2func_code: Map containing the task names and their AST code representations
        """

        self.taskify_loop_level = taskify_loop_level
        self.task_counter_id = task_counter_id
        self.task2headers = task2headers
        self.task2func_code = task2func_code

    def get_final_task_counter_id(self):
        """
        Returns the task counter

        :return task_counter_id: task counter
        """

        return self.task_counter_id

    def get_final_task2headers(self):
        """
        Returns the map containing the task names and their headers

        :return task2headers: Map containing the task names and their headers
        """

        return self.task2headers

    def get_final_task2func_code(self):
        """
        Returns the map containing the task names and their AST code representations

        :return task2func_code: Map containing the task names and their AST code representations
        """

        return self.task2func_code

    def visit_For(self, node):
        """
        Checks whether the node is a ast.For instance and it is valid for taskification. If so, creates the
        corresponding taskification task, and modifies the current node with a call to a it

        :param node: For AST node representation
        :return new_node: If the node is valid for taskification, a AST Call node. Otherwise the same node
        """

        # Compute for level
        for_level = LoopTaskificator._get_for_level(node)

        # Taskify for loop if it has the right depth-level
        if for_level == self.taskify_loop_level:
            if __debug__:
                import astor
                from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
                logger.debug("Original Taskified loop:")
                # logger.debug(ast.dump(node))
                logger.debug(astor.to_source(node, pretty_source=PyCOMPSsSourceGen.long_line_ps))

            new_node = self._taskify_loop(node)

            if __debug__:
                import astor
                from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
                logger.debug("New Taskified loop:")
                if isinstance(new_node, list):
                    for internal_op in new_node:
                        logger.debug(astor.to_source(internal_op, pretty_source=PyCOMPSsSourceGen.long_line_ps))
                else:
                    logger.debug(astor.to_source(new_node, pretty_source=PyCOMPSsSourceGen.long_line_ps))
        else:
            new_node = node

        # Process children
        self.generic_visit(node)

        # Return modified node
        return new_node

    @staticmethod
    def _get_for_level(node):
        """
        Returns the number of nested for loops inside the current node

        :param node: Node to evaluate
        :return current_for_level: Number of nested for loops
        """

        # Child recursion
        current_for_level = 0
        for _, value in ast.iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        children_for_level = LoopTaskificator._get_for_level(item)
                        if children_for_level > current_for_level:
                            current_for_level = children_for_level
            elif isinstance(value, ast.AST):
                children_for_level = LoopTaskificator._get_for_level(value)
                if children_for_level > current_for_level:
                    current_for_level = children_for_level

        # Process current node
        if isinstance(node, ast.For):
            current_for_level = current_for_level + 1

        # Return all the outermost loops
        return current_for_level

    def _taskify_loop(self, node):
        """
        Taskifies the loop represented by the given node

        :param node: Node representing the head of the loop
        :return: New taskified node (or list of nodes)
        """

        # Get loop indexes and loop bounds
        # loops_info = {index1:bounds1, index2:bounds2}
        loops_info = LoopTaskificator._get_loop_info(node)
        # if __debug__:
        #     import astor
        #     logger.debug("- Loop information:")
        #     for k, v in loops_info.items():
        #         logger.debug(str(astor.to_source(k)) + " -> " + str(astor.dump_tree(v)))
        #         logger.debug(str(astor.to_source(k)) + " -> " + str(astor.to_source(v)))

        # Get information about subscript accesses on task code
        # Return is of the form: {var_name:[access_expr1, access_expr2,...]}
        subscripts_accesses = self._get_subscript_accesses_info(node)
        # if __debug__:
        #     import astor
        #     logger.debug("- Subscripts Accesses:")
        #     for var_name, values in subscripts_accesses.items():
        #         for a in values:
        #             logger.debug(str(var_name) + ": " + str([str(astor.dump_tree(dim)) for dim in a]))
        #             logger.debug(str(var_name) + ": " + str([str(astor.to_source(dim)) for dim in a]))

        subscripts_info = _SubscriptInformation(loops_info, subscripts_accesses, node)
        # if __debug__:
        #     logger.debug("- Subscripts information:")
        #     logger.debug(subscripts_info)
        #     logger.debug(" -> LBS:")
        #     for k, v in subscripts_info.lbs.items():
        #         logger.debug(str(k) + ": " + str([str(astor.to_source(dim)) for dim in v]))
        #     logger.debug(" -> UBS:")
        #     for k, v in subscripts_info.ubs.items():
        #         logger.debug(str(k) + ": " + str([str(astor.to_source(dim)) for dim in v]))
        #     logger.debug(" -> STEPS:")
        #     for k, v in subscripts_info.steps.items():
        #         logger.debug(str(k) + ": " + str([str(astor.to_source(dim)) for dim in v]))

        # Rebuild loop body with temporary variables
        import copy
        func_node = copy.deepcopy(node)
        rs = _RewriteTaskSubscripts(subscripts_info)
        func_node = rs.visit(func_node)
        registered_vars = rs.get_registered_vars()
        # if __debug__:
        #     import astor
        #     logger.debug("- Re-written task subscripts:")
        #     logger.debug(astor.to_source(func_node))

        # Get accessed variables
        in_vars, inout_vars = self._get_accesed_variables(func_node, loops_info)
        # if __debug__:
        #     import astor
        #     logger.debug("- Detected IN variables:")
        #     logger.debug(in_vars)
        #     logger.debug("- Detected INOUT variables:")
        #     logger.debug(inout_vars)
        #     logger.debug("- Detected Subscript variables:")
        #     for k, v in registered_vars.items():
        #         logger.debug(str(k) + " -> " + str(v))

        # Build task general information
        self.task_counter_id += 1
        task_name = "LT" + str(self.task_counter_id)
        len_var = task_name + "_args_size"
        task_args, in_task_star_args, inout_task_star_args = LoopTaskificator._build_task_args(in_vars,
                                                                                               inout_vars,
                                                                                               registered_vars)
        # if __debug__:
        #     import astor
        #     logger.debug("- Task Args:")
        #     logger.debug(str([str(astor.to_source(ta)) for ta in task_args]))
        #     logger.debug("- IN Star Args:")
        #     logger.debug(str([str(astor.to_source(itsa)) for itsa in in_task_star_args]))
        #     logger.debug("- INOUT Star Args:")
        #     logger.debug(str([str(astor.to_source(iotsa)) for iotsa in inout_task_star_args]))

        # Build task code
        self._build_task_code(task_name, in_vars, inout_vars, task_args, in_task_star_args, inout_task_star_args,
                              len_var, func_node)

        # Build callee
        callee = LoopTaskificator._build_task_callee(task_name, task_args, in_task_star_args, inout_task_star_args,
                                                     len_var, subscripts_info)

        return callee

    @staticmethod
    def _get_loop_info(node):
        """
        Returns the loop index and bounds information

        :param node: Head node to analyze
        :return: A dictionary containing key = AST representation of the loop indexes, and value = AST
        representation of the loop bounds
        """

        loop_info = {}

        # Direct case
        if isinstance(node, ast.For):
            loop_info[node.target] = node.iter

        # Child recursion
        for field, value in ast.iter_fields(node):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            nested_loop_info = LoopTaskificator._get_loop_info(item)
                            # There are no repeated loop indices so we can update dictionary directly
                            loop_info.update(nested_loop_info)
                elif isinstance(value, ast.AST):
                    nested_loop_info = LoopTaskificator._get_loop_info(value)
                    # There are no repeated loop indices so we can update dictionary directly
                    loop_info.update(nested_loop_info)
        return loop_info

    @staticmethod
    def _get_subscript_accesses_info(node):
        """
        Returns the subscripts information
        :param node:  Head node to analyze
        :return: A dictionary containing key = string var name, and value = list of accesses
        """

        # Direct case
        if isinstance(node, ast.Subscript):
            # Process the node for information
            # name : [expr_dim1, expr_dim2]
            name = LoopTaskificator._extract_subscript_name(node)
            dims = LoopTaskificator._extract_subscript_dims(node)

            # Do not look in children because its inside the subscript
            return {name: [dims]}

        # Child recursion
        subscript_info = {}
        for field, value in ast.iter_fields(node):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            nested_subscript_info = LoopTaskificator._get_subscript_accesses_info(item)
                            # Might contain repeated entries, manual merge
                            for k, v in nested_subscript_info.items():
                                new_values = v
                                if k in subscript_info.keys():
                                    # Append value
                                    new_values.extend(subscript_info[k])
                                else:
                                    new_values = v
                                subscript_info[k] = new_values
                elif isinstance(value, ast.AST):
                    nested_subscript_info = LoopTaskificator._get_subscript_accesses_info(value)
                    # Might contain repeated entries, manual merge
                    for k, v in nested_subscript_info.items():
                        new_values = v
                        if k in subscript_info.keys():
                            # Append value
                            new_values.extend(subscript_info[k])
                        else:
                            new_values = v
                        subscript_info[k] = new_values
        return subscript_info

    @staticmethod
    def _extract_subscript_name(node):
        """
        Extracts the subscript variable name

        :param node: Head subscript AST node
        :return: String containing the name of the subscript variable
        """

        # Direct case
        if isinstance(node, ast.Name):
            return node.id

        # Child recursion: Only through subscript value fields
        if isinstance(node, ast.Subscript):
            return LoopTaskificator._extract_subscript_name(node.value)
        else:
            raise Py2PyCOMPSsLoopTaskificatorException("ERROR: Unrecognised type on subscript name extraction")

    @staticmethod
    def _extract_subscript_dims(node):
        """
        Extracts the subscript expressions for each dimension
        :param node: Head subscript AST node
        :return: List containing the access expressions for each dimension of the subscript
        """

        # Direct case
        if isinstance(node, ast.Index):
            return [node]

        # Child recursion
        dims = []
        for field, value in ast.iter_fields(node):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            internal_dims = LoopTaskificator._extract_subscript_dims(item)
                            dims.extend(internal_dims)
                elif isinstance(value, ast.AST):
                    internal_dims = LoopTaskificator._extract_subscript_dims(value)
                    dims.extend(internal_dims)
        return dims

    def _get_accesed_variables(self, node, loops_info):
        """
        Process the directions of the accessed variables considering that they might be calls to existing tasks

        :param node: AST node representing the head of the statement
        :param loops_info: Information about loop variables and bounds (used to exclude loop variables)
        :return fixed_in_vars: List of names of IN variables
        :return fixed_inout_vars: List of names of INOUT variables
        """

        loop_ind_ids = [li.id for li in loops_info.keys()]
        in_vars, inout_vars = self._get_access_vars(node, loop_ind_ids, False)

        # Remove CLooG control variables assignment
        written, readen = LoopTaskificator._get_cloog_vars(node, False, 0)
        for var_name, read_level in readen.items():
            if var_name in written:
                write_level = written[var_name]
                if read_level < write_level:
                    in_vars.append(var_name)
            else:
                in_vars.append(var_name)

        # Fix duplicate variables and directions
        fixed_in_vars = []
        fixed_inout_vars = []
        for iv in in_vars:
            if iv in inout_vars:
                if iv not in fixed_inout_vars:
                    fixed_inout_vars.append(iv)
            else:
                if iv not in fixed_in_vars:
                    fixed_in_vars.append(iv)
        for iov in inout_vars:
            if iov not in fixed_inout_vars:
                fixed_inout_vars.append(iov)

        # Return variables
        return fixed_in_vars, fixed_inout_vars

    def _get_access_vars(self, statement, loop_ind_ids, is_target):
        """
        Returns the accessed variable names within the given expression

        :param statement: AST node representing the head of the statement
        :param loop_ind_ids: Loop variable ids (to exclude loop variables)
        :param is_target: Indicates whether the current node belongs to a target node or not

        :return in_vars: List of names of accessed variables
        :return inout_vars: List of names of INOUT variables
        :raise Py2PyCOMPSsException: For unrecognised types as task arguments
        """

        in_vars = []
        inout_vars = []

        # Direct case
        if isinstance(statement, ast.Name):
            var_name = statement.id
            if var_name not in loop_ind_ids and var_name not in LoopTaskificator._cloog_vars:
                if is_target:
                    inout_vars.append(var_name)
                else:
                    in_vars.append(var_name)
            return in_vars, inout_vars

        # Direct case
        if isinstance(statement, ast.Subscript):
            # Add subscript variable
            subscript_var = self._extract_subscript_name(statement)
            if subscript_var not in loop_ind_ids:
                if is_target:
                    inout_vars.append(subscript_var)
                else:
                    in_vars.append(subscript_var)

            # When processing all the children, add them as read
            is_target = False

        # Direct case
        if isinstance(statement, ast.Call):
            if "id" in statement.func._fields:
                call_name = statement.func.id
            else:
                call_name = None
            # Maybe its a call to a task we want to remove
            if call_name is not None and call_name in self.task2headers.keys():
                # Retrieve task definition arguments and directions
                task_def_arguments = self.task2func_code[call_name].args.args
                from pycompss.util.translators.py2pycompss.components.header_builder import HeaderBuilder
                task_def_args2directions = HeaderBuilder.split_task_header(self.task2headers[call_name])
                # Get callee arguments
                task_call_args = statement.args
                # Process all arguments
                for position, task_def_arg in enumerate(task_def_arguments):
                    call_arg = task_call_args[position]
                    arg_name = task_def_arg.id
                    arg_direction = task_def_args2directions[arg_name]

                    call_names = LoopTaskificator._get_var_names(call_arg, loop_ind_ids)
                    if arg_direction == "IN":
                        in_vars.extend(call_names)
                    elif arg_direction == "INOUT":
                        inout_vars.extend(call_names)
                    else:
                        # OUT VARS ARE ADDED AS INOUT
                        inout_vars.extend(call_names)

                    # If the current callee parameter is a subscript, add its subscripts indexes as in
                    if isinstance(call_arg, ast.Subscript):
                        iv, iov = self._get_access_vars(call_arg.slice, loop_ind_ids, False)
                        in_vars.extend(iv)
                        inout_vars.extend(iov)

                return in_vars, inout_vars

        # Child recursion
        for field, value in ast.iter_fields(statement):
            if field in ["func", "op", "ops", "keywords"] or (isinstance(statement, ast.For) and field == "target"):
                # Skip function names, operation nodes, var_args keywords, and loop indexes
                pass
            else:
                children_are_target = is_target or (field == "targets")
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            iv, iov = self._get_access_vars(item, loop_ind_ids, children_are_target)
                            in_vars.extend(iv)
                            inout_vars.extend(iov)
                elif isinstance(value, ast.AST):
                    iv, iov = self._get_access_vars(value, loop_ind_ids, children_are_target)
                    in_vars.extend(iv)
                    inout_vars.extend(iov)
        return in_vars, inout_vars

    @staticmethod
    def _get_cloog_vars(statement, is_target, for_level):
        written = {}
        readen = {}

        # Base case
        if isinstance(statement, ast.Name):
            var_name = statement.id
            if var_name in LoopTaskificator._cloog_vars:
                if is_target:
                    # We are assigning a CLooG var on depth = for_level
                    written[var_name] = for_level
                else:
                    # We are assigning a CLooG var on depth = for_level
                    readen[var_name] = for_level
            return written, readen

        # Child recursion
        for field, value in ast.iter_fields(statement):
            if isinstance(statement, ast.For) and field == "body":
                for_level = for_level + 1

            if field in ["func", "op", "ops", "keywords"] or (isinstance(statement, ast.For) and field == "target"):
                # Skip function names, operation nodes, var_args keywords, and loop indexes
                pass
            else:
                children_are_target = is_target or (field == "targets")
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            wr, r = LoopTaskificator._get_cloog_vars(item, children_are_target, for_level)
                            for wr_var, wr_depth in wr.items():
                                if wr_var in written.keys():
                                    if wr_depth < written[wr_var]:
                                        written[wr_var] = wr_depth
                                else:
                                    written[wr_var] = wr_depth
                            for r_var, r_depth in r.items():
                                if r_var in readen.keys():
                                    if r_depth < readen[r_var]:
                                        readen[r_var] = r_depth
                                else:
                                    readen[r_var] = r_depth
                elif isinstance(value, ast.AST):
                    wr, r = LoopTaskificator._get_cloog_vars(value, children_are_target, for_level)
                    for wr_var, wr_depth in wr.items():
                        if wr_var in written.keys():
                            if wr_depth < written[wr_var]:
                                written[wr_var] = wr_depth
                        else:
                            written[wr_var] = wr_depth
                    for r_var, r_depth in r.items():
                        if r_var in readen.keys():
                            if r_depth < readen[r_var]:
                                readen[r_var] = r_depth
                        else:
                            readen[r_var] = r_depth
        return written, readen

    @staticmethod
    def _get_var_names(node, loop_ind_ids):
        """
        Returns the variable name of a Subscript or Name AST node

        :param node: Head node of the Subscript/Name statement
        :param loop_ind_ids: List Loop index ids
        :return: String containing the name of the variable
        """

        if isinstance(node, ast.Num):
            return []
        if isinstance(node, ast.Name):
            if node.id not in loop_ind_ids:
                return [node.id]
            return []
        elif isinstance(node, ast.Subscript):
            return LoopTaskificator._get_var_names(node.value, loop_ind_ids)
        elif isinstance(node, ast.BinOp):
            return LoopTaskificator._get_var_names(node.left, loop_ind_ids) + LoopTaskificator._get_var_names(
                node.right, loop_ind_ids)
        else:
            raise Py2PyCOMPSsLoopTaskificatorException(
                "[ERROR] Unrecognised type " + str(type(node)) + " on task argument")

    @staticmethod
    def _build_task_args(in_vars, inout_vars, registered_vars):
        """
        Builds the task arguments

        :param in_vars: List of IN variables (List<String>)
        :param inout_vars: List of INOUT variables (List<String>)
        :param registered_vars: Map of registered subscript variables
        :return: <List,List,List> 3 Lists of task arguments, IN star arguments, and INOUT star arguments
        """
        task_args = []
        in_task_star_args = []
        inout_task_star_args = []
        for var in in_vars + inout_vars:
            var_ast = ast.Name(id=var)
            # Add plain variables to task header
            if var not in registered_vars.keys():
                # Do not repeat variables
                if var not in task_args:
                    task_args.append(var_ast)
            else:
                # Add subscript variables to star_args (as task input or task output)
                if var in in_vars:
                    if var not in in_task_star_args:
                        in_task_star_args.append(var_ast)
                elif var in inout_vars:
                    if var not in inout_task_star_args:
                        inout_task_star_args.append(var_ast)

        return task_args, in_task_star_args, inout_task_star_args

    def _build_task_code(self, task_name, in_vars, inout_vars, task_args, in_task_star_args, inout_task_star_args,
                         len_var, func_node):
        """
        Rebuilds the code to be performed on the task and its header and stores it into the internal structures

        :param task_name: New name for the task
        :param in_vars: List of IN variables (List<String>)
        :param inout_vars: List of INOUT variables (List<String>)
        :param task_args: List of task arguments (List<AST>)
        :param in_task_star_args: List of task star arguments (List<AST>)
        :param inout_task_star_args: List of INOUT task star arguments (List<AST>)
        :param len_var: Name of the length var for star arguments
        :param func_node: Previous function code
        """
        # Build task header
        from pycompss.util.translators.py2pycompss.components.header_builder import HeaderBuilder
        task_header = HeaderBuilder.build_task_header(in_vars,
                                                      [],
                                                      inout_vars,
                                                      [],
                                                      in_task_star_args + inout_task_star_args,
                                                      len_var)

        # Build task body
        func_node = _UntaskCallees(self.task2headers, self.task2func_code).visit(func_node)
        task_body = LoopTaskificator._create_task_body_with_argutils(func_node, in_task_star_args, inout_task_star_args,
                                                                     len_var)

        # Build task complete node
        task_vararg = "args" if len(in_task_star_args + inout_task_star_args) > 0 else None
        new_task = ast.FunctionDef(name=task_name,
                                   args=ast.arguments(args=task_args, vararg=task_vararg, kwarg=None, defaults=[]),
                                   body=task_body,
                                   decorator_list=[])

        # Add information to internal structures
        self.task2func_code[task_name] = new_task
        self.task2headers[task_name] = task_header
        if __debug__:
            from pycompss.util.translators.astor_source_gen.pycompss_source_gen import PyCOMPSsSourceGen
            import astor
            logger.debug("- New Task Header:")
            logger.debug(task_header)
            logger.debug("- New task:")
            logger.debug(astor.to_source(new_task, pretty_source=PyCOMPSsSourceGen.long_line_ps))

    @staticmethod
    def _create_task_body_with_argutils(func_node, in_var_list, inout_var_list, len_var):
        """
        Creates the body of a LoopTasking function by adding the rebuild and flatten calls before and after the loop
        execution

        :param func_node: Node containing the loop execution
        :param in_var_list: List of IN stared variables (List<String>)
        :param inout_var_list: List of INOUT stared variables (List<String>)
        :param len_var: Name of the global length variable
        :return: <List<ast.Node>> Containing the task body representation
        """

        # Construct an ast var list
        in_ast_var_list = []
        for var_name in in_var_list + inout_var_list:
            in_ast_var_list.append(ast.Name(id=var_name))
        out_ast_var_list = []
        for var_name in inout_var_list:
            out_ast_var_list.append(ast.Name(id=var_name))

        # Construct the global length  node
        global_node = ast.Global(names=[len_var])

        # Construct the rebuild arguments node
        rebuild_args_call = ast.Call(func=ast.Attribute(value=ast.Name(id="ArgUtils"), attr="rebuild_args"),
                                     args=[ast.Name(id="args")],
                                     keywords=[],
                                     starargs=None,
                                     kwargs=None)
        rebuild_node = ast.Assign(targets=[ast.Tuple(elts=in_ast_var_list)], value=rebuild_args_call)

        # Construct the flatten variables for return node
        flatten_args_call = ast.Call(func=ast.Attribute(value=ast.Name(id="ArgUtils"), attr="flatten_args"),
                                     args=out_ast_var_list,
                                     keywords=[],
                                     starargs=None,
                                     kwargs=None)
        flatten_node = ast.Return(value=flatten_args_call)

        # Construct the task body
        task_body = [global_node, rebuild_node, func_node, flatten_node]
        return task_body

    @staticmethod
    def _build_task_callee(task_name, task_args, in_task_star_args, inout_task_star_args, len_var, subscripts_info):
        """
        Constructs the complete task callee: chunks subscripts, flats arguments, calls the task, rebuilds arguments,
        and un-chunks subscripts

        :param task_name: Name of the task to be called
        :param task_args: List of task arguments (List<AST>)
        :param in_task_star_args: List of task IN star arguments (List<AST>)
        :param inout_task_star_args: List of task INOUT star arguments (List<AST>)
        :param len_var: Name of the variable used to length the star arguments
        :param subscripts_info: Information about subscript accesses and bounds
        :return: List of nodes representing the full task callee (List<AST>)
        """

        # Replace the current node by a task callee

        # If there are no star_args, plain expression
        if len(in_task_star_args) == 0 and len(inout_task_star_args) == 0:
            # Regular callee expression
            new_node = ast.Expr(value=ast.Call(func=ast.Name(id=task_name),
                                               args=task_args,
                                               keywords=[],
                                               starargs=None,
                                               kwargs=None))
            return new_node

        # ELSE: Build the chunks, plain with ArgUtils, call task, rebuild with ArgUtils, and rebuild the chunks

        new_nodes = []

        # Create nodes for chunk assign and unassign
        chunked_in_task_star_args = []
        chunked_out_task_star_args = []
        unassign_nodes = []
        star_arg_ind = 0
        for var_ast in in_task_star_args + inout_task_star_args:
            orig_subscript_name = var_ast.id
            # New chunk var
            star_arg_name = task_name + "_aux_" + str(star_arg_ind)
            star_arg_ind = star_arg_ind + 1
            star_arg = ast.Name(id=star_arg_name)
            # Store star_arg name
            chunked_in_task_star_args.append(star_arg)
            if var_ast in inout_task_star_args:
                chunked_out_task_star_args.append(star_arg)

            # Build chunk assignation
            list_comp = subscripts_info.get_as_list_comp(orig_subscript_name)
            assign_node = ast.Assign(targets=[star_arg],
                                     value=list_comp)
            # if __debug__:
            #     import astor
            #     logger.debug("- Add chunk statement:")
            #     # logger.debug(astor.dump_tree(assign_node))
            #     logger.debug(astor.to_source(assign_node))
            new_nodes.append(assign_node)

            # Build chunk un-assignation
            unassign_for = subscripts_info.get_as_loop(orig_subscript_name, star_arg_name)
            # if __debug__:
            #     import astor
            #     logger.debug("- Add un-chunk statement:")
            #     logger.debug(astor.to_source(unassign_for))
            unassign_nodes.append(unassign_for)
        # if __debug__:
        #     logger.debug("Chunked IN Star args:")
        #     logger.debug(str([str(astor.to_source(citsa)) for citsa in chunked_in_task_star_args]))
        #     logger.debug("Chunked INOUT Star args:")
        #     logger.debug(str([str(astor.to_source(cotsa)) for cotsa in chunked_in_task_star_args]))

        # Insert ArgUtils instantiation
        argutils_var = ast.Name(id=task_name + "_argutils")
        argutils_node = ast.Assign(targets=[argutils_var],
                                   value=ast.Call(func=ast.Name(id="ArgUtils"),
                                                  args=[],
                                                  keywords=[],
                                                  starargs=None,
                                                  kwargs=None))
        new_nodes.append(argutils_node)
        # logger.debug("ArgUtils Node:")
        # logger.debug(astor.to_source(argutils_node))

        # Insert Global node for length
        global_node = ast.Global(names=[len_var])
        new_nodes.append(global_node)
        # logger.debug("Global Node:")
        # logger.debug(astor.to_source(global_node))

        # Flatten args
        flat_args_var = ast.Name(id=task_name + "_flat_args")
        args_for_call_flat_args = [ast.Num(n=len(chunked_in_task_star_args))]
        args_for_call_flat_args.extend(chunked_in_task_star_args)
        args_for_call_flat_args.extend(chunked_out_task_star_args)
        flat_args_node = ast.Assign(targets=[ast.Tuple(elts=[flat_args_var, ast.Name(id=len_var)])],
                                    value=ast.Call(func=ast.Attribute(value=argutils_var, attr="flatten"),
                                                   args=args_for_call_flat_args,
                                                   keywords=[],
                                                   starargs=None,
                                                   kwargs=None))

        new_nodes.append(flat_args_node)
        # logger.debug("Flat Args Node:")
        # logger.debug(astor.to_source(flat_args_node))

        # Insert task call
        new_args_var = ast.Name(id=task_name + "_new_args")
        task_call_node = ast.Assign(targets=[new_args_var],
                                    value=ast.Call(func=ast.Name(id=task_name),
                                                   args=task_args,
                                                   keywords=[],
                                                   starargs=flat_args_var,
                                                   kwargs=None))
        new_nodes.append(task_call_node)
        # logger.debug("Task Call Node:")
        # logger.debug(astor.to_source(task_call_node))

        # Rebuild chunks from flat new arguments
        if len(chunked_out_task_star_args) > 0:
            rebuild_node = ast.Assign(targets=[ast.Tuple(elts=chunked_out_task_star_args)],
                                      value=ast.Call(func=ast.Attribute(value=argutils_var, attr="rebuild"),
                                                     args=[new_args_var],
                                                     keywords=[],
                                                     starargs=None,
                                                     kwargs=None))
            new_nodes.append(rebuild_node)
            # logger.debug("Rebuild Node:")
            # logger.debug(astor.to_source(rebuild_node))

        # Undo chunks to user subscripts
        new_nodes.extend(unassign_nodes)

        # Assign to function return
        return new_nodes


#
# Class representing subscript accesses information
#

class _SubscriptInformation(object):
    """
    Class containing the expressions for the subscript accesses

    Attributes:
            - loops_info : Information about loop indices and bounds
            - lbs : Expression for minimum lower bound of any dimension of an access to any subscript
            - ubs : Expression for maximum upper bound of any dimension of an access to any subscript
            - steps : Expression for the gcb step size of any dimension of an access to any subscript
    """

    def __init__(self, loops_info, subscript_accesses_info, node):
        """
        Initializes the _RewriteSubscriptToSubscript internal structures.

        :param loops_info: Information about loop variables and bounds
        :param subscript_accesses_info: Information about subscript access expressions
        :param node: Head of the AST For expression
        """

        # if __debug__:
        #     import astor
        #     logger.debug("- PREV Loop information:")
        #     for k, v in loops_info.items():
        #         # logger.debug(str(astor.to_source(k)) + " -> " + str(astor.dump_tree(v)))
        #         logger.debug(str(astor.to_source(k)) + " -> " + str(astor.to_source(v)))

        rcv = _RewriteCloogVars(node)
        fixed_loops_info = {}
        for depth_index, loop_ind_var in enumerate(
                sorted(loops_info.keys(), key=_SubscriptInformation.sort_loop_indexes)):
            loop_bounds = loops_info[loop_ind_var]
            fixed_loop_bounds = rcv.set_level(depth_index).visit(loop_bounds)
            fixed_loops_info[loop_ind_var] = fixed_loop_bounds
        # if __debug__:
        #     import astor
        #     logger.debug("- FIXED Loop information:")
        #     for k, v in fixed_loops_info.items():
        #         # logger.debug(str(astor.to_source(k)) + " -> " + str(astor.dump_tree(v)))
        #         logger.debug(str(astor.to_source(k)) + " -> " + str(astor.to_source(v)))

        # Compute lbs and ubs
        from pycompss.util.translators.py2pycompss.components.calculator import Calculator
        self.subs2glob_lbs, self.subs2glob_ubs = Calculator.compute_lex_bounds(fixed_loops_info,
                                                                               subscript_accesses_info)

        # Fix upper bounds for iteration variables (t_i) that are not inside the task loops
        for subscript_name, ubs in self.subs2glob_ubs.items():
            new_ubs = []
            for dim_expr in ubs:
                new_dim_expr = dim_expr
                if isinstance(dim_expr, ast.Name):
                    if dim_expr.id.startswith("t"):
                        new_dim_expr = ast.BinOp(left=dim_expr, op=ast.Add(), right=ast.Num(n=1))
                new_ubs.append(new_dim_expr)
            self.subs2glob_ubs[subscript_name] = new_ubs

        # if __debug__:
        #     import astor
        #     logger.debug("Registered Global LBS:")
        #     for subscript_name, lbs in self.subs2glob_lbs.items():
        #         logger.debug("Subscript " + str(subscript_name) + " -> " + str(
        #             [str(astor.to_source(dim_expr)) for dim_expr in lbs]))
        #     logger.debug("Registered Global UBS:")
        #     for subscript_name, ubs in self.subs2glob_ubs.items():
        #         logger.debug("Subscript " + str(subscript_name) + " -> " + str(
        #             [str(astor.to_source(dim_expr)) for dim_expr in ubs]))

    def get_chunk_access(self, var_name, current_access_subscript):
        """
        Returns the modified offset access to a given subscript

        :param var_name: Variable name
        :param current_access_subscript: Subscript representing the original access
        :return: Modified access to the subscript according to chunks
        """

        # Extract original subscript accesses for each dimension
        access = []
        while isinstance(current_access_subscript, ast.Subscript):
            access.append(current_access_subscript.slice.value)
            current_access_subscript = current_access_subscript.value
        access = list(reversed(access))

        # Get lower bounds
        access_lbs = self.subs2glob_lbs[var_name]
        dim = len(access_lbs)

        # Create chunk access
        new_chunk_access = None
        for index in range(dim):
            new_index = ast.BinOp(left=access[index],
                                  op=ast.Sub(),
                                  right=access_lbs[index])
            if new_chunk_access is None:
                new_chunk_access = ast.Subscript(value=ast.Name(id=var_name),
                                                 slice=ast.Index(value=new_index))
            else:
                new_chunk_access = ast.Subscript(value=new_chunk_access,
                                                 slice=ast.Index(value=new_index))

        return new_chunk_access

    def get_as_list_comp(self, var_name):
        """
        Returns the chunking expression for the given variable

        :param var_name: Variable to be chunked
        :return: Expression for chunking
        """

        lbs = self.subs2glob_lbs[var_name]
        ubs = self.subs2glob_ubs[var_name]
        dim = len(lbs)

        # Subscript access
        access = None
        for index in range(dim):
            gen_var = "gv" + str(index)
            if access is None:
                access = ast.Subscript(value=ast.Name(id=var_name),
                                       slice=ast.Index(value=ast.Name(id=gen_var)))
            else:
                access = ast.Subscript(value=access,
                                       slice=ast.Index(value=ast.Name(id=gen_var)))

        # Build list comp
        list_comp = None
        for index in reversed(range(dim)):
            generators_list = [ast.comprehension(target=ast.Name(id="gv" + str(index)),
                                                 iter=ast.Call(func=ast.Name(id="range"),
                                                               args=[lbs[index], ubs[index], ast.Num(n=1)],
                                                               keywords=[],
                                                               starargs=None,
                                                               kwargs=None),
                                                 ifs=[])]
            if list_comp is None:
                list_comp = ast.ListComp(elt=access,
                                         generators=generators_list)
            else:
                list_comp = ast.ListComp(elt=list_comp,
                                         generators=generators_list)
        return list_comp

    def get_as_loop(self, orig_var_name, chunk_var_name):
        """
        Returns the for expression required to unchunk the given variable to its original positions

        :param orig_var_name: Original subscript variable name
        :param chunk_var_name: Chunk variable name
        :return: The for expression to un-chunk the chunk variable to its original subscript
        """

        lbs = self.subs2glob_lbs[orig_var_name]
        ubs = self.subs2glob_ubs[orig_var_name]
        dim = len(lbs)

        # Subscript access
        orig_var_access = None
        chunk_var_access = None
        for index in range(dim):
            gen_var = "gv" + str(index)
            if orig_var_access is None:
                orig_var_access = ast.Subscript(value=ast.Name(id=orig_var_name),
                                                slice=ast.Index(value=ast.Name(id=gen_var)))
            else:
                orig_var_access = ast.Subscript(value=orig_var_access,
                                                slice=ast.Index(value=ast.Name(id=gen_var)))
            if chunk_var_access is None:
                chunk_var_access = ast.Subscript(value=ast.Name(id=chunk_var_name),
                                                 slice=ast.Index(value=ast.BinOp(left=ast.Name(id=gen_var),
                                                                                 op=ast.Sub(),
                                                                                 right=lbs[index])))
            else:
                chunk_var_access = ast.Subscript(value=chunk_var_access,
                                                 slice=ast.Index(value=ast.BinOp(left=ast.Name(id=gen_var),
                                                                                 op=ast.Sub(),
                                                                                 right=lbs[index])))

        # Create for structure
        loop = None
        for index in reversed(range(dim)):
            gen_var = "gv" + str(index)
            if loop is None:
                assign_node = ast.Assign(targets=[orig_var_access],
                                         value=chunk_var_access)
                loop = ast.For(target=ast.Name(id=gen_var),
                               iter=ast.Call(func=ast.Name(id="range"),
                                             args=[lbs[index], ubs[index], ast.Num(n=1)],
                                             keywords=[],
                                             starargs=None,
                                             kwargs=None),
                               body=[assign_node],
                               orelse=[])
            else:
                loop = ast.For(target=ast.Name(id=gen_var),
                               iter=ast.Call(func=ast.Name(id="range"),
                                             args=[lbs[index], ubs[index], ast.Num(n=1)],
                                             keywords=[],
                                             starargs=None,
                                             kwargs=None),
                               body=[loop],
                               orelse=[])

        return loop

    @staticmethod
    def sort_loop_indexes(loop_index_ast):
        import astor
        return astor.to_source(loop_index_ast)

    @staticmethod
    def _equal_accesses(plain_access_list, index_access_list):
        # Check that accesses have the same number of dimensions
        if len(plain_access_list) != len(index_access_list):
            return False

        # Check each dimension
        for i, plain_access in enumerate(plain_access_list):
            if str(ast.dump(plain_access)) != str(ast.dump(index_access_list[i].value)):
                return False

        # All dimensions match
        return True


#
# Class Node transformer for CLooG variables
#

class _RewriteCloogVars(ast.NodeTransformer):
    """
    Node Transformer class to visit all the CLooG variable nodes and substitute them by its expression

    Attributes:
            - cloog_var_names : List of CLooG var names to detect
            - cloog_vars : Mapping between set cloog vars and its expressions
    """

    def __init__(self, for_node):
        """
        Initializes the structures to rewrite cloog variables

        :param for_node: Head FOR Ast Node
        """

        # Map containing the expression of set CLooG vars
        # Notice that if a variable is not set inside the loop tasks, it won't appear on this map
        # Map => KV = {level:assign, level:assign, ...}
        self.cloog_vars = self._search_cloog_vars(for_node, 0)

        # if __debug__:
        #     import astor
        #     logger.debug("CLooG Variables:")
        #     for var, assignations in self.cloog_vars.items():
        #         logger.debug("- " + str(var) + ":")
        #         for level, assign in assignations.items():
        #             logger.debug(str(level) + " -> " + str(astor.to_source(assign)))

        # Set current for level to default value
        self.current_for_level = None

    def set_level(self, for_level):
        self.current_for_level = for_level
        return self

    def visit_Name(self, node):
        """
        When found a usage of a CLooG variable, replace by its expression if it has been modified inside the
        task loops. The node remains intact otherwise

        :param node: Node containing usage of a CLooG variable
        :return:
        """

        if node.id in self.cloog_vars.keys():
            # Get assignations per level
            assignations = self.cloog_vars[node.id]
            # Get current for level
            for_level = self.current_for_level
            # Search for the nearest assignation
            found = False
            expr = node
            while for_level >= 0 and not found:
                if for_level in assignations.keys():
                    found = True
                    expr = assignations[for_level]
                for_level = for_level - 1
            # Return the expression of the CLooG variable if it was set
            if found:
                return ast.copy_location(expr, node)

        return node

    def _search_cloog_vars(self, node, for_level):
        """
        Searches a node recursively to found assignations to CLooG variables. When found, it stores its expression

        :param node: Head AST node
        :return: Maping between found CLooG variables and its assign expressions
        """

        cloog_vars = {}

        # Direct case
        if isinstance(node, ast.Assign):
            target_vars = node.targets
            if len(target_vars) == 1:
                target_var = target_vars[0]
                if isinstance(target_var, ast.Name):
                    target_var_name = target_var.id
                    if target_var_name in LoopTaskificator._cloog_vars:
                        # Expression for a CLooG variable
                        cloog_vars[target_var_name] = {for_level: node.value}
            # No need over children
            return cloog_vars

        # Child recursion
        for field, value in ast.iter_fields(node):
            if isinstance(node, ast.For) and field == "body":
                for_level = for_level + 1

            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            cv = self._search_cloog_vars(item, for_level)
                            for var, assignations in cv.items():
                                new_assignations = assignations
                                if var in cloog_vars.keys():
                                    # Internal map (level -> assign) cannot contain repeated values
                                    new_assignations.update(cloog_vars[var])
                                cloog_vars[var] = new_assignations
                elif isinstance(value, ast.AST):
                    cv = self._search_cloog_vars(value, for_level)
                    for var, assignations in cv.items():
                        new_assignations = assignations
                        if var in cloog_vars.keys():
                            # Internal map (level -> assign) cannot contain repeated values
                            new_assignations.update(cloog_vars[var])
                        cloog_vars[var] = new_assignations
        return cloog_vars


#
# Class Node transformer for subscripts to 1D lists
#

class _RewriteTaskSubscripts(ast.NodeTransformer):
    """
    Node Transformer class to visit all the Subscript AST nodes and change them
    by the corresponding access bounds. The performed modifications are stored inside the
    class object so that users can retrieve them when necessary

    Attributes:
            - subscript_info : Subscript information (loop bounds, accesses, etc.)
            - var_counter : Number of replaced variables
            - registered_vars : Dictionary mapping replaced variables and its original subscript name
    """

    def __init__(self, subscript_info):
        """
        Initializes the _RewriteSubscriptToSubscript internal structures.

        :param subscript_info: Information about subscript access (Object of type SubscriptInformation)
        """

        self.subscript_info = subscript_info
        self.var_counter = 1
        self.registered_vars = {}

    def get_registered_vars(self):
        """
        Returns the mapping between detected variables and its original subscript names

        :return registered_vars: Dictionary mapping replaced variables and its original subscript name
        """

        return self.registered_vars

    def visit_Subscript(self, node):
        """
        Modifies the subscript node by a 1D subscript variable and internally stores the relation between
        the new variable and the old subscript

        :param node: Subscript node to process
        :return new_node: New AST representation of a plain variable
        """

        # Retrieve subscript variable name
        subscript_orig_name = LoopTaskificator._extract_subscript_name(node)

        # Check if we need to register a new variable
        if subscript_orig_name not in self.registered_vars.keys():
            chunk_name = self._get_next_var()
            self.registered_vars[subscript_orig_name] = chunk_name

        # In any case, change the way it is accessed in the task
        var_ast = self._get_var_ast(subscript_orig_name, node)
        return ast.copy_location(var_ast, node)

    def _get_next_var(self):
        """
        Returns the next variable name

        :return var_name: Variable name
        """

        # Create new var name
        var_name = "var" + str(self.var_counter)

        # Increase counter for next call
        self.var_counter += 1

        # Return var object
        return var_name

    def _get_var_ast(self, var_name, node):
        """
        Creates an AST subscript representation of the variable var_name

        :param var_name: String containing the name of the variable
        :param node: Node the access that is currently beeing modified
        :return var_ast: AST subscript representation of the given variable
        """

        # Create variable access
        return self.subscript_info.get_chunk_access(var_name, node)


#
# Class Node transformer to change task callees by method callees
#

class _UntaskCallees(ast.NodeTransformer):
    """
    Node Transformer class to visit all the CALL to change task callees by method callees

    Attributes:
        - task2headers : Map containing the task name and its header
        - task2func_code : Map containing the task name and its AST code representation
    """

    def __init__(self, task2headers, task2func_code):
        self.task2headers = task2headers
        self.task2func_code = task2func_code

    def visit_Call(self, node):
        """
        Process the call node to modify the callee with the new task_function parameters

        :param node: Call AST node
        :return new_call: New Call AST node containing the modified task call
        """

        # Task calls can only use ast.Name as function
        if isinstance(node.func, ast.Name):
            task_name = node.func.id
            # Check if the call name is registered as task
            if task_name in self.task2headers.keys():
                func_name = task_name + "_no_task"

                # Create a new function entry if we haven't replaced the task before
                if func_name not in self.task2func_code.keys():
                    # Copy the task code and rename it
                    import copy
                    func_code = copy.deepcopy(self.task2func_code[task_name])
                    func_code.name = func_name

                    # Add the task as a function to write it later
                    self.task2func_code[func_name] = func_code

                # Always modify the callee
                node.func = ast.Name(id=func_name)

        # Return the current node (might have been modified)
        return node


#
# Exception Class
#

class Py2PyCOMPSsLoopTaskificatorException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on Py2PyCOMPSs.translate.LoopTaskificator method.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TESTS
#

class TestLoopTaskificator(unittest.TestCase):
    pass


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
