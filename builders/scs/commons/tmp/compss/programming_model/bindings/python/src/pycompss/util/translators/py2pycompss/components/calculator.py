#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import logging
import unittest
import ast
import islpy as isl

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# Calculator class
#

class Calculator(object):

    @staticmethod
    def compute_lex_bounds(loops_info, subscript_accesses_info):
        """
        Computes the lexmin and lexmax of a given set of accesses considering the given bounds

        :param loops_info: Information about loop bounds and indexes
        :param subscript_accesses_info: Map between subscript names and all its access expressions
        :return: Two maps of the form Map<String, List<List<AST>> containing the global lexmin and lexmax expressions
        for all the dimensions of each subscript
        """

        # Per each subscript, store a ISL object representing each of its accesses
        # The global info is common to all subscripts because it refers to the loop bounds
        subscript2isl_per_access, subscript2isl_global_min, subscript2isl_global_max = Calculator._convert_to_isl(
            loops_info, subscript_accesses_info)

        # Per each subscript, compute the lexmin and lexmax of all the dimensions of all its accesses
        # subscript2acclexmin, subscript2acclexmax, original_accesses = Calculator._compute_lex_minmax(
        #    subscript2isl_per_access)

        # Per each subscript, compute global lexmin and lexmax
        subscript2globlexmin, subscript2globlexmax = Calculator._compute_global_lex_minmax(subscript2isl_global_min,
                                                                                           subscript2isl_global_max)

        # Return lists of minimums and maximums of each dimension of each access per each subscript variable
        return subscript2globlexmin, subscript2globlexmax

    @staticmethod
    def _convert_to_isl(loops_info, subscript_accesses_info):
        """
        Creates the ISL Access objects given the loops information and the accesses information

        :param loops_info: Information about loop bounds and indexes
        :param subscript_accesses_info: Map between subscript names and all its access expressions
        :return: A map of the form Map<String, (Integer, AST, List<BasicISLSet>)> containing the number
        of dimensions, the original access and the basic ISL sets of each access of each subscript
        Two maps of the form Map<String, (Int,List<BasicISLSet>)> containing the global min/max ISL sets of each
        subscript
        """

        if __debug__:
            import astor
            logger.debug("- Loop information:")
            for k, v in loops_info.items():
                # logger.debug(str(astor.to_source(k)) + " -> " + str(astor.dump_tree(v)))
                logger.debug(str(astor.to_source(k)) + " -> " + str(astor.to_source(v)))
            logger.debug("- Subscripts Accesses:")
            for var_name, values in subscript_accesses_info.items():
                for a in values:
                    # logger.debug(str(var_name) + ": " + str([str(astor.dump_tree(dim)) for dim in a]))
                    logger.debug(str(var_name) + ": " + str([str(astor.to_source(dim)) for dim in a]))

        # Global loop information (defining the three spaces)
        global_min_isl_builder = _IslSetBuilder()
        global_min_isl_builder.set_variables([loop_ind.id for loop_ind in loops_info.keys()])
        for loop_ind, loop_bounds in loops_info.items():
            loop_ind_varname = loop_ind.id
            global_min_isl_builder.add_constraint(2, loop_ind_varname, loop_bounds.args[0])
            global_min_isl_builder.add_constraint(4, loop_ind_varname, loop_bounds.args[1])
        import copy
        global_max_isl_builder = copy.deepcopy(global_min_isl_builder)
        specific_access_isl_builder = copy.deepcopy(global_min_isl_builder)

        # Per each subscript, store a ISL object representing each of its accesses
        # The global info is common to all subscripts because it refers to the loop bounds
        subscript2isl_per_access = {}
        subscript2isl_global_min = {}
        subscript2isl_global_max = {}
        for subscript_name, subscript_accesses in subscript_accesses_info.items():
            num_dims = len(subscript_accesses[0])
            # Process each access
            accesses_isl_set = []
            mins_isl_set = []
            maxs_isl_set = []
            for access in subscript_accesses:
                # Add a variable per subscript dimension
                specific_access_isl_builder.set_acccess_variables(num_dims)
                global_min_isl_builder.set_global_variables(num_dims)
                global_max_isl_builder.set_global_variables(num_dims)

                # Add a constraint for each subscript dimension
                for dim_id, dim_access_ast in enumerate(access):
                    specific_access_isl_builder.add_access_constraint(dim_id, dim_access_ast.value)
                    global_min_isl_builder.add_global_constraint(2, dim_id, dim_access_ast.value)
                    global_max_isl_builder.add_global_constraint(4, dim_id, dim_access_ast.value)

                # Generate the specific access ISL object and store it
                # if __debug__:
                #     logger.debug("ISL Access Builder:")
                #     logger.debug(str(specific_access_isl_builder))
                isl_access = specific_access_isl_builder.build_isl_set()
                # if __debug__:
                #     logger.debug("ISL Access Object:")
                #     logger.debug(isl_access)
                accesses_isl_set.append(isl_access)

                # Generate the global access ISL object and store it
                min_isl_access = global_min_isl_builder.build_isl_set()
                # if __debug__:
                #     logger.debug("ISL MIN Access Object:")
                #     logger.debug(min_isl_access)
                mins_isl_set.append(min_isl_access)

                # Generate the global access ISL object and store it
                max_isl_access = global_max_isl_builder.build_isl_set()
                # if __debug__:
                #     logger.debug("ISL MAX Access Object:")
                #     logger.debug(max_isl_access)
                maxs_isl_set.append(max_isl_access)

                # Clear specific access information from global object
                specific_access_isl_builder.clear_access_variables()
                specific_access_isl_builder.clear_access_constraints()

                # Clear specific access information from global MIN object
                global_min_isl_builder.clear_global_variables()
                global_min_isl_builder.clear_global_constraints()

                # Clear specific access information from global MAX object
                global_max_isl_builder.clear_global_variables()
                global_max_isl_builder.clear_global_constraints()

            # Store the per access information
            subscript2isl_per_access[subscript_name] = num_dims, subscript_accesses, accesses_isl_set

            # Store the per access information to global MIN
            subscript2isl_global_min[subscript_name] = num_dims, mins_isl_set

            # Store the per access information to global MAX
            subscript2isl_global_max[subscript_name] = num_dims, maxs_isl_set

        # if __debug__:
        #     import astor
        #     logger.debug("Subscript To ISL Access Info:")
        #     for subscript_name, info in subscript2isl_per_access.items():
        #         num_dims, subscript_accesses, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  subscript_accesses -> " + str(
        #             [[str(astor.to_source(dim)) for dim in a] for a in subscript_accesses]))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))
        #     logger.debug("Subscript To ISL Global Min Info:")
        #     for subscript_name, info in subscript2isl_global_min.items():
        #         num_dims, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))
        #     logger.debug("Subscript To ISL Global Max Info:")
        #     for subscript_name, info in subscript2isl_global_max.items():
        #         num_dims, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))

        return subscript2isl_per_access, subscript2isl_global_min, subscript2isl_global_max

    @staticmethod
    def _compute_lex_minmax(subscript2isl_per_access):
        """
        Computes the lexmin and lexmax expressions of all the given subscript accesses

        :param subscript2isl_per_access: A map of the form Map<String, (Integer, AST, List<BasicISLSet>)> containing
        the number of dimensions, the original access and the basic ISL sets of each access of each subscript
        :return: Two maps of the form Map<String, List<List<AST>> containing the lexmin and lexmax expressions for
        all the dimensions of all the accesses of each subscript and one Map of the form
        Map<String,List<AST>> containing the original accesses
        """

        # if __debug__:
        #     logger.debug("LEXMIN / LEXMAX of:")
        #     import astor
        #     for subscript_name, info in subscript2isl_per_access.items():
        #         num_dims, subscript_accesses, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  subscript_accesses -> " + str(
        #             [[str(astor.to_source(dim)) for dim in a] for a in subscript_accesses]))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))

        # Per each subscript, compute the lexmin and lexmax of all the dimensions of all its accesses
        subscript2access_lexmin = {}
        subscript2access_lexmax = {}
        subscript2original_access = {}
        for subscript_name, value in subscript2isl_per_access.items():
            num_dims, original_accesses, accesses_isl_set = value
            # Result lists for minimum and maximum of each access
            access_mins = []
            access_maxs = []

            # Process each subscript access
            for access_isl in accesses_isl_set:
                # Obtain lexmin / lexmax
                lex_min = access_isl.lexmin_pw_multi_aff()
                lex_max = access_isl.lexmax_pw_multi_aff()

                # Process each component (dimension) of the access
                mins_ast = []
                maxs_ast = []
                for dim in range(num_dims):
                    min_dim_str = lex_min.get_pw_aff(dim).__str__()
                    min_dim_expr = Calculator._extract_expr(min_dim_str)
                    min_dim_ast = Calculator._build_ast(min_dim_expr)
                    mins_ast.append(min_dim_ast)

                    max_dim_str = lex_max.get_pw_aff(dim).__str__()
                    max_dim_expr = Calculator._extract_expr(max_dim_str)
                    max_dim_ast = Calculator._build_ast(max_dim_expr)
                    maxs_ast.append(max_dim_ast)

                # Add minimum and maximum of each dimension to the access min/max lists
                access_mins.append(mins_ast)
                access_maxs.append(maxs_ast)

            # Add to subscript 2 min/max map
            subscript2access_lexmin[subscript_name] = access_mins
            subscript2access_lexmax[subscript_name] = access_maxs
            subscript2original_access[subscript_name] = original_accesses

        # Return lists of minimums and maximums of each dimension of each access per each subscript variable
        if __debug__:
            import astor
            # logger.debug("ORIG ACCESS:")
            # for subscript_name, original_accesses in subscript2original_access.items():
            #     logger.debug("- Subscript: " + subscript_name)
            #     for a in original_accesses:
            #         # logger.debug(str([str(astor.dump_tree(dim)) for dim in a]))
            #         logger.debug(str([str(astor.to_source(dim)) for dim in a]))
            logger.debug("LEXMIN:")
            for subscript_name, lbs in subscript2access_lexmin.items():
                logger.debug("- Subscript: " + subscript_name)
                for access_lb in lbs:
                    logger.debug(str([str(astor.to_source(minimum_of_dim)) for minimum_of_dim in access_lb]))
            logger.debug("LEXMAX:")
            for subscript_name, ubs in subscript2access_lexmax.items():
                logger.debug("- Subscript: " + subscript_name)
                for access_ub in ubs:
                    logger.debug(str([str(astor.to_source(minimum_of_dim)) for minimum_of_dim in access_ub]))

        return subscript2access_lexmin, subscript2access_lexmax, subscript2original_access

    @staticmethod
    def _compute_global_lex_minmax(subscript2isl_global_min, subscript2isl_global_max):
        """
        Computes the global lexmin and lexmax expressions of all the given subscript accesses

        :param subscript2isl_global_min: A map of the form Map<String, (Int,List<BasicISLSet>)> containing the global
        min ISL sets of each subscript
        :param subscript2isl_global_max: A map of the form Map<String, (Int,List<BasicISLSet>)> containing the global
        max ISL sets of each subscript
        :return: Two maps of the form Map<String, List<List<AST>> containing the global lexmin and lexmax expressions
        for all the dimensions of each subscript
        """

        # if __debug__:
        #     import astor
        #     logger.debug("Global Lexmin/Lexmax from:")
        #     logger.debug("Subscript To ISL Global Min Info:")
        #     for subscript_name, info in subscript2isl_global_min.items():
        #         num_dims, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))
        #     logger.debug("Subscript To ISL Global Max Info:")
        #     for subscript_name, info in subscript2isl_global_max.items():
        #         num_dims, accesses_isl_set = info
        #         logger.debug("- Subscript " + str(subscript_name))
        #         logger.debug("  num_dims -> " + str(num_dims))
        #         logger.debug("  isl_sets -> " + str(accesses_isl_set))

        # Compute global lexmin
        subscript2global_lexmin = {}
        for subscript_name, value in subscript2isl_global_min.items():
            num_dims, accesses_isl_set = value
            # Unify each subscript access to global space
            isl_gl_min = None
            for access_isl in accesses_isl_set:
                if isl_gl_min is None:
                    isl_gl_min = access_isl
                else:
                    isl_gl_min = isl_gl_min.union(access_isl)

            # Obtain global minimum (minimum of union tmpX >= access_cnstr)
            lex_min = isl_gl_min.lexmin_pw_multi_aff()
            # if __debug__:
            #     logger.debug("ISL Global min Object:")
            #     logger.debug(lex_min)

            # Process each component (dimension) of the access
            global_min_ast = []
            for dim in range(num_dims):
                min_dim_str = lex_min.get_pw_aff(dim).__str__()
                min_dim_expr = Calculator._extract_expr(min_dim_str)
                min_dim_ast = Calculator._build_ast(min_dim_expr)
                global_min_ast.append(min_dim_ast)

            # Store global minimum
            subscript2global_lexmin[subscript_name] = global_min_ast

        # Compute global lexmax
        subscript2global_lexmax = {}
        for subscript_name, value in subscript2isl_global_max.items():
            num_dims, accesses_isl_set = value
            # Process each subscript access
            isl_gl_max = None
            for access_isl in accesses_isl_set:
                if isl_gl_max is None:
                    isl_gl_max = access_isl
                else:
                    isl_gl_max = isl_gl_max.union(access_isl)

            # Obtain global minimum (maximum of union tmpX <= access_cnstr)
            lex_max = isl_gl_max.lexmax_pw_multi_aff()
            # if __debug__:
            #     logger.debug("ISL Global min Object:")
            #     logger.debug(lex_min)

            # Process each component (dimension) of the access
            global_max_ast = []
            for dim in range(num_dims):
                max_dim_str = lex_max.get_pw_aff(dim).__str__()
                max_dim_expr = Calculator._extract_expr(max_dim_str)
                max_dim_ast = Calculator._build_ast(max_dim_expr)
                global_max_ast.append(max_dim_ast)

            # Store global minimum
            subscript2global_lexmax[subscript_name] = global_max_ast

        # Return lists of global minimums and maximums per each subscript variable
        if __debug__:
            import astor
            logger.debug("Registered Global LBS:")
            for subscript_name, lbs in subscript2global_lexmin.items():
                logger.debug("Subscript " + str(subscript_name) + " -> " + str(
                    [str(astor.to_source(dim_expr)) for dim_expr in lbs]))
            logger.debug("Registered Global UBS:")
            for subscript_name, ubs in subscript2global_lexmax.items():
                logger.debug("Subscript " + str(subscript_name) + " -> " + str(
                    [str(astor.to_source(dim_expr)) for dim_expr in ubs]))

        return subscript2global_lexmin, subscript2global_lexmax

    @staticmethod
    def _extract_expr(str_line):
        """
        Extracts the access expression from a full ISL output line

        :param str_line: Full ISL output line
        :return: String containing the access expression
        """

        # The expression is of the form [DOMAIN_VARS] -> { assign1 : cond1 ; assign2 : cond2 }
        # if __debug__:
        #     logger.debug("Extracting from: " + str(str_line))

        # Remove domain vars header
        import re
        str_line = re.findall("{.*\}", str_line)[0][2:-2]

        # Separate the different expressions
        processed_assigns = []
        processed_conditions = []
        str_exprs = str_line.split(";")
        for str_expr in str_exprs:
            if "exists" not in str_expr:
                # Split expression into assignment and condition
                fields = str_expr.split(":")
                str_assign = fields[0].strip()
                str_condition = fields[1].strip() if len(fields) > 1 else None

                # Remove the '[(' and ')]' from the assignment
                str_assign = str_assign[2:-2]
                # Insert multiplication marks in assign
                str_assign = re.sub(r'(\d+)(\w)', r'\1*\2', str_assign)
                # Insert multiplication marks and double = in condition
                if str_condition is not None:
                    str_condition = re.sub(r'(\d+)(\w)', r'\1*\2', str_condition)
                    str_condition = re.sub(r' = ', r' == ', str_condition)
                    str_condition = re.sub(r' mod ', r' % ', str_condition)
                # Store expression
                processed_assigns.append(str_assign)
                processed_conditions.append(str_condition)
            else:
                # TODO: Treat exists expressions
                # Expressions are of the form [val] : exists (e0: cond1 and 2e0 >= LB and 6e0 <= UB)
                # Could be treated by removing the exists clause, the e0 variable and checking that LB <= UB
                # For now, we skip this case
                pass

        # Join all expressions
        full_str = processed_assigns[0]
        for i in range(1, len(processed_assigns)):
            full_str += " if " + processed_conditions[i - 1] + " else " + processed_assigns[i]

        # Return full expression
        # if __debug__:
        #     logger.debug("Extracted: " + str(full_str))

        return full_str

    @staticmethod
    def _build_ast(str_expr):
        """
        Builds an AST node representing the given expression

        :param str_expr: String expression
        :return: AST node representing the given expression
        """

        # Add int cast and math module to floor/ceil operations
        str_expr = str_expr.replace("floor", "int(math.floor")
        str_expr = str_expr.replace("ceil", "int(math.ceil")

        # Add an extra closing braket for the int cast on floor cases
        str_expr = Calculator._add_closing_braket(str_expr, "floor")
        str_expr = Calculator._add_closing_braket(str_expr, "ceil")

        # Parse expression to AST
        ast_translation = ast.parse(str_expr)

        # Remove AST module and expression (get only expression value)
        ast_expr = ast_translation.body[0].value

        return ast_expr

    @staticmethod
    def _add_closing_braket(str_expr, keyword):
        """
        Adds an extra closing braket after each occurrence of the keyword in the str_expr

        :param str_expr: String representing the all expression
        :param keyword:  String representing the keyword
        :return: Modified string containing an extra closing braket after each keyword occurrence
        """

        begin_index = str_expr.find(keyword)
        while begin_index != -1:
            # Move index to the start of the internal expression
            begin_index = begin_index + len(keyword)
            # Calculate the ending of the internal expression
            end_index = Calculator._find_closing_braket(str_expr, begin_index)
            # Add the braket
            str_expr = str_expr[:end_index] + ")" + str_expr[end_index:]
            # Iterate to find next floor call
            begin_index = str_expr.find(keyword, begin_index)

        return str_expr

    @staticmethod
    def _find_closing_braket(expr_str, index_start):
        """
        Finds the closing braket of the expression starting on index_start

        :param expr_str: String representing the all expression
        :param index_start: Starting index
        :return: Index of the closing braket
        """

        index_end = index_start + 1
        parentesis_counter = 1
        while parentesis_counter > 0 and index_end < len(expr_str):
            if expr_str[index_end] == "(":
                parentesis_counter += 1
            elif expr_str[index_end] == ")":
                parentesis_counter -= 1
            index_end += 1

        return index_end


#
# AccessSet class for ISL
#

class _IslSetBuilder:
    """
    Auxiliar Class to build ISL Basic Sets
    """

    def __init__(self):
        """
        Initialization of the AccessSet class
        """
        self.space = []

        self.access_variables = []
        self.global_variables = []
        self.variables = []

        self.access_constraints = []
        self.global_constraints = []
        self.constraints = []

    def set_acccess_variables(self, num_dims):
        """
        Craetes num_dims access variables

        :param num_dims: Number of dimensions of the subscript
        """

        dim_vars = []
        for i in range(num_dims):
            varname = "d" + str(i)
            dim_vars.append(varname)
        self.access_variables = dim_vars

    def set_global_variables(self, num_dims):
        """
        Craetes num_dims global variables

        :param num_dims: Number of dimensions of the subscript
        """

        dim_vars = []
        for i in range(num_dims):
            varname = "g" + str(i)
            dim_vars.append(varname)
        self.global_variables = dim_vars

    def set_variables(self, variables):
        """
        Sets the ISL space variables to the given ones

        :param variables: List of variable names
        """

        self.variables = variables

    def add_access_constraint(self, dim_id, right_ast):
        """
        Adds a new access constraint of the form d[dim_id] = right_ast

        :param dim_id: Constraint variable id
        :param right_ast: AST expression for the assignation
        """

        op, right_exprs = self._process_constraint(0, right_ast)

        left_expr = "d" + str(dim_id)

        # Add all the expressions
        for right_expr in right_exprs:
            expr = left_expr + op + right_expr
            self.access_constraints.append(expr)

    def add_constraint(self, constraint_type, left_expr, right_ast):
        """
        Adds a new bound constraint of the form var =,>,>=,<,<= right_ast

        :param constraint_type: Number for the constraint type
        :param left_expr: String containing the left variable
        :param right_ast: AST expression for the assignation
        """

        op, right_exprs = self._process_constraint(constraint_type, right_ast)

        # Add all the expressions
        for right_expr in right_exprs:
            expr = left_expr + op + right_expr
            self.constraints.append(expr)

    def add_global_constraint(self, constraint_type, dim_id, right_ast):
        """
        Adds a new global constraint of the form tmp[dim_id] =,>,>=,<,<= right_ast

        :param constraint_type: Number for the constraint type
        :param dim_id: Constraint variable id
        :param right_ast: AST expression for the assignation
        """

        op, right_exprs = self._process_constraint(constraint_type, right_ast)

        left_expr = "g" + str(dim_id)

        # Add all the expressions
        for right_expr in right_exprs:
            expr = left_expr + op + right_expr
            self.global_constraints.append(expr)

    def clear_access_variables(self):
        """
        Clears the access variables
        """

        self.access_variables = []

    def clear_global_variables(self):
        """
        Clears the global variables
        """

        self.global_variables = []

    def clear_access_constraints(self):
        """
        Clears the access constraints
        """

        self.access_constraints = []

    def clear_global_constraints(self):
        """
        Clears the access constraints
        """

        self.global_constraints = []

    def build_isl_set(self):
        """
        Builds the ISL set
        :return: The ISL BasicSet represented by the self object
        """

        return isl.BasicSet(self.__str__())

    def __str__(self):
        str_space = ", ".join(self.space)
        str_vars = ", ".join(self.access_variables + self.global_variables + self.variables)
        str_constraints = " and ".join(self.access_constraints + self.global_constraints + self.constraints)

        str_expr = "[" + str_space + "] -> { [" + str_vars + "] : " + str_constraints + "}"

        return str_expr

    def __repr__(self):
        return self.__str__()

    def _process_constraint(self, constraint_type, right_ast):
        """
        Process the constraint to build the space variables, the operand and the right expressions

        :param constraint_type: Number indicating the constraint type
        :param right_ast: AST expression for the assignation
        :return: An string representing the operand and a list of strings representing the right expressions
        """

        # Check operator
        if constraint_type == 0:
            op = " = "
        elif constraint_type == 1:
            op = " > "
        elif constraint_type == 2:
            op = " >= "
        elif constraint_type == 3:
            op = " < "
        elif constraint_type == 4:
            op = " <= "
        else:
            raise Py2PyCOMPSsCalculatorException("ERROR: Unrecognised operand type")

        # Check for undefined accessed variables
        space_vars = self._get_used_space_vars(right_ast)
        for sv in space_vars:
            if sv not in self.space:
                self.space.append(sv)

        # Divide right ast into expressions without min/max
        exprs_ast = _IslSetBuilder._extract_exprs_without_minmax(right_ast)
        import astor
        exprs_str = [astor.to_source(expr) for expr in exprs_ast]

        # Process right ast
        return op, exprs_str

    def _get_used_space_vars(self, node_ast):
        """
        Returns a list containing the used space variables by the given node

        :param node_ast: Head node of the expression
        :return: List of space variable names (String)
        """

        # Base case
        if isinstance(node_ast, ast.Name):
            varname = node_ast.id
            # Mark as space constant if it is not a variable
            if varname not in self.variables and varname not in self.access_variables:
                return [varname]

        # Child recursion
        space_vars = []
        for field, value in ast.iter_fields(node_ast):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            sv = self._get_used_space_vars(item)
                            space_vars.extend(sv)
                elif isinstance(value, ast.AST):
                    sv = self._get_used_space_vars(value)
                    space_vars.extend(sv)
        return space_vars

    @staticmethod
    def _extract_exprs_without_minmax(node_original):
        """
        Divides the given expression in several expressions that do not containg min or max operations

        :param node_original: Head AST node
        :return: List of AST expressions without min and max operations
        """

        # Erase Python cast expressions
        import copy
        node_clean = copy.deepcopy(node_original)
        rpc = _RemovePythonCasts()
        node_clean = rpc.visit(node_clean)
        # if __debug__:
        #     import astor
        #     logger.debug("Clean PythonCasts node:")
        #     # logger.debug(ast.dump(node_clean))
        #     logger.debug(astor.to_source(node_clean))

        # Remove min/max expressions if required
        num_minmax = _IslSetBuilder._count_minmax(node_clean)
        if num_minmax > 0:
            exprs_ast = []
            expr_permutations = [[1 if x & (1 << i) else 0 for i in range(num_minmax)] for x in range(1 << num_minmax)]
            for expr_perm in expr_permutations:
                # Create a new copy from the original expression
                new_node_ast = copy.deepcopy(node_clean)
                # Remove the min max expressions according to the given permutation
                rmm = _RemoveMinMax(expr_perm)
                new_node_ast = rmm.visit(new_node_ast)
                # Add new node to the list of expressions
                # if __debug__:
                #     import astor
                #     logger.debug("Clean min/max node:")
                #     # logger.debug(ast.dump(new_node_ast))
                #     logger.debug(astor.to_source(new_node_ast))
                exprs_ast.append(new_node_ast)
            return exprs_ast

        # AST Node does not contain min/max expressions, return clean node
        return [node_clean]

    @staticmethod
    def _count_minmax(node):
        # Base case
        count = 0
        if isinstance(node, ast.Call):
            call_func = node.func
            if isinstance(call_func, ast.Name):
                func_name = call_func.id
                if func_name == "min" or func_name == "max":
                    count = 1

        # Child recursion
        for field, value in ast.iter_fields(node):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            count += _IslSetBuilder._count_minmax(item)
                elif isinstance(value, ast.AST):
                    count += _IslSetBuilder._count_minmax(value)

        return count


#
# Class Node transformer to remove Python cast expressions
#

class _RemovePythonCasts(ast.NodeTransformer):
    """
    Node Transformer class to remove Python cast expressions from a node

    Attributes:
    """

    def __init__(self):
        """
        Initialize the RemovePythonCasts internal structures
        """
        pass

    def visit_Call(self, node):
        # Keep visiting children
        self.generic_visit(node)

        # Check if it is a min/max function call
        call_func = node.func
        if isinstance(call_func, ast.Name):
            func_name = call_func.id
            if func_name == "int" or func_name == "float":
                # Replace node
                import copy
                op = copy.deepcopy(node.args[0])
                return ast.copy_location(op, node)

        # Check if it is a floor/ceil function call
        if isinstance(call_func, ast.Attribute):
            attr_val = call_func.value
            if isinstance(attr_val, ast.Name):
                module_name = attr_val.id
                func_name = call_func.attr
                if module_name == "math" and (func_name == "floor" or func_name == "ceil"):
                    # Replace node
                    import copy
                    op = ast.Call(func=ast.Name(id=func_name),
                                  args=[copy.deepcopy(node.args[0])],
                                  keywords=[],
                                  starargs=None,
                                  kwargs=None)

                    return ast.copy_location(op, node)

        # No need to modify it
        return node


#
# Class Node transformer to remove Min/Max expressions
#

class _RemoveMinMax(ast.NodeTransformer):
    """
    Node Transformer class to remove min max expressions from a node

    Attributes:
        - minmax_expr_perm : Permutation indicating which side of min/max expression needs to be taken
        - visited_minmax_counter : Counter of visited min/max expressions
    """

    def __init__(self, minmax_expr_perm):
        """
        Initialize the RemoveMinMax internal structures
        """

        self.minmax_expr_perm = minmax_expr_perm
        self.visited_minmax_counter = 0

    def visit_Call(self, node):
        # Check if it is a min/max function call
        call_func = node.func
        if isinstance(call_func, ast.Name):
            func_name = call_func.id
            if func_name == "min" or func_name == "max":
                # Obtain the minmax_index and update counter
                minmax_index = self.minmax_expr_perm[self.visited_minmax_counter]
                self.visited_minmax_counter = self.visited_minmax_counter + 1

                # Replace node
                import copy
                op = copy.deepcopy(node.args[minmax_index])

                # Visit nested min/max
                op = self.visit(op)

                return ast.copy_location(op, node)

        # No need to modify it
        return node


#
# Exception Class
#

class Py2PyCOMPSsCalculatorException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on Py2PyCOMPSs.translate.Calculator class.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TESTS
#

class TestCalculator(unittest.TestCase):

    def test_extract_isl_line(self):
        # Create string for full line
        full_line = "[N, M] -> { [(-6 + M)] : N > 0 and M > 0 }"

        # Parse it
        parsed_line = Calculator._extract_expr(full_line)

        # Check result
        expected_parsed_line = "-6 + M"
        self.assertEqual(parsed_line, expected_parsed_line)

    def test_extract_isl_complex_line(self):
        # Create string for full line
        full_line = "[N, M] -> { [(-6 + M)] : N >= 2 and M >= 6 + 2N; [(-1 + 2N)] : N >= 2 and M >= -3 }"

        # Parse it
        parsed_line = Calculator._extract_expr(full_line)

        # Check result
        expected_parsed_line = "-6 + M if N >= 2 and M >= 6 + 2*N else -1 + 2*N"
        self.assertEqual(parsed_line, expected_parsed_line)

    def test_rebuild(self):
        # Create string for full line
        expression_str = "-1 + 2 * N"

        # Parse it
        expr_ast = Calculator._build_ast(expression_str)

        # Check result
        expected_expr_ast = ast.BinOp(left=ast.Num(n=-1),
                                      op=ast.Add(),
                                      right=ast.BinOp(left=ast.Num(n=2),
                                                      op=ast.Mult(),
                                                      right=ast.Name(id="N", ctx=ast.Load())))
        self.assertEqual(ast.dump(expr_ast), ast.dump(expected_expr_ast))

    def test_extract_exprs(self):
        # Create main AST node
        node_ast = ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                           op=ast.Mult(),
                                                           right=ast.Name(id="t1")),
                                            op=ast.Sub(),
                                            right=ast.Call(func=ast.Name(id="min"), args=[
                                                ast.BinOp(left=ast.Name(id="M"), op=ast.Sub(), right=ast.Num(n=1)),
                                                ast.BinOp(left=ast.Num(n=2), op=ast.Sub(), right=ast.Name(id="t1"))],
                                                           keywords=[],
                                                           starargs=None,
                                                           kwargs=None)),
                             op=ast.Add(),
                             right=ast.Num(n=1))

        # Perform extraction
        exprs = _IslSetBuilder._extract_exprs_without_minmax(node_ast)

        # Check result
        expr0 = ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                        op=ast.Mult(),
                                                        right=ast.Name(id="t1")),
                                         op=ast.Sub(),
                                         right=ast.BinOp(left=ast.Name(id="M"), op=ast.Sub(), right=ast.Num(n=1))),
                          op=ast.Add(),
                          right=ast.Num(n=1))
        expr1 = ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                        op=ast.Mult(),
                                                        right=ast.Name(id="t1")),
                                         op=ast.Sub(),
                                         right=ast.BinOp(left=ast.Num(n=2), op=ast.Sub(), right=ast.Name(id="t1"))),
                          op=ast.Add(),
                          right=ast.Num(n=1))
        self.assertEqual(str(ast.dump(exprs[0])), str(ast.dump(expr0)))
        self.assertEqual(str(ast.dump(exprs[1])), str(ast.dump(expr1)))

    def test_extract_exprs_nested(self):
        # Create main AST node
        node_ast = ast.BinOp(left=ast.Call(func=ast.Name(id='min', ctx=ast.Load()),
                                           args=[ast.Call(func=ast.Name(id='min', ctx=ast.Load()),
                                                          args=[ast.BinOp(left=ast.BinOp(left=ast.Num(n=4),
                                                                                         op=ast.Mult(),
                                                                                         right=ast.Name(id='t2',
                                                                                                        ctx=ast.Load())),
                                                                          op=ast.Add(),
                                                                          right=ast.Num(n=2)),
                                                                ast.BinOp(left=ast.Name(id='t_size', ctx=ast.Load()),
                                                                          op=ast.Sub(),
                                                                          right=ast.Num(n=1))],
                                                          keywords=[],
                                                          starargs=None,
                                                          kwargs=None),
                                                 ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                                                         op=ast.Mult(),
                                                                                         right=ast.Name(id='t1',
                                                                                                        ctx=ast.Load())),
                                                                          op=ast.Sub(),
                                                                          right=ast.BinOp(left=ast.Num(n=2),
                                                                                          op=ast.Mult(),
                                                                                          right=ast.Name(id='t2',
                                                                                                         ctx=ast.Load()))),
                                                           op=ast.Add(), right=ast.Num(n=1))],
                                           keywords=[],
                                           starargs=None,
                                           kwargs=None),
                             op=ast.Add(),
                             right=ast.Num(n=1))

        # Perform extraction
        exprs = _IslSetBuilder._extract_exprs_without_minmax(node_ast)

        # Check result
        expr0 = ast.BinOp(
            left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=4),
                                          op=ast.Mult(),
                                          right=ast.Name(id='t2', ctx=ast.Load())),
                           op=ast.Add(),
                           right=ast.Num(n=2)),
            op=ast.Add(),
            right=ast.Num(n=1))
        expr1 = ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                                       op=ast.Mult(),
                                                                       right=ast.Name(id='t1', ctx=ast.Load())),
                                                        op=ast.Sub(),
                                                        right=ast.BinOp(left=ast.Num(n=2),
                                                                        op=ast.Mult(),
                                                                        right=ast.Name(id='t2', ctx=ast.Load()))),
                                         op=ast.Add(),
                                         right=ast.Num(n=1)),
                          op=ast.Add(),
                          right=ast.Num(n=1))
        expr2 = ast.BinOp(left=ast.BinOp(left=ast.Name(id='t_size', ctx=ast.Load()),
                                         op=ast.Sub(),
                                         right=ast.Num(n=1)),
                          op=ast.Add(),
                          right=ast.Num(n=1))
        expr3 = ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                                       op=ast.Mult(),
                                                                       right=ast.Name(id='t1', ctx=ast.Load())),
                                                        op=ast.Sub(),
                                                        right=ast.BinOp(left=ast.Num(n=2),
                                                                        op=ast.Mult(),
                                                                        right=ast.Name(id='t2', ctx=ast.Load()))),
                                         op=ast.Add(),
                                         right=ast.Num(n=1)),
                          op=ast.Add(),
                          right=ast.Num(n=1))
        self.assertEqual(str(ast.dump(exprs[0])), str(ast.dump(expr0)))
        self.assertEqual(str(ast.dump(exprs[1])), str(ast.dump(expr1)))
        self.assertEqual(str(ast.dump(exprs[2])), str(ast.dump(expr2)))
        self.assertEqual(str(ast.dump(exprs[3])), str(ast.dump(expr3)))

    def test_compute_lex_minmax(self):
        # Create test access sets for 2d matrix mat
        a1 = [ast.Num(n=1), ast.Num(n=1)]
        a2 = [ast.Num(n=2), ast.Num(n=2)]
        a1_isl = isl.BasicSet(
            "[N,M] -> { [d1,d2,t1,t2] : d1 = 2*t1 + 1 and d2 = t2 - 1 and t1 >= 0 and t1 < N and t2 >= 0 and t2 < M }")
        a2_isl = isl.BasicSet(
            "[N,M] -> { [d1,d2,t1,t2] : d1 = t2 - 5 and d2 = t1 - 5 and t1 >= 0 and t1 < N and t2 >= 0 and t2 < M }")
        subscript2isl_access = {"mat": (2, [a1, a2], [a1_isl, a2_isl])}

        # Call calculator lexmin/max
        subscript2lexmin, subscript2lexmax, original = Calculator._compute_lex_minmax(subscript2isl_access)

        # Check result
        self.assertEqual(str(ast.dump(subscript2lexmin["mat"][0][0])), str(ast.dump(ast.Num(n=1))))
        self.assertEqual(str(ast.dump(subscript2lexmin["mat"][0][1])), str(ast.dump(ast.Num(n=-1))))
        self.assertEqual(str(ast.dump(subscript2lexmin["mat"][1][0])), str(ast.dump(ast.Num(n=-5))))
        self.assertEqual(str(ast.dump(subscript2lexmin["mat"][1][1])), str(ast.dump(ast.Num(n=-5))))

        self.assertEqual(str(ast.dump(subscript2lexmax["mat"][0][0])), str(
            ast.dump(ast.BinOp(left=ast.Num(n=-1),
                               op=ast.Add(),
                               right=ast.BinOp(
                                   left=ast.Num(n=2),
                                   op=ast.Mult(),
                                   right=ast.Name(id="N", ctx=ast.Load()))))))
        self.assertEqual(str(ast.dump(subscript2lexmax["mat"][0][1])), str(
            ast.dump(ast.BinOp(left=ast.Num(n=-2),
                               op=ast.Add(),
                               right=ast.Name(id="M", ctx=ast.Load())))))
        self.assertEqual(str(ast.dump(subscript2lexmax["mat"][1][0])), str(
            ast.dump(ast.BinOp(left=ast.Num(n=-6),
                               op=ast.Add(),
                               right=ast.Name(id="M", ctx=ast.Load())))))
        self.assertEqual(str(ast.dump(subscript2lexmax["mat"][1][1])), str(
            ast.dump(ast.BinOp(left=ast.Num(n=-6),
                               op=ast.Add(),
                               right=ast.Name(id="N", ctx=ast.Load())))))

    def test_convert_isl(self):
        # Construct loops information
        bounds1 = ast.Call(func=ast.Name(id="range"),
                           args=[ast.Num(n=0), ast.Name(id="N")],
                           keywords=[],
                           starargs=None,
                           kwargs=None)
        bounds2 = ast.Call(func=ast.Name(id="range"),
                           args=[ast.Num(n=0), ast.Name(id="M")],
                           keywords=[],
                           starargs=None,
                           kwargs=None)
        loops_info = {ast.Name(id="t1"): bounds1, ast.Name(id="t2"): bounds2}

        # Construct accesses info
        access1 = [ast.Index(value=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                            op=ast.Mult(),
                                                            right=ast.Name(id="t1")),
                                             op=ast.Add(),
                                             right=ast.Num(n=1))),
                   ast.Index(value=ast.BinOp(left=ast.Name(id="t2"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=1)))]
        access2 = [ast.Index(value=ast.BinOp(left=ast.Name(id="t2"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=5))),
                   ast.Index(value=ast.BinOp(left=ast.Name(id="t1"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=5)))]
        subscript_accesses_info = {"mat": [access1, access2]}

        # Call calculator convert
        subscript2isl_per_access, subscript2isl_global_min, subscript2isl_global_max = Calculator._convert_to_isl(
            loops_info, subscript_accesses_info)

        # Compute expected accesses
        expected_dims = 2
        a10 = "[M, N] -> { [d0, d1, t2, t1] : t2 = 1 + d1 and 2t1 = -1 + d0 and 0 < d0 <= 1 + 2N and -1 <= d1 < M }"
        a11 = "[N, M] -> { [d0, d1, t1, t2] : 2t1 = -1 + d0 and t2 = 1 + d1 and 0 < d0 <= 1 + 2N and -1 <= d1 < M }"
        a20 = "[M, N] -> { [d0, d1, t2, t1] : t2 = 5 + d0 and t1 = 5 + d1 and -5 <= d0 <= -5 + M and -5 <= d1 <= -5 + N }"
        a21 = "[N, M] -> { [d0, d1, t1, t2] : t1 = 5 + d1 and t2 = 5 + d0 and -5 <= d0 <= -5 + M and -5 <= d1 <= -5 + N }"

        # Check per access result
        self.assertEqual(subscript2isl_per_access["mat"][0], expected_dims)
        for index, dim_acces in enumerate(access1):
            self.assertEqual(str(ast.dump(subscript2isl_per_access["mat"][1][0][index])), str(ast.dump(dim_acces)))
        for index, dim_acces in enumerate(access2):
            self.assertEqual(str(ast.dump(subscript2isl_per_access["mat"][1][1][index])), str(ast.dump(dim_acces)))
        self.assertIn(subscript2isl_per_access["mat"][2][0].__str__(), [a10, a11])
        self.assertIn(subscript2isl_per_access["mat"][2][1].__str__(), [a20, a21])

        # Check global result
        gmin_10_0 = "[M, N] -> { [g0, g1, t2, t1] : 0 <= t2 <= 1 + g1 and t2 <= M and 0 <= t1 <= N and 2t1 < g0 }"
        gmin_10_1 = "[N, M] -> { [g0, g1, t1, t2] : 0 <= t1 <= N and 2t1 < g0 and 0 <= t2 <= 1 + g1 and t2 <= M }"
        gmin_11_0 = "[M, N] -> { [g0, g1, t2, t1] : 0 <= t2 <= 5 + g0 and t2 <= M and 0 <= t1 <= 5 + g1 and t1 <= N }"
        gmin_11_1 = "[N, M] -> { [g0, g1, t1, t2] : 0 <= t1 <= 5 + g1 and t1 <= N and 0 <= t2 <= 5 + g0 and t2 <= M }"
        gmax_10_0 = "[M, N] -> { [g0, g1, t2, t1] : t2 > g1 and 0 <= t2 <= M and 0 <= t1 <= N and 2t1 >= -1 + g0 }"
        gmax_10_1 = "[N, M] -> { [g0, g1, t1, t2] : 0 <= t1 <= N and 2t1 >= -1 + g0 and t2 > g1 and 0 <= t2 <= M }"
        gmax_11_0 = "[M, N] -> { [g0, g1, t2, t1] : t2 >= 5 + g0 and 0 <= t2 <= M and t1 >= 5 + g1 and 0 <= t1 <= N }"
        gmax_11_1 = "[N, M] -> { [g0, g1, t1, t2] : t1 >= 5 + g1 and 0 <= t1 <= N and t2 >= 5 + g0 and 0 <= t2 <= M }"

        self.assertEqual(subscript2isl_global_min["mat"][0], expected_dims)
        self.assertIn(str(subscript2isl_global_min["mat"][1][0]), [gmin_10_0, gmin_10_1])
        self.assertIn(str(subscript2isl_global_min["mat"][1][1]), [gmin_11_0, gmin_11_1])
        self.assertEqual(subscript2isl_global_max["mat"][0], expected_dims)
        self.assertIn(str(subscript2isl_global_max["mat"][1][0]), [gmax_10_0, gmax_10_1])
        self.assertIn(str(subscript2isl_global_max["mat"][1][1]), [gmax_11_0, gmax_11_1])

    def test_full(self):
        # Construct loops information
        bounds1 = ast.Call(func=ast.Name(id="range"),
                           args=[ast.Num(n=0), ast.Name(id="N")],
                           keywords=[],
                           starargs=None,
                           kwargs=None)
        bounds2 = ast.Call(func=ast.Name(id="range"),
                           args=[ast.Num(n=0), ast.Name(id="M")],
                           keywords=[],
                           starargs=None,
                           kwargs=None)
        loops_info = {ast.Name(id="t1"): bounds1, ast.Name(id="t2"): bounds2}

        # Construct accesses info
        access1 = [ast.Index(value=ast.BinOp(left=ast.BinOp(left=ast.Num(n=2),
                                                            op=ast.Mult(),
                                                            right=ast.Name(id="t1")),
                                             op=ast.Add(),
                                             right=ast.Num(n=1))),
                   ast.Index(value=ast.BinOp(left=ast.Name(id="t2"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=1)))]
        access2 = [ast.Index(value=ast.BinOp(left=ast.Name(id="t2"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=5))),
                   ast.Index(value=ast.BinOp(left=ast.Name(id="t1"),
                                             op=ast.Sub(),
                                             right=ast.Num(n=5)))]
        subscript_accesses_info = {"mat": [access1, access2]}

        # Call calculator lexmin/max
        subs2glob_min, subs2glob_max = Calculator.compute_lex_bounds(loops_info, subscript_accesses_info)

        # Check global results
        self.assertEqual(str(ast.dump(subs2glob_min["mat"][0])), str(ast.dump(ast.Num(n=-5))))
        self.assertEqual(str(ast.dump(subs2glob_min["mat"][1])), str(ast.dump(ast.Num(n=-5))))

        a11 = ast.IfExp(test=ast.BoolOp(op=ast.And(), values=[
            ast.Compare(left=ast.Name(id='N', ctx=ast.Load()),
                        ops=[ast.GtE()],
                        comparators=[ast.Num(n=0)]),
            ast.Compare(left=ast.BinOp(left=ast.Num(n=2), op=ast.Mult(), right=ast.Name(id='N', ctx=ast.Load())),
                        ops=[ast.LtE()],
                        comparators=[ast.BinOp(left=ast.Num(n=-7),
                                               op=ast.Add(),
                                               right=ast.Name(id='M', ctx=ast.Load()))])]),
                        body=ast.BinOp(left=ast.Num(n=-5),
                                       op=ast.Add(),
                                       right=ast.Name(id='M', ctx=ast.Load())),
                        orelse=ast.BinOp(left=ast.Num(n=1),
                                         op=ast.Add(),
                                         right=ast.BinOp(left=ast.Num(n=2),
                                                         op=ast.Mult(),
                                                         right=ast.Name(id='N', ctx=ast.Load()))))

        a12 = ast.IfExp(test=ast.BoolOp(op=ast.And(), values=[
            ast.Compare(left=ast.Name(id='N', ctx=ast.Load()),
                        ops=[ast.GtE()],
                        comparators=[ast.Num(n=0)]),
            ast.Compare(left=ast.Num(n=0),
                        ops=[ast.LtE(), ast.LtE()],
                        comparators=[ast.Name(id='M', ctx=ast.Load()),
                                     ast.BinOp(left=ast.Num(n=6),
                                               op=ast.Add(),
                                               right=ast.BinOp(left=ast.Num(n=2),
                                                               op=ast.Mult(),
                                                               right=ast.Name(id='N', ctx=ast.Load())))])]),
                        body=ast.BinOp(left=ast.Num(n=1),
                                       op=ast.Add(),
                                       right=ast.BinOp(left=ast.Num(n=2),
                                                       op=ast.Mult(),
                                                       right=ast.Name(id='N', ctx=ast.Load()))),
                        orelse=ast.BinOp(left=ast.Num(n=-5), op=ast.Add(), right=ast.Name(id='M', ctx=ast.Load())))

        a21 = ast.IfExp(test=ast.BoolOp(op=ast.And(), values=[
            ast.Compare(left=ast.Name(id='N', ctx=ast.Load()),
                        ops=[ast.GtE()],
                        comparators=[ast.Num(n=0)]),
            ast.Compare(left=ast.BinOp(left=ast.Num(n=2), op=ast.Mult(), right=ast.Name(id='N', ctx=ast.Load())),
                        ops=[ast.LtE()],
                        comparators=[ast.BinOp(left=ast.Num(n=-7),
                                               op=ast.Add(),
                                               right=ast.Name(id='M', ctx=ast.Load()))])]),
                        body=ast.BinOp(left=ast.Num(n=-5),
                                       op=ast.Add(),
                                       right=ast.Name(id='N', ctx=ast.Load())),
                        orelse=ast.BinOp(left=ast.Num(n=-1),
                                         op=ast.Add(),
                                         right=ast.Name(id='M', ctx=ast.Load())))

        a22 = ast.IfExp(test=ast.BoolOp(op=ast.And(), values=[
            ast.Compare(left=ast.Name(id='N', ctx=ast.Load()),
                        ops=[ast.GtE()],
                        comparators=[ast.Num(n=0)]),
            ast.Compare(left=ast.Num(n=0),
                        ops=[ast.LtE(), ast.LtE()],
                        comparators=[ast.Name(id='M', ctx=ast.Load()),
                                     ast.BinOp(left=ast.Num(n=6),
                                               op=ast.Add(),
                                               right=ast.BinOp(left=ast.Num(n=2),
                                                               op=ast.Mult(),
                                                               right=ast.Name(id='N', ctx=ast.Load())))])]),
                        body=ast.BinOp(left=ast.Num(n=-1), op=ast.Add(), right=ast.Name(id='M', ctx=ast.Load())),
                        orelse=ast.BinOp(left=ast.Num(n=-5), op=ast.Add(), right=ast.Name(id='N', ctx=ast.Load())))

        self.assertIn(str(ast.dump(subs2glob_max["mat"][0])), [str(ast.dump(a11)), str(ast.dump(a12))])
        self.assertIn(str(ast.dump(subs2glob_max["mat"][1])), [str(ast.dump(a21)), str(ast.dump(a22))])


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
