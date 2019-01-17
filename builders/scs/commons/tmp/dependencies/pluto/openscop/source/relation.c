
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           relation.c                            **
 **-----------------------------------------------------------------**
 **                   First version: 30/04/2008                     **
 **-----------------------------------------------------------------**


*****************************************************************************
* OpenScop: Structures and formats for polyhedral tools to talk together    *
*****************************************************************************
*    ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,                *
*    /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,             *
*   /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\             *
*  |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \            *
*  | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\           *
*  | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\          *
*  | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \         *
*  | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \        *
*  | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \       *
*  | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \      *
*  | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \     *
*  | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \    *
*  | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \   *
*  | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ /   *
*  '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'    *
*                                                                           *
* Copyright (C) 2008 University Paris-Sud 11 and INRIA                      *
*                                                                           *
* (3-clause BSD license)                                                    *
* Redistribution and use in source  and binary forms, with or without       *
* modification, are permitted provided that the following conditions        *
* are met:                                                                  *
*                                                                           *
* 1. Redistributions of source code must retain the above copyright notice, *
*    this list of conditions and the following disclaimer.                  *
* 2. Redistributions in binary form must reproduce the above copyright      *
*    notice, this list of conditions and the following disclaimer in the    *
*    documentation and/or other materials provided with the distribution.   *
* 3. The name of the author may not be used to endorse or promote products  *
*    derived from this software without specific prior written permission.  *
*                                                                           *
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      *
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES *
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   *
* IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          *
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  *
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, *
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     *
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       *
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  *
* THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         *
*                                                                           *
* OpenScop Library, a library to manipulate OpenScop formats and data       *
* structures. Written by:                                                   *
* Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                         *
* Louis-Noel Pouchet <Louis-Noel.pouchet@inria.fr>                          *
*                                                                           *
*****************************************************************************/


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <osl/macros.h>
#include <osl/int.h>
#include <osl/util.h>
#include <osl/vector.h>
#include <osl/strings.h>
#include <osl/names.h>
#include <osl/relation.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_relation_sprint_type function:
 * this function prints the textual type of an osl_relation_t structure into
 * a string, according to the OpenScop specification, and returns that string.
 * \param[in] relation The relation whose type has to be printed.
 * \return A string containing the relation type.
 */
static
char * osl_relation_sprint_type(osl_relation_p relation) {
    char * string = NULL;

    OSL_malloc(string, char *, OSL_MAX_STRING * sizeof(char));
    string[0] = '\0';

    if (relation != NULL) {
        switch (relation->type) {
        case OSL_UNDEFINED: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_UNDEFINED);
            break;
        }
        case OSL_TYPE_CONTEXT: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_CONTEXT);
            break;
        }
        case OSL_TYPE_DOMAIN: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_DOMAIN);
            break;
        }
        case OSL_TYPE_SCATTERING: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_SCATTERING);
            break;
        }
        case OSL_TYPE_READ: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_READ);
            break;
        }
        case OSL_TYPE_WRITE: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_WRITE);
            break;
        }
        case OSL_TYPE_MAY_WRITE: {
            snprintf(string, OSL_MAX_STRING, OSL_STRING_MAY_WRITE);
            break;
        }
        default: {
            OSL_warning("unknown relation type, "
                        "replaced with "OSL_STRING_UNDEFINED);
            snprintf(string, OSL_MAX_STRING, OSL_STRING_UNDEFINED);
        }
        }
    }

    return string;
}


/**
 * osl_relation_print_type function:
 * this function displays the textual type of an osl_relation_t structure into
 * a file (file, possibly stdout), according to the OpenScop specification.
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose type has to be printed.
 */
static
void osl_relation_print_type(FILE * file, osl_relation_p relation) {
    char * string = osl_relation_sprint_type(relation);
    fprintf(file, "%s", string);
    free(string);
}


/**
 * osl_relation_idump function:
 * this function displays a osl_relation_t structure (*relation) into a
 * file (file, possibly stdout) in a way that trends to be understandable.
 * It includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose information has to be printed.
 * \param[in] level    Number of spaces before printing, for each line.
 */
void osl_relation_idump(FILE * file, osl_relation_p relation, int level) {
    int i, j, first = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (relation != NULL) {
        fprintf(file, "+-- osl_relation_t (");
        osl_relation_print_type(file, relation);
        fprintf(file, ", ");
        osl_int_dump_precision(file, relation->precision);
        fprintf(file, ")\n");
    } else {
        fprintf(file, "+-- NULL relation\n");
    }

    while (relation != NULL) {
        if (! first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_relation_t (");
            osl_relation_print_type(file, relation);
            fprintf(file, ", ");
            osl_int_dump_precision(file, relation->precision);
            fprintf(file, ")\n");
        } else
            first = 0;

        // A blank line
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "%d %d %d %d %d %d\n",
                relation->nb_rows,        relation->nb_columns,
                relation->nb_output_dims, relation->nb_input_dims,
                relation->nb_local_dims,  relation->nb_parameters);

        // Display the relation.
        for (i = 0; i < relation->nb_rows; i++) {
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");

            fprintf(file, "[ ");

            for (j = 0; j < relation->nb_columns; j++) {
                osl_int_print(file, relation->precision, relation->m[i][j]);
                fprintf(file, " ");
            }

            fprintf(file, "]\n");
        }

        relation = relation->next;

        // Next line.
        if (relation != NULL) {
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|\n");
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");
            fprintf(file, "V\n");
        }
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_relation_dump function:
 * this function prints the content of a osl_relation_t structure
 * (*relation) into a file (file, possibly stdout).
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose information have to be printed.
 */
void osl_relation_dump(FILE * file, osl_relation_p relation) {
    osl_relation_idump(file, relation, 0);
}


/**
 * osl_relation_expression_element function:
 * this function returns a string containing the printing of a value (e.g.,
 * an iterator with its coefficient or a constant).
 * \param[in]     val       Coefficient or constant value.
 * \param[in]     precision The precision of the value.
 * \param[in,out] first     Pointer to a boolean set to 1 if the current value
 *                          is the first of an expresion, 0 otherwise (maybe
 *                          updated).
 * \param[in]     cst       A boolean set to 1 if the value is a constant,
 *                          0 otherwise.
 * \param[in]     name      String containing the name of the element.
 * \return A string that contains the printing of a value.
 */
static
char * osl_relation_expression_element(osl_int_t val,
                                       int precision, int * first,
                                       int cst, char * name) {
    char * temp, * body, * sval;

    OSL_malloc(temp, char *, OSL_MAX_STRING * sizeof(char));
    OSL_malloc(body, char *, OSL_MAX_STRING * sizeof(char));
    OSL_malloc(sval, char *, OSL_MAX_STRING * sizeof(char));

    body[0] = '\0';
    sval[0] = '\0';

    // statements for the 'normal' processing.
    if (!osl_int_zero(precision, val) && (!cst)) {
        if ((*first) || osl_int_neg(precision, val)) {
            if (osl_int_one(precision, val)) {         // case 1
                sprintf(sval, "%s", name);
            } else {
                if (osl_int_mone(precision, val)) {      // case -1
                    sprintf(sval, "-%s", name);
                } else {                                    // default case
                    osl_int_sprint_txt(sval, precision, val);
                    sprintf(temp, "*%s", name);
                    strcat(sval, temp);
                }
            }
            *first = 0;
        } else {
            if (osl_int_one(precision, val)) {
                sprintf(sval, "+%s", name);
            } else {
                sprintf(sval, "+");
                osl_int_sprint_txt(temp, precision, val);
                strcat(sval, temp);
                sprintf(temp, "*%s", name);
                strcat(sval, temp);
            }
        }
    } else {
        if (cst) {
            if ((osl_int_zero(precision, val) && (*first)) ||
                    (osl_int_neg(precision, val)))
                osl_int_sprint_txt(sval, precision, val);
            if (osl_int_pos(precision, val)) {
                if (!(*first)) {
                    sprintf(sval, "+");
                    osl_int_sprint_txt(temp, precision, val);
                    strcat(sval, temp);
                } else {
                    osl_int_sprint_txt(sval, precision, val);
                }
            }
        }
    }
    free(temp);
    free(body);

    return(sval);
}


/**
 * osl_relation_strings function:
 * this function creates a NULL-terminated array of strings from an
 * osl_names_t structure in such a way that the ith string is the "name"
 * corresponding to the ith column of the constraint matrix.
 * \param[in] relation The relation for which we need an array of names.
 * \param[in] names    The set of names for each element.
 * \return An array of strings with one string per constraint matrix column.
 */
static
char ** osl_relation_strings(osl_relation_p relation, osl_names_p names) {
    char ** strings;
    char temp[OSL_MAX_STRING];
    int i, offset;

    if ((relation == NULL) || (names == NULL)) {
        OSL_debug("no names or relation to build the name array");
        return NULL;
    }

    OSL_malloc(strings, char **, ((size_t)relation->nb_columns + 1)*sizeof(char *));
    strings[relation->nb_columns] = NULL;

    // 1. Equality/inequality marker.
    OSL_strdup(strings[0], "e/i");
    offset = 1;

    // 2. Output dimensions.
    if (osl_relation_is_access(relation)) {
        // The first output dimension is the array name.
        OSL_strdup(strings[offset], "Arr");
        // The other ones are the array dimensions [1]...[n]
        for (i = offset + 1; i < relation->nb_output_dims + offset; i++) {
            sprintf(temp, "[%d]", i - 1);
            OSL_strdup(strings[i], temp);
        }
    } else if ((relation->type == OSL_TYPE_DOMAIN) ||
               (relation->type == OSL_TYPE_CONTEXT)) {
        for (i = offset; i < relation->nb_output_dims + offset; i++) {
            OSL_strdup(strings[i], names->iterators->string[i - offset]);
        }
    } else {
        for (i = offset; i < relation->nb_output_dims + offset; i++) {
            OSL_strdup(strings[i], names->scatt_dims->string[i - offset]);
        }
    }
    offset += relation->nb_output_dims;

    // 3. Input dimensions.
    for (i = offset; i < relation->nb_input_dims + offset; i++)
        OSL_strdup(strings[i], names->iterators->string[i - offset]);
    offset += relation->nb_input_dims;

    // 4. Local dimensions.
    for (i = offset; i < relation->nb_local_dims + offset; i++)
        OSL_strdup(strings[i], names->local_dims->string[i - offset]);
    offset += relation->nb_local_dims;

    // 5. Parameters.
    for (i = offset; i < relation->nb_parameters + offset; i++)
        OSL_strdup(strings[i], names->parameters->string[i - offset]);
    offset += relation->nb_parameters;

    // 6. Scalar.
    OSL_strdup(strings[offset], "1");

    return strings;
}


/**
 * osl_relation_subexpression function:
 * this function returns a string corresponding to an affine (sub-)expression
 * stored at the "row"^th row of the relation pointed by "relation" between
 * the start and stop columns. Optionally it may oppose the whole expression.
 * \param[in] relation A set of linear expressions.
 * \param[in] row     The row corresponding to the expression.
 * \param[in] start   The first column for the expression (inclusive).
 * \param[in] stop    The last column for the expression (inclusive).
 * \param[in] oppose  Boolean set to 1 to negate the expression, 0 otherwise.
 * \param[in] strings Array of textual names of the various elements.
 * \return A string that contains the printing of an affine (sub-)expression.
 */
static
char * osl_relation_subexpression(osl_relation_p relation,
                                  int row, int start, int stop, int oppose,
                                  char ** strings) {
    int i, first = 1, constant;
    char * sval;
    char * sline;

    OSL_malloc(sline, char *, OSL_MAX_STRING * sizeof(char));
    sline[0] = '\0';

    // Create the expression. The constant is a special case.
    for (i = start; i <= stop; i++) {
        if (oppose) {
            osl_int_oppose(relation->precision,
                           &relation->m[row][i], relation->m[row][i]);
        }

        if (i == relation->nb_columns - 1)
            constant = 1;
        else
            constant = 0;

        sval = osl_relation_expression_element(relation->m[row][i],
                                               relation->precision, &first, constant, strings[i]);

        if (oppose) {
            osl_int_oppose(relation->precision,
                           &relation->m[row][i], relation->m[row][i]);
        }
        strcat(sline, sval);
        free(sval);
    }

    return sline;
}


/**
 * osl_relation_expression function:
 * this function returns a string corresponding to an affine expression
 * stored at the "row"^th row of the relation pointed by "relation".
 * \param[in] relation A set of linear expressions.
 * \param[in] row     The row corresponding to the expression.
 * \param[in] strings Array of textual names of the various elements.
 * \return A string that contains the printing of an affine expression.
 */
char * osl_relation_expression(osl_relation_p relation,
                               int row, char ** strings) {

    return osl_relation_subexpression(relation, row,
                                      1, relation->nb_columns - 1, 0,
                                      strings);
}


/**
 * osl_relation_is_simple_output function:
 * this function returns 1 or -1 if a given constraint row of a relation
 * corresponds to an output, 0 otherwise. We call a simple output an equality
 * constraint where exactly one output coefficient is not 0 and is either
 * 1 (in this case the function returns 1) or -1 (in this case the function
 * returns -1).
 * \param[in] relation The relation to test for simple output.
 * \param[in] row      The row corresponding to the constraint to test.
 * \return 1 or -1 if the row is a simple output, 0 otherwise.
 */
static
int osl_relation_is_simple_output(osl_relation_p relation, int row) {
    int i;
    int first = 1;
    int sign  = 0;

    if ((relation == NULL) ||
            (relation->m == NULL) ||
            (relation->nb_output_dims == 0))
        return 0;

    if ((row < 0) || (row > relation->nb_rows))
        OSL_error("the specified row does not exist in the relation");

    // The constraint must be an equality.
    if (!osl_int_zero(relation->precision, relation->m[row][0]))
        return 0;

    // Check the output part has one and only one non-zero +1 or -1 coefficient.
    first = 1;
    for (i = 1; i <= relation->nb_output_dims; i++) {
        if (!osl_int_zero(relation->precision, relation->m[row][i])) {
            if (first)
                first = 0;
            else
                return 0;

            if (osl_int_one(relation->precision, relation->m[row][i]))
                sign = 1;
            else if (osl_int_mone(relation->precision, relation->m[row][i]))
                sign = -1;
            else
                return 0;
        }
    }

    return sign;
}


/**
 * osl_relation_sprint_comment function:
 * this function prints into a string a comment corresponding to a constraint
 * of a relation, according to its type, then it returns this string. This
 * function does not check that printing the comment is possible (i.e., are
 * there enough names ?), hence it is the responsibility of the user to ensure
 * he/she can call this function safely.
 * \param[in] relation The relation for which a comment has to be printed.
 * \param[in] row      The constrain row for which a comment has to be printed.
 * \param[in] strings  Array of textual names of the various elements.
 * \param[in] arrays   Array of textual identifiers of the arrays.
 * \return A string which contains the comment for the row.
 */
static
char * osl_relation_sprint_comment(osl_relation_p relation, int row,
                                   char ** strings, char ** arrays) {
    int sign;
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char * expression;
    char buffer[OSL_MAX_STRING];

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    if ((relation == NULL) || (strings == NULL)) {
        OSL_debug("no relation or names while asked to print a comment");
        return string;
    }

    if ((sign = osl_relation_is_simple_output(relation, row))) {
        // First case : output == expression.

        expression = osl_relation_subexpression(relation, row,
                                                1, relation->nb_output_dims,
                                                sign < 0,
                                                strings);
        snprintf(buffer, OSL_MAX_STRING, "   ## %s", expression);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        free(expression);

        // We don't print the right hand side if it's an array identifier.
        if (!osl_relation_is_access(relation) ||
                osl_int_zero(relation->precision, relation->m[row][1])) {
            expression = osl_relation_subexpression(relation, row,
                                                    relation->nb_output_dims + 1,
                                                    relation->nb_columns - 1,
                                                    sign > 0,
                                                    strings);
            snprintf(buffer, OSL_MAX_STRING, " == %s", expression);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            free(expression);
        } else {
            snprintf(buffer, OSL_MAX_STRING, " == %s",
                     arrays[osl_relation_get_array_id(relation) - 1]);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }
    } else {
        // Second case : general case.

        expression = osl_relation_expression(relation, row, strings);
        snprintf(buffer, OSL_MAX_STRING, "   ## %s", expression);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        free(expression);

        if (osl_int_zero(relation->precision, relation->m[row][0]))
            snprintf(buffer, OSL_MAX_STRING, " == 0");
        else
            snprintf(buffer, OSL_MAX_STRING, " >= 0");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    }

    return string;
}


/**
 * osl_relation_column_string function:
 * this function returns an OpenScop comment string showing all column
 * names. It is designed to nicely fit a constraint matrix that would be
 * printed just below this line.
 * \param[in] relation The relation related to the comment line to build.
 * \param[in] strings  Array of textual names of the various elements.
 * \return A fancy comment string with all the dimension names.
 */
static
char * osl_relation_column_string(osl_relation_p relation, char ** strings) {
    int i, j;
    int index_output_dims;
    int index_input_dims;
    int index_local_dims;
    int index_parameters;
    int index_scalar;
    int space, length, left, right;
    char * scolumn;
    char temp[OSL_MAX_STRING];

    OSL_malloc(scolumn, char *, OSL_MAX_STRING);

    index_output_dims = 1;
    index_input_dims  = index_output_dims + relation->nb_output_dims;
    index_local_dims  = index_input_dims  + relation->nb_input_dims;
    index_parameters  = index_local_dims  + relation->nb_local_dims;
    index_scalar      = index_parameters  + relation->nb_parameters;

    // 1. The comment part.
    sprintf(scolumn, "#");
    for (j = 0; j < (OSL_FMT_LENGTH - 1)/2 - 1; j++)
        strcat(scolumn, " ");

    i = 0;
    while (strings[i] != NULL) {
        space  = OSL_FMT_LENGTH;
        length = (space > (int)strlen(strings[i])) ? (int)strlen(strings[i]) : space;
        right  = (space - length + (OSL_FMT_LENGTH % 2)) / 2;
        left   = space - length - right;

        // 2. Spaces before the name
        for (j = 0; j < left; j++)
            strcat(scolumn, " ");

        // 3. The (abbreviated) name
        for (j = 0; j < length - 1; j++) {
            sprintf(temp, "%c", strings[i][j]);
            strcat(scolumn, temp);
        }
        if (length >= (int)strlen(strings[i]))
            sprintf(temp, "%c", strings[i][j]);
        else
            sprintf(temp, ".");
        strcat(scolumn, temp);

        // 4. Spaces after the name
        for (j = 0; j < right; j++)
            strcat(scolumn, " ");

        i++;
        if ((i == index_output_dims) ||
                (i == index_input_dims)  ||
                (i == index_local_dims)  ||
                (i == index_parameters)  ||
                (i == index_scalar))
            strcat(scolumn, "|");
        else
            strcat(scolumn, " ");
    }
    strcat(scolumn, "\n");

    return scolumn;
}


/**
 * osl_relation_column_string_scoplib function:
 * this function returns an OpenScop comment string showing all column
 * names. It is designed to nicely fit a constraint matrix that would be
 * printed just below this line.
 * \param[in] relation The relation related to the comment line to build.
 * \param[in] strings  Array of textual names of the various elements.
 * \return A fancy comment string with all the dimension names.
 */
static
char * osl_relation_column_string_scoplib(osl_relation_p relation,
        char ** strings) {
    int i, j;
    int index_output_dims;
    int index_input_dims;
    int index_local_dims;
    int index_parameters;
    int index_scalar;
    int space, length, left, right;
    char * scolumn;
    char temp[OSL_MAX_STRING];

    OSL_malloc(scolumn, char *, OSL_MAX_STRING);

    index_output_dims = 1;
    index_input_dims  = index_output_dims + relation->nb_output_dims;
    index_local_dims  = index_input_dims  + relation->nb_input_dims;
    index_parameters  = index_local_dims  + relation->nb_local_dims;
    index_scalar      = index_parameters  + relation->nb_parameters;

    // 1. The comment part.
    sprintf(scolumn, "#");
    for (j = 0; j < (OSL_FMT_LENGTH - 1)/2 - 1; j++)
        strcat(scolumn, " ");

    i = 0;
    while (strings[i] != NULL) {

        if (i == 0 ||
                (relation->type != OSL_TYPE_DOMAIN && i >= index_input_dims) ||
                (relation->type == OSL_TYPE_DOMAIN && i <= index_output_dims) ||
                i >= index_parameters) {
            space  = OSL_FMT_LENGTH;
            length = (space > (int)strlen(strings[i])) ? (int)strlen(strings[i]) : space;
            right  = (space - length + (OSL_FMT_LENGTH % 2)) / 2;
            left   = space - length - right;

            // 2. Spaces before the name
            for (j = 0; j < left; j++)
                strcat(scolumn, " ");

            // 3. The (abbreviated) name
            for (j = 0; j < length - 1; j++) {
                sprintf(temp, "%c", strings[i][j]);
                strcat(scolumn, temp);
            }
            if (length >= (int)strlen(strings[i]))
                sprintf(temp, "%c", strings[i][j]);
            else
                sprintf(temp, ".");
            strcat(scolumn, temp);

            // 4. Spaces after the name
            for (j = 0; j < right; j++)
                strcat(scolumn, " ");

            if ((i == index_output_dims-1) ||
                    (i == index_input_dims-1)  ||
                    (i == index_local_dims-1)  ||
                    (i == index_parameters-1)  ||
                    (i == index_scalar-1))
                strcat(scolumn, "|");
            else
                strcat(scolumn, " ");
        }

        i++;
    }
    strcat(scolumn, "\n");

    return scolumn;
}


/**
 * osl_relation_names function:
 * this function generates as set of names for all the dimensions
 * involved in a given relation.
 * \param[in] relation The relation we have to generate names for.
 * \return A set of generated names for the input relation dimensions.
 */
static
osl_names_p osl_relation_names(osl_relation_p relation) {
    int nb_parameters = OSL_UNDEFINED;
    int nb_iterators  = OSL_UNDEFINED;
    int nb_scattdims  = OSL_UNDEFINED;
    int nb_localdims  = OSL_UNDEFINED;
    int array_id      = OSL_UNDEFINED;

    osl_relation_get_attributes(relation, &nb_parameters, &nb_iterators,
                                &nb_scattdims, &nb_localdims, &array_id);

    return osl_names_generate("P", nb_parameters,
                              "i", nb_iterators,
                              "c", nb_scattdims,
                              "l", nb_localdims,
                              "A", array_id);
}


/**
 * osl_relation_nb_components function:
 * this function returns the number of component in the union of relations
 * provided as parameter.
 * \param[in] relation The input union of relations.
 * \return The number of components in the input union of relations.
 */
int osl_relation_nb_components(osl_relation_p relation) {
    int nb_components = 0;

    while (relation != NULL) {
        nb_components++;
        relation = relation->next;
    }

    return nb_components;
}


/**
 * osl_relation_spprint_polylib function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a string in the extended polylib format, and returns this
 * string. This format is the same as OpenScop's, minus the type.
 * \param[in] relation The relation whose information has to be printed.
 * \param[in] names    The names of the constraint columns for comments.
 * \return A string containing the relation pretty-printing.
 */
char * osl_relation_spprint_polylib(osl_relation_p relation,
                                    osl_names_p names) {
    int i, j;
    int part, nb_parts;
    int generated_names = 0;
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];
    char ** name_array = NULL;
    char * scolumn;
    char * comment;

    if (relation == NULL)
        return osl_util_strdup("# NULL relation\n");

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    // Generates the names for the comments if necessary.
    if (names == NULL) {
        generated_names = 1;
        names = osl_relation_names(relation);
    }

    nb_parts = osl_relation_nb_components(relation);

    if (nb_parts > 1) {
        snprintf(buffer, OSL_MAX_STRING, "# Union with %d parts\n%d\n",
                 nb_parts, nb_parts);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    }

    // Print each part of the union.
    for (part = 1; part <= nb_parts; part++) {
        // Prepare the array of strings for comments.
        name_array = osl_relation_strings(relation, names);

        if (nb_parts > 1) {
            snprintf(buffer, OSL_MAX_STRING, "# Union part No.%d\n", part);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        snprintf(buffer, OSL_MAX_STRING, "%d %d %d %d %d %d\n",
                 relation->nb_rows,        relation->nb_columns,
                 relation->nb_output_dims, relation->nb_input_dims,
                 relation->nb_local_dims,  relation->nb_parameters);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        if (relation->nb_rows > 0) {
            scolumn = osl_relation_column_string(relation, name_array);
            snprintf(buffer, OSL_MAX_STRING, "%s", scolumn);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            free(scolumn);
        }

        for (i = 0; i < relation->nb_rows; i++) {
            for (j = 0; j < relation->nb_columns; j++) {
                osl_int_sprint(buffer, relation->precision, relation->m[i][j]);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                snprintf(buffer, OSL_MAX_STRING, " ");
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
            }

            if (name_array != NULL) {
                comment = osl_relation_sprint_comment(relation, i, name_array,
                                                      names->arrays->string);
                osl_util_safe_strcat(&string, comment, &high_water_mark);
                free(comment);
            }
            snprintf(buffer, OSL_MAX_STRING, "\n");
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        // Free the array of strings.
        if (name_array != NULL) {
            for (i = 0; i < relation->nb_columns; i++)
                free(name_array[i]);
            free(name_array);
        }

        relation = relation->next;
    }

    if (generated_names)
        osl_names_free(names);

    return string;
}


/**
 * osl_relation_spprint_polylib_scoplib function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a string in the extended polylib format, and returns this
 * string.
 * \param[in] relation        The relation whose information has to be printed.
 * \param[in] names           The names of the constraint columns for comments.
 * \param[in] print_nth_part  Print the value of `n' (used for domain union)
 * \param[in] add_fakeiter
 * \return A string containing the relation pretty-printing.
 */
char * osl_relation_spprint_polylib_scoplib(osl_relation_p relation,
        osl_names_p names,
        int print_nth_part,
        int add_fakeiter) {
    int i, j;
    int part, nb_parts;
    int generated_names = 0;
    int is_access_array;
    size_t high_water_mark = OSL_MAX_STRING;
    int start_row; // for removing the first line in the access matrix
    int index_output_dims;
    int index_input_dims;
    int index_params;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];
    char ** name_array = NULL;
    char * scolumn;
    char * comment;

    if (relation == NULL)
        return osl_util_strdup("# NULL relation\n");

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    // Generates the names for the comments if necessary.
    if (names == NULL) {
        generated_names = 1;
        names = osl_relation_names(relation);
    }

    nb_parts = osl_relation_nb_components(relation);
    if (nb_parts > 1) {
        snprintf(buffer, OSL_MAX_STRING, "# Union with %d parts\n%d\n",
                 nb_parts, nb_parts);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    }

    is_access_array = (relation->type == OSL_TYPE_READ ||
                       relation->type == OSL_TYPE_WRITE ? 1 : 0);

    // Print each part of the union.

    for (part = 1; part <= nb_parts; part++) {

        index_output_dims = 1;
        index_input_dims  = index_output_dims + relation->nb_output_dims;
        index_params      = index_input_dims + relation->nb_input_dims;

        // Prepare the array of strings for comments.
        name_array = osl_relation_strings(relation, names);

        if (nb_parts > 1) {
            snprintf(buffer, OSL_MAX_STRING, "# Union part No.%d\n", part);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        if (print_nth_part) {
            snprintf(buffer, OSL_MAX_STRING, "%d\n", part);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        // Don't print the array size for access array
        // (the total size is printed in osl_relation_list_pprint_access_array_scoplib)
        if (!is_access_array) {

            // Print array size
            if (relation->type == OSL_TYPE_DOMAIN) {

                if (add_fakeiter) {

                    snprintf(buffer, OSL_MAX_STRING, "%d %d\n",
                             relation->nb_rows+1, relation->nb_columns -
                             relation->nb_input_dims + 1);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);

                    // add the fakeiter line
                    snprintf(buffer, OSL_MAX_STRING, "   0 ");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    snprintf(buffer, OSL_MAX_STRING, "   1 "); // fakeiter
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);

                    for (i = 0 ; i < relation->nb_parameters ; i++) {
                        snprintf(buffer, OSL_MAX_STRING, "   0 ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    }

                    snprintf(buffer, OSL_MAX_STRING, "    0  ## fakeiter == 0\n");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);

                } else {
                    snprintf(buffer, OSL_MAX_STRING, "%d %d\n",
                             relation->nb_rows, relation->nb_columns -
                             relation->nb_input_dims);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }

            } else { // SCATTERING

                if (add_fakeiter) {
                    snprintf(buffer, OSL_MAX_STRING, "%d %d\n",
                             relation->nb_rows+2, relation->nb_columns -
                             relation->nb_output_dims + 1);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                } else {
                    snprintf(buffer, OSL_MAX_STRING, "%d %d\n",
                             relation->nb_rows, relation->nb_columns -
                             relation->nb_output_dims);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }
            }

            // Print column names in comment
            if (relation->nb_rows > 0) {
                scolumn = osl_relation_column_string_scoplib(relation, name_array);
                snprintf(buffer, OSL_MAX_STRING, "%s", scolumn);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                free(scolumn);
            }

            start_row = 0;

        } else {

            if (relation->nb_rows == 1) // for non array variables
                start_row = 0;
            else // Remove the 'Arr' line
                start_row = 1;
        }

        // Print the array
        for (i = start_row; i < relation->nb_rows; i++) {

            // First column
            if (!is_access_array) {
                // array index name for scoplib
                osl_int_sprint(buffer, relation->precision, relation->m[i][0]);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                snprintf(buffer, OSL_MAX_STRING, " ");
                osl_util_safe_strcat(&string, buffer, &high_water_mark);

            } else {
                // The first column represents the array index name in openscop
                if (i == start_row)
                    osl_int_sprint(buffer, relation->precision,
                                   relation->m[0][relation->nb_columns-1]);
                else
                    snprintf(buffer, OSL_MAX_STRING, "   0 ");

                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                snprintf(buffer, OSL_MAX_STRING, " ");
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
            }

            // Rest of the array
            if (relation->type == OSL_TYPE_DOMAIN) {

                for (j = 1; j < index_input_dims; j++) {
                    osl_int_sprint(buffer, relation->precision, relation->m[i][j]);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    snprintf(buffer, OSL_MAX_STRING, " ");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }

                // Jmp input_dims
                for (j = index_params; j < relation->nb_columns; j++) {
                    osl_int_sprint(buffer, relation->precision, relation->m[i][j]);
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    snprintf(buffer, OSL_MAX_STRING, " ");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }

            } else {

                // Jmp output_dims
                for (j = index_input_dims; j < index_params; j++) {
                    if (is_access_array && relation->nb_rows == 1 &&
                            j == relation->nb_columns-1) {
                        snprintf(buffer, OSL_MAX_STRING, "   0 ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    } else {
                        osl_int_sprint(buffer, relation->precision, relation->m[i][j]);
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                        snprintf(buffer, OSL_MAX_STRING, " ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    }
                }

                if (add_fakeiter) {
                    snprintf(buffer, OSL_MAX_STRING, "   0 ");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }

                for (; j < relation->nb_columns; j++) {
                    if (is_access_array && relation->nb_rows == 1 &&
                            j == relation->nb_columns-1) {
                        snprintf(buffer, OSL_MAX_STRING, "  0 ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    } else {
                        osl_int_sprint(buffer, relation->precision, relation->m[i][j]);
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                        snprintf(buffer, OSL_MAX_STRING, " ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    }
                }
            }

            // equation in comment
            if (name_array != NULL) {
                comment = osl_relation_sprint_comment(relation, i, name_array,
                                                      names->arrays->string);
                osl_util_safe_strcat(&string, comment, &high_water_mark);
                free(comment);
                snprintf(buffer, OSL_MAX_STRING, "\n");
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
            }

            // add the lines in the scattering if we need the fakeiter
            if (relation->nb_rows > 0 && add_fakeiter &&
                    relation->type == OSL_TYPE_SCATTERING) {

                for (i = 0 ; i < 2 ; i++) {
                    for (j = 0; j < relation->nb_columns; j++) {
                        if (j == index_output_dims && i == 0)
                            snprintf(buffer, OSL_MAX_STRING, "   1 "); // fakeiter
                        else
                            snprintf(buffer, OSL_MAX_STRING, "   0 ");
                        osl_util_safe_strcat(&string, buffer, &high_water_mark);
                    }
                    snprintf(buffer, OSL_MAX_STRING, "\n");
                    osl_util_safe_strcat(&string, buffer, &high_water_mark);
                }
            }

        }

        // Free the array of strings.
        if (name_array != NULL) {
            for (i = 0; i < relation->nb_columns; i++)
                free(name_array[i]);
            free(name_array);
        }

        relation = relation->next;
    }

    if (generated_names)
        osl_names_free(names);

    return string;
}


/**
 * osl_relation_spprint function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a string in the OpenScop format, and returns this string.
 * \param[in] relation The relation whose information has to be printed.
 * \param[in] names    The names of the constraint columns for comments.
 * \return A string
 */
char * osl_relation_spprint(osl_relation_p relation, osl_names_p names) {
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char * temp;
    char buffer[OSL_MAX_STRING];
    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    if (osl_relation_nb_components(relation) > 0) {
        temp = osl_relation_sprint_type(relation);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);

        snprintf(buffer, OSL_MAX_STRING, "\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        temp = osl_relation_spprint_polylib(relation, names);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);
    }

    return string;
}


/**
 * osl_relation_spprint_scoplib function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a string in the SCoPLib format, and returns this string.
 * \param[in] relation        The relation whose information has to be printed.
 * \param[in] names           The names of the constraint columns for comments.
 * \param[in] print_nth_part  Print the value of `n' (used for domain union)
 * \param[in] add_fakeiter
 * \return A string
 */
char * osl_relation_spprint_scoplib(osl_relation_p relation, osl_names_p names,
                                    int print_nth_part, int add_fakeiter) {
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char * temp;
    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    if (relation) {
        temp = osl_relation_spprint_polylib_scoplib(relation, names,
                print_nth_part, add_fakeiter);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);
    }

    return string;
}


/**
 * osl_relation_pprint function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose information has to be printed.
 * \param[in] names    The names of the constraint columns for comments.
 */
void osl_relation_pprint(FILE * file, osl_relation_p relation,
                         osl_names_p names) {
    char * string = osl_relation_spprint(relation, names);
    fprintf(file, "%s", string);
    free(string);
}


/**
 * osl_relation_pprint_scoplib function:
 * this function pretty-prints the content of an osl_relation_t structure
 * (*relation) into a file (file, possibly stdout) in the SCoPLibformat.
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose information has to be printed.
 * \param[in] names    The names of the constraint columns for comments.
 * \param[in] print_nth_part
 * \param[in] add_fakeiter
 */
void osl_relation_pprint_scoplib(FILE * file, osl_relation_p relation,
                                 osl_names_p names, int print_nth_part,
                                 int add_fakeiter) {
    char * string = osl_relation_spprint_scoplib(relation, names,
                    print_nth_part, add_fakeiter);
    fprintf(file, "%s", string);
    free(string);
}


/**
 * osl_relation_sprint function:
 * this function prints the content of an osl_relation_t structure
 * (*relation) into a string (returned) in the OpenScop textual format.
 * \param[in] relation  The relation structure to print.
 * \return A string containing the OpenScop dump of the relation structure.
 */
char * osl_relation_sprint(osl_relation_p relation) {

    return osl_relation_spprint(relation, NULL);
}


/**
 * osl_relation_print function:
 * this function prints the content of an osl_relation_t structure
 * (*relation) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file     File where informations are printed.
 * \param[in] relation The relation whose information has to be printed.
 */
void osl_relation_print(FILE * file, osl_relation_p relation) {

    osl_relation_pprint(file, relation, NULL);
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_relation_read_type function:
 * this function reads a textual relation type on the input 'file' or the
 * input string 'str' depending on which one is not NULL (exactly
 * one of them must not be NULL). It returns its integer counterpart.
 * \param[in]     file The file where to read a relation type (if not NULL).
 * \param[in,out] str  The string where to read a relation type (if not NULL).
 *                     This pointer is updated to reflect the read and points
 *                     after the tag in the input string.
 * \return The relation type.
 */
static
int osl_relation_read_type(FILE * file, char ** str) {
    int type;
    osl_strings_p strings;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    if (file != NULL)
        strings = osl_strings_read(file);
    else
        strings = osl_strings_sread(str);

    if (osl_strings_size(strings) > 1) {
        OSL_warning("uninterpreted information (after the relation type)");
    }
    if (osl_strings_size(strings) == 0)
        OSL_error("no relation type");

    if (!strcmp(strings->string[0], OSL_STRING_UNDEFINED)) {
        type = OSL_UNDEFINED;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_CONTEXT)) {
        type = OSL_TYPE_CONTEXT;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_DOMAIN)) {
        type = OSL_TYPE_DOMAIN;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_SCATTERING)) {
        type = OSL_TYPE_SCATTERING;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_READ)) {
        type = OSL_TYPE_READ;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_WRITE)) {
        type = OSL_TYPE_WRITE;
        goto return_type;
    }

    if (!strcmp(strings->string[0], OSL_STRING_MAY_WRITE)) {
        type = OSL_TYPE_MAY_WRITE;
        goto return_type;
    }

    OSL_error("relation type not supported");

return_type:
    osl_strings_free(strings);
    return type;
}


/**
 * osl_relation_pread function ("precision read"):
 * this function reads a relation into a file (foo, posibly stdin) and
 * returns a pointer this relation.
 * \param[in] foo       The input stream.
 * \param[in] precision The precision of the relation elements.
 * \return A pointer to the relation structure that has been read.
 */
osl_relation_p osl_relation_pread(FILE * foo, int precision) {
    int i, j, k, n, read = 0;
    int nb_rows, nb_columns;
    int nb_output_dims, nb_input_dims, nb_local_dims, nb_parameters;
    int nb_union_parts = 1;
    int may_read_nb_union_parts = 1;
    int read_attributes = 1;
    int first = 1;
    int type;
    char * c, s[OSL_MAX_STRING], str[OSL_MAX_STRING], *tmp;
    osl_relation_p relation, relation_union = NULL, previous = NULL;

    type = osl_relation_read_type(foo, NULL);

    // Read each part of the union (the number of parts may be updated inside)
    for (k = 0; k < nb_union_parts; k++) {
        // Read the number of union parts or the attributes of the union part
        while (read_attributes) {
            read_attributes = 0;

            // Read relation attributes.
            c = osl_util_skip_blank_and_comments(foo, s);
            read = sscanf(c, " %d %d %d %d %d %d", &nb_rows, &nb_columns,
                          &nb_output_dims, &nb_input_dims,
                          &nb_local_dims, &nb_parameters);

            if (((read != 1) && (read != 6)) ||
                    ((read == 1) && (may_read_nb_union_parts != 1)))
                OSL_error("not 1 or 6 integers on the first relation line");

            if (read == 1) {
                // Only one number means a union and is the number of parts.
                nb_union_parts = nb_rows;
                if (nb_union_parts < 1)
                    OSL_error("negative nb of union parts");

                // Allow to read the properties of the first part of the union.
                read_attributes = 1;
            }

            may_read_nb_union_parts = 0;
        }

        // Allocate the union part and fill its properties.
        relation = osl_relation_pmalloc(precision, nb_rows, nb_columns);
        relation->type           = type;
        relation->nb_output_dims = nb_output_dims;
        relation->nb_input_dims  = nb_input_dims;
        relation->nb_local_dims  = nb_local_dims;
        relation->nb_parameters  = nb_parameters;

        // Read the matrix of constraints.
        for (i = 0; i < relation->nb_rows; i++) {
            c = osl_util_skip_blank_and_comments(foo, s);
            if (c == NULL)
                OSL_error("not enough rows");

            for (j = 0; j < relation->nb_columns; j++) {
                if (c == NULL || *c == '#' || *c == '\n')
                    OSL_error("not enough columns");
                if (sscanf(c, "%s%n", str, &n) == 0)
                    OSL_error("not enough rows");

                // TODO: remove this tmp (sread updates the pointer).
                tmp = str;
                osl_int_sread(&tmp, precision, &relation->m[i][j]);
                c += n;
            }
        }

        // Build the linked list of union parts.
        if (first == 1) {
            relation_union = relation;
            first = 0;
        } else {
            previous->next = relation;
        }

        previous = relation;
        read_attributes = 1;
    }

    return relation_union;
}

/**
 * osl_relation_psread function ("precision read"):
 * this function is equivalent to osl_relation_psread_polylib() except that
 * it reads an the type of the relation before reading the rest of the
 * input.
 * \see{osl_relation_psread_polylib}
 */
osl_relation_p osl_relation_psread(char ** input, int precision) {
    int type;
    osl_relation_p relation;

    type = osl_relation_read_type(NULL, input);
    relation = osl_relation_psread_polylib(input, precision);
    relation->type = type;

    return relation;
}

/**
 * osl_relation_psread_polylib function ("precision read"):
 * this function reads a relation from a string complying to the Extended
 * PolyLib textual format and returns a pointer this relation. The input
 * parameter is updated to the position in the input string this function
 * reach right after reading the generic structure.
 * \param[in,out] input     The input string where to find a relation.
 *                          Updated to the position after what has been read.
 * \param[in]     precision The precision of the relation elements.
 * \return A pointer to the relation structure that has been read.
 */
osl_relation_p osl_relation_psread_polylib(char ** input, int precision) {
    int i, j, k, n, read = 0;
    int nb_rows, nb_columns;
    int nb_output_dims, nb_input_dims, nb_local_dims, nb_parameters;
    int nb_union_parts = 1;
    int may_read_nb_union_parts = 1;
    int read_attributes = 1;
    int first = 1;
    char str[OSL_MAX_STRING], *tmp;
    osl_relation_p relation, relation_union = NULL, previous = NULL;

    // Read each part of the union (the number of parts may be updated inside)
    for (k = 0; k < nb_union_parts; k++) {
        // Read the number of union parts or the attributes of the union part
        while (read_attributes) {
            read_attributes = 0;
            // Read relation attributes.
            osl_util_sskip_blank_and_comments(input);
            // make a copy of the first row
            size_t row_size = 0;
            tmp = *input;
            while ((*tmp != '\0') && (*tmp != '\n')) {
                tmp++;
                row_size += 1;
            }
            strncpy(str, *input, sizeof(char) * row_size);
            str[(tmp-*input)] = '\0';

            read = sscanf(str, " %d %d %d %d %d %d",
                          &nb_rows, &nb_columns,
                          &nb_output_dims, &nb_input_dims,
                          &nb_local_dims, &nb_parameters);
            *input = tmp;

            if (((read != 1) && (read != 6)) ||
                    ((read == 1) && (may_read_nb_union_parts != 1)))
                OSL_error("not 1 or 6 integers on the first relation line");

            if (read == 1) {
                // Only one number means a union and is the number of parts.
                nb_union_parts = nb_rows;
                if (nb_union_parts < 1)
                    OSL_error("negative nb of union parts");

                // Allow to read the properties of the first part of the union.
                read_attributes = 1;
            }

            may_read_nb_union_parts = 0;
        }

        // Allocate the union part and fill its properties.
        relation = osl_relation_pmalloc(precision, nb_rows, nb_columns);
        relation->nb_output_dims = nb_output_dims;
        relation->nb_input_dims  = nb_input_dims;
        relation->nb_local_dims  = nb_local_dims;
        relation->nb_parameters  = nb_parameters;

        // Read the matrix of constraints.
        for (i = 0; i < relation->nb_rows; i++) {
            osl_util_sskip_blank_and_comments(input);
            if (!(*input))
                OSL_error("not enough rows");

            for (j = 0; j < relation->nb_columns; j++) {
                if (*input == NULL || **input == '#' || **input == '\n')
                    OSL_error("not enough columns");
                if (sscanf(*input, "%s%n", str, &n) == 0)
                    OSL_error("not enough rows");

                // TODO: remove this tmp (sread updates the pointer).
                tmp = str;
                osl_int_sread(&tmp, precision, &relation->m[i][j]);
                *input += n;
            }
        }

        // Build the linked list of union parts.
        if (first == 1) {
            relation_union = relation;
            first = 0;
        } else {
            previous->next = relation;
        }

        previous = relation;
        read_attributes = 1;
    }

    return relation_union;
}


/**
 * osl_relation_sread function:
 * this function is equivalent to osl_relation_psread() except that
 * the precision corresponds to the precision environment variable or
 * to the highest available precision if it is not defined.
 * \see{osl_relation_psread}
 */
osl_relation_p osl_relation_sread(char ** input) {
    int precision = osl_util_get_precision();
    return osl_relation_psread(input, precision);
}

/**
 * osl_relation_sread function:
 * this function is equivalent to osl_relation_psread_polylib() except that
 * the precision corresponds to the precision environment variable or
 * to the highest available precision if it is not defined.
 * \see{osl_relation_posread_polylib}
 */
osl_relation_p osl_relation_sread_polylib(char ** input) {
    int precision = osl_util_get_precision();
    return osl_relation_psread_polylib(input, precision);
}


/**
 * osl_relation_read function:
 * this function is equivalent to osl_relation_pread() except that
 * the precision corresponds to the precision environment variable or
 * to the highest available precision if it is not defined.
 * \see{osl_relation_pread}
 */
osl_relation_p osl_relation_read(FILE * foo) {
    int precision = osl_util_get_precision();
    return osl_relation_pread(foo, precision);
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_relation_pmalloc function:
 * (precision malloc) this function allocates the memory space for an
 * osl_relation_t structure and sets its fields with default values.
 * Then it returns a pointer to the allocated space.
 * \param[in] precision  The precision of the constraint matrix.
 * \param[in] nb_rows    The number of row of the relation to allocate.
 * \param[in] nb_columns The number of columns of the relation to allocate.
 * \return A pointer to an empty relation with fields set to default values
 *         and a ready-to-use constraint matrix.
 */
osl_relation_p osl_relation_pmalloc(int precision,
                                    int nb_rows, int nb_columns) {
    osl_relation_p relation;
    osl_int_t ** p, * q;
    int i, j;

    if ((precision != OSL_PRECISION_SP) &&
            (precision != OSL_PRECISION_DP) &&
            (precision != OSL_PRECISION_MP))
        OSL_error("unknown precision");

    if ((nb_rows < 0) || (nb_columns < 0))
        OSL_error("negative sizes");

    OSL_malloc(relation, osl_relation_p, sizeof(osl_relation_t));
    relation->type           = OSL_UNDEFINED;
    relation->nb_rows        = nb_rows;
    relation->nb_columns     = nb_columns;
    relation->nb_output_dims = OSL_UNDEFINED;
    relation->nb_input_dims  = OSL_UNDEFINED;
    relation->nb_parameters  = OSL_UNDEFINED;
    relation->nb_local_dims  = OSL_UNDEFINED;
    relation->precision      = precision;

    if ((nb_rows == 0) || (nb_columns == 0) ||
            (nb_rows == OSL_UNDEFINED) || (nb_columns == OSL_UNDEFINED)) {
        relation->m = NULL;
    } else {
        OSL_malloc(p, osl_int_t**, (size_t)nb_rows * sizeof(osl_int_t*));
        OSL_malloc(q, osl_int_t*, (size_t)nb_rows * (size_t)nb_columns * sizeof(osl_int_t));
        relation->m = p;
        for (i = 0; i < nb_rows; i++) {
            relation->m[i] = q + i * nb_columns ;
            for (j = 0; j < nb_columns; j++)
                osl_int_init_set_si(precision, &relation->m[i][j], 0);
        }
    }

    relation->next = NULL;

    return relation;
}


/**
 * osl_relation_malloc function:
 * this function is equivalent to osl_relation_pmalloc() except that
 * the precision corresponds to the precision environment variable or
 * to the highest available precision if it is not defined.
 * \see{osl_relation_pmalloc}
 */
osl_relation_p osl_relation_malloc(int nb_rows, int nb_columns) {
    int precision = osl_util_get_precision();
    return osl_relation_pmalloc(precision, nb_rows, nb_columns);
}


/**
 * osl_relation_free_inside function:
 * this function frees the allocated memory for the inside of a
 * osl_relation_t structure, i.e. only m.
 * \param[in] relation The pointer to the relation we want to free internals.
 */
void osl_relation_free_inside(osl_relation_p relation) {
    int i, nb_elements;

    if (relation == NULL)
        return;

    nb_elements = relation->nb_rows * relation->nb_columns;

    for (i = 0; i < nb_elements; i++)
        osl_int_clear(relation->precision, &relation->m[0][i]);

    if (relation->m != NULL) {
        if (nb_elements > 0)
            free(relation->m[0]);
        free(relation->m);
    }
}


/**
 * osl_relation_free function:
 * this function frees the allocated memory for an osl_relation_t
 * structure.
 * \param[in] relation The pointer to the relation we want to free.
 */
void osl_relation_free(osl_relation_p relation) {
    osl_relation_p tmp;

    while (relation != NULL) {
        tmp = relation->next;
        osl_relation_free_inside(relation);
        free(relation);
        relation = tmp;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_relation_nclone function:
 * this functions builds and returns a "hard copy" (not a pointer copy) of the
 * first n parts of a relation union.
 * \param[in] relation The pointer to the relation we want to clone.
 * \param[in] n        The number of union parts of the relation we want to
 *                     clone (the special value -1 means "all the parts").
 * \return A pointer to the clone of the relation union restricted to the
 *         first n parts of the relation union.
 */
osl_relation_p osl_relation_nclone(osl_relation_p relation, int n) {
    int i, j, k;
    int first = 1, nb_components, nb_parts;
    osl_relation_p clone = NULL, node, previous = NULL;

    nb_components = osl_relation_nb_components(relation);
    nb_parts = (n == -1) ? nb_components : n;
    if (nb_components < nb_parts)
        OSL_error("not enough union parts to clone");

    for (k = 0; k < nb_parts; k++) {
        node = osl_relation_pmalloc(relation->precision,
                                    relation->nb_rows, relation->nb_columns);
        node->type           = relation->type;
        node->nb_output_dims = relation->nb_output_dims;
        node->nb_input_dims  = relation->nb_input_dims;
        node->nb_local_dims  = relation->nb_local_dims;
        node->nb_parameters  = relation->nb_parameters;

        for (i = 0; i < relation->nb_rows; i++)
            for (j = 0; j < relation->nb_columns; j++)
                osl_int_assign(relation->precision,
                               &node->m[i][j], relation->m[i][j]);

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        relation = relation->next;
    }

    return clone;
}


/**
 * osl_relation_clone_nconstraints function:
 * this functions builds and returns a "hard copy" (not a pointer copy) of a
 * osl_relation_t data structure such that the clone is restricted to the
 * "n" first rows of the relation. This applies to all the parts in the case
 * of a relation union.
 * \param[in] relation The pointer to the relation we want to clone.
 * \param[in] n        The number of row of the relation we want to clone (the
 *                     special value -1 means "all the rows").
 * \return A pointer to the clone of the relation union restricted to the
 *         first n rows of constraint matrix for each part of the union.
 */
osl_relation_p osl_relation_clone_nconstraints(osl_relation_p relation,
        int n) {
    int i, j;
    int first = 1, all_rows = 0;
    osl_relation_p clone = NULL, node, previous = NULL;

    if (n == -1)
        all_rows = 1;

    while (relation != NULL) {
        if (all_rows)
            n = relation->nb_rows;

        if (n > relation->nb_rows)
            OSL_error("not enough rows to clone in the relation");

        node = osl_relation_pmalloc(relation->precision, n, relation->nb_columns);
        node->type           = relation->type;
        node->nb_output_dims = relation->nb_output_dims;
        node->nb_input_dims  = relation->nb_input_dims;
        node->nb_local_dims  = relation->nb_local_dims;
        node->nb_parameters  = relation->nb_parameters;

        for (i = 0; i < n; i++)
            for (j = 0; j < relation->nb_columns; j++)
                osl_int_assign(relation->precision,
                               &node->m[i][j], relation->m[i][j]);

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        relation = relation->next;
    }

    return clone;
}


/**
 * osl_relation_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_relation_t data structure (the full union of relation).
 * \param[in] relation The pointer to the relation we want to clone.
 * \return A pointer to the clone of the union of relations.
 */
osl_relation_p osl_relation_clone(osl_relation_p relation) {
    if (relation == NULL)
        return NULL;

    return osl_relation_nclone(relation, -1);
}


/**
 * osl_relation_add function:
 * this function adds a relation (union) at the end of the relation (union)
 * pointed by r1. No new relation is created: this functions links the two
 * input unions. If the first relation is NULL, it is set to the
 * second relation.
 * \param[in,out] r1  Pointer to the first relation (union).
 * \param[in]     r2  The second relation (union).
 */
void osl_relation_add(osl_relation_p *r1, osl_relation_p r2) {
    while (*r1 != NULL)
        r1 = &((*r1)->next);

    *r1 = r2;
}


/**
 * osl_relation_union function:
 * this function builds a new relation from two relations provided
 * as parameters. The new relation is built as an union of the
 * two relations: the list of constraint sets are linked together.
 * \param[in] r1 The first relation.
 * \param[in] r2 The second relation.
 * \return A new relation corresponding to the union of r1 and r2.
 */
osl_relation_p osl_relation_union(osl_relation_p r1,
                                  osl_relation_p r2) {
    osl_relation_p copy1, copy2;

    if ((r1 == NULL) && (r2 == NULL))
        return NULL;

    copy1 = osl_relation_clone(r1);
    copy2 = osl_relation_clone(r2);
    osl_relation_add(&copy1, copy2);

    return copy1;
}


/**
 * osl_relation_replace_vector function:
 * this function replaces the "row"^th row of a relation "relation" with the
 * vector "vector". It directly updates the relation union part pointed
 * by "relation" and this part only.
 * \param[in,out] relation The relation we want to replace a row.
 * \param[in]     vector   The vector that will replace a row of the relation.
 * \param[in]     row      The row of the relation to be replaced.
 */
void osl_relation_replace_vector(osl_relation_p relation,
                                 osl_vector_p vector, int row) {
    int i;

    if ((relation == NULL) || (vector == NULL)     ||
            (relation->precision != vector->precision) ||
            (relation->nb_columns != vector->size)     ||
            (row >= relation->nb_rows) || (row < 0))
        OSL_error("vector cannot replace relation row");

    for (i = 0; i < vector->size; i++)
        osl_int_assign(relation->precision, &relation->m[row][i], vector->v[i]);
}


/**
 * osl_relation_add_vector function:
 * this function adds (meaning, +) a vector to the "row"^th row of a
 * relation "relation". It directly updates the relation union part pointed
 * by "relation" and this part only.
 * \param[in,out] relation The relation we want to add a vector to a row.
 * \param[in]     vector   The vector that will replace a row of the relation.
 * \param[in]     row      The row of the relation to add the vector.
 */
void osl_relation_add_vector(osl_relation_p relation,
                             osl_vector_p vector, int row) {
    int i;

    if ((relation == NULL) || (vector == NULL)     ||
            (relation->precision != vector->precision) ||
            (relation->nb_columns != vector->size)     ||
            (row >= relation->nb_rows) || (row < 0))
        OSL_error("vector cannot be added to relation");

    if (osl_int_get_si(relation->precision, relation->m[row][0]) == 0)
        osl_int_assign(relation->precision, &relation->m[row][0], vector->v[0]);

    for (i = 1; i < vector->size; i++)
        osl_int_add(relation->precision,
                    &relation->m[row][i], relation->m[row][i], vector->v[i]);
}


/**
 * osl_relation_sub_vector function:
 * this function subtracts the vector "vector" to the "row"^th row of
 * a relation "relation. It directly updates the relation union part pointed
 * by "relation" and this part only.
 * \param[in,out] relation The relation where to subtract a vector to a row.
 * \param[in]     vector   The vector to subtract to a relation row.
 * \param[in]     row      The row of the relation to subtract the vector.
 */
void osl_relation_sub_vector(osl_relation_p relation,
                             osl_vector_p vector, int row) {
    int i;

    if ((relation == NULL) || (vector == NULL)     ||
            (relation->precision != vector->precision) ||
            (relation->nb_columns != vector->size)     ||
            (row >= relation->nb_rows) || (row < 0))
        OSL_error("vector cannot be subtracted to row");

    if (osl_int_get_si(relation->precision, relation->m[row][0]) == 0)
        osl_int_assign(relation->precision, &relation->m[row][0], vector->v[0]);

    for (i = 1; i < vector->size; i++)
        osl_int_sub(relation->precision,
                    &relation->m[row][i], relation->m[row][i], vector->v[i]);
}


/**
 * osl_relation_insert_vector function:
 * this function inserts a new row corresponding to the vector "vector" to
 * the relation "relation" by inserting it at the "row"^th row of
 * "relation" (-1 is a shortcut to insert the vector after the constraints
 * of the relation). It directly updates the relation union part pointed
 * by "relation" and this part only. If "vector" (or "relation") is NULL,
 * the relation is left unmodified.
 * \param[in,out] relation The relation we want to extend.
 * \param[in]     vector   The vector that will be added relation.
 * \param[in]     row      The row where to insert the vector (-1 to
 *                         insert it after the relation constraints).
 */
void osl_relation_insert_vector(osl_relation_p relation,
                                osl_vector_p vector, int row) {
    osl_relation_p temp;

    temp = osl_relation_from_vector(vector);
    osl_relation_insert_constraints(relation, temp, row);
    osl_relation_free(temp);
}


/**
 * osl_relation_concat_vector function:
 * this function builds a new relation from one relation and a vector sent as
 * parameters. The new set of constraints is built as the concatenation
 * of the rows of the first part of the relation and of the vector
 * constraint. This means, there is no next field in the result.
 * \param[in] r The input relation.
 * \param[in] v The input vector.
 * \return A pointer to the relation resulting from the concatenation of
 *         the constraints of the relation and of the vector.
 */
osl_relation_p osl_relation_concat_vector(osl_relation_p relation,
        osl_vector_p vector) {
    osl_relation_p new, temp;

    temp = osl_relation_from_vector(vector);
    new = osl_relation_concat_constraints(relation, temp);
    osl_relation_free(temp);
    return new;
}


/**
 * osl_relation_insert_blank_row function:
 * this function inserts a new row filled with zeros o an existing relation
 * union part (it only affects the first union part).
 * \param[in,out] relation The relation to add a row in.
 * \param[in]     row      The row where to insert the blank row.
 */
void osl_relation_insert_blank_row(osl_relation_p relation, int row) {
    osl_vector_p vector;

    if (relation != NULL) {
        vector = osl_vector_pmalloc(relation->precision, relation->nb_columns);
        osl_relation_insert_vector(relation, vector, row);
        osl_vector_free(vector);
    }
}


/**
 * osl_relation_insert_blank_column function:
 * this function inserts a new column filled with zeros to an existing
 * relation union part (it only affects the first union part). WARNING:
 * this function does not update the relation attributes.
 * \param[in,out] relation The relation to add a column in.
 * \param[in]     column   The column where to insert the blank column.
 */
void osl_relation_insert_blank_column(osl_relation_p relation, int column) {

    int i, j;
    osl_relation_p temp;

    if (relation == NULL)
        return;

    if ((column < 0) || (column > relation->nb_columns))
        OSL_error("bad column number");

    // We use a temporary relation just to reuse existing functions. Cleaner.
    temp = osl_relation_pmalloc(relation->precision,
                                relation->nb_rows, relation->nb_columns + 1);

    for (i = 0; i < relation->nb_rows; i++) {
        for (j = 0; j < column; j++)
            osl_int_assign(relation->precision, &temp->m[i][j], relation->m[i][j]);

        for (j = column; j < relation->nb_columns; j++)
            osl_int_assign(relation->precision, &temp->m[i][j+1], relation->m[i][j]);
    }

    osl_relation_free_inside(relation);

    // Replace the inside of relation.
    relation->nb_columns = temp->nb_columns;
    relation->m = temp->m;

    // Free the temp "shell".
    free(temp);
}


/**
 * osl_relation_from_vector function:
 * this function converts a vector "vector" to a relation with a single row
 * and returns a pointer to that relation.
 * \param[in] vector The vector to convert to a relation.
 * \return A pointer to a relation resulting from the vector conversion.
 */
osl_relation_p osl_relation_from_vector(osl_vector_p vector) {
    osl_relation_p relation;

    if (vector == NULL)
        return NULL;

    relation = osl_relation_pmalloc(vector->precision, 1, vector->size);
    osl_relation_replace_vector(relation, vector, 0);
    return relation;
}


/**
 * osl_relation_replace_constraints function:
 * this function replaces some rows of a relation "r1" with the rows of
 * the relation "r2". It begins at the "row"^th row of "r1". It directly
 * updates the relation union part pointed by "r1" and this part only.
 * \param[in,out] r1  The relation we want to change some rows.
 * \param[in]     r2  The relation containing the new rows.
 * \param[in]     row The first row of the relation r1 to be replaced.
 */
void osl_relation_replace_constraints(osl_relation_p r1,
                                      osl_relation_p r2, int row) {
    int i, j;

    if ((r1 == NULL) || (r2 == NULL)       ||
            (r1->precision != r2->precision)   ||
            (r1->nb_columns != r2->nb_columns) ||
            ((row + r2->nb_rows) > r1->nb_rows) || (row < 0))
        OSL_error("relation rows could not be replaced");

    for (i = 0; i < r2->nb_rows; i++)
        for (j = 0; j < r2->nb_columns; j++)
            osl_int_assign(r1->precision, &r1->m[i+row][j], r2->m[i][j]);
}


/**
 * osl_relation_insert_constraints function:
 * this function inserts the rows of the relation "r2" to the relation
 * "r1", starting from the "row"^th row of "r1" (-1 is a
 * shortcut to insert the "r2" constraints after the constraints of r1).
 * It directly updates the relation union part pointed by "r1" and this
 * part only. If "r2" (or "r1") is NULL, the relation is left unmodified.
 * \param[in,out] r1  The relation we want to extend.
 * \param[in]     r2  The relation to be inserted.
 * \param[in]     row The row where to insert the constraints (-1 to
 *                    insert them after those of "r1").
 */
void osl_relation_insert_constraints(osl_relation_p r1,
                                     osl_relation_p r2, int row) {
    int i, j;
    osl_relation_p temp;

    if ((r1 == NULL) || (r2 == NULL))
        return;

    if (row == -1)
        row = r1->nb_rows;

    if ((r1->nb_columns != r2->nb_columns) ||
            (r1->precision != r2->precision)   ||
            (row > r1->nb_rows) || (row < 0))
        OSL_error("constraints cannot be inserted");

    // We use a temporary relation just to reuse existing functions. Cleaner.
    temp = osl_relation_pmalloc(r1->precision,
                                r1->nb_rows + r2->nb_rows, r1->nb_columns);

    for (i = 0; i < row; i++)
        for (j = 0; j < r1->nb_columns; j++)
            osl_int_assign(r1->precision, &temp->m[i][j], r1->m[i][j]);

    osl_relation_replace_constraints(temp, r2, row);

    for (i = row + r2->nb_rows; i < r2->nb_rows + r1->nb_rows; i++)
        for (j = 0; j < r1->nb_columns; j++)
            osl_int_assign(r1->precision, &temp->m[i][j], r1->m[i-r2->nb_rows][j]);

    osl_relation_free_inside(r1);

    // Replace the inside of relation.
    r1->nb_rows = temp->nb_rows;
    r1->m = temp->m;

    // Free the temp "shell".
    free(temp);
}


/**
 * osl_relation_swap_constraints function:
 * this function swaps two constraints (i.e., rows) of the relation matrix.
 * This function updates the relation directly.
 * \param[in,out] relation The relation to swap two rows (modified).
 * \param[in]     c1       The row corresponding to the first constraint.
 * \param[in]     c2       The row corresponding to the second constraint.
 */
void osl_relation_swap_constraints(osl_relation_p relation, int c1, int c2) {
    int i;

    if ((relation == NULL) || (c1 == c2))
        return;

    if ((c1 >= relation->nb_rows) || (c1 < 0) ||
            (c2 >= relation->nb_rows) || (c2 < 0))
        OSL_error("bad constraint rows");

    for (i = 0; i < relation->nb_columns; i++)
        osl_int_swap(relation->precision,
                     &relation->m[c1][i], &relation->m[c2][i]);
}


/**
 * osl_relation_remove_row function:
 * this function removes a given row to the relation "r". It directly
 * updates the relation union part pointed by "r" and this part only.
 * \param[in,out] r   The relation to remove a row.
 * \param[in]     row The row number to remove.
 */
void osl_relation_remove_row(osl_relation_p r, int row) {
    int i, j;
    osl_relation_p temp;

    if (r == NULL)
        return;

    if ((row < 0) || (row >= r->nb_rows))
        OSL_error("bad row number");

    // We use a temporary relation just to reuse existing functions. Cleaner.
    temp = osl_relation_pmalloc(r->precision,
                                r->nb_rows - 1, r->nb_columns);

    for (i = 0; i < row; i++)
        for (j = 0; j < r->nb_columns; j++)
            osl_int_assign(r->precision, &temp->m[i][j], r->m[i][j]);

    for (i = row + 1; i < r->nb_rows; i++)
        for (j = 0; j < r->nb_columns; j++)
            osl_int_assign(r->precision, &temp->m[i - 1][j], r->m[i][j]);

    osl_relation_free_inside(r);

    // Replace the inside of relation.
    r->nb_rows = temp->nb_rows;
    r->m = temp->m;

    // Free the temp "shell".
    free(temp);
}


/**
 * osl_relation_remove_column function:
 * this function removes a given column to the relation "r". It directly
 * updates the relation union part pointed by "r" and this part only.
 * \param[in,out] r      The relation to remove a column.
 * \param[in]     column The column number to remove.
 */
void osl_relation_remove_column(osl_relation_p r, int column) {
    int i, j;
    osl_relation_p temp;

    if (r == NULL)
        return;

    if ((column < 0) || (column >= r->nb_columns))
        OSL_error("bad column number");

    // We use a temporary relation just to reuse existing functions. Cleaner.
    temp = osl_relation_pmalloc(r->precision,
                                r->nb_rows, r->nb_columns - 1);

    for (i = 0; i < r->nb_rows; i++) {
        for (j = 0; j < column; j++)
            osl_int_assign(r->precision, &temp->m[i][j], r->m[i][j]);

        for (j = column + 1; j < r->nb_columns; j++)
            osl_int_assign(r->precision, &temp->m[i][j - 1], r->m[i][j]);
    }

    osl_relation_free_inside(r);

    // Replace the inside of relation.
    r->nb_columns = temp->nb_columns;
    r->m = temp->m;

    // Free the temp "shell".
    free(temp);
}


/**
 * osl_relation_insert_columns function:
 * this function inserts new columns to an existing relation union part (it
 * only affects the first union part). The columns are copied out from the
 * matrix of an input relation which must have the convenient number of rows.
 * All columns of the input matrix are copied. WARNING: this function does not
 * update the relation attributes of the modified matrix.
 * \param[in,out] relation The relation to add columns in.
 * \param[in]     insert   The relation containing the columns to add.
 * \param[in]     column   The column where to insert the new columns.
 */
void osl_relation_insert_columns(osl_relation_p relation,
                                 osl_relation_p insert, int column) {
    int i, j;
    osl_relation_p temp;

    if ((relation == NULL) || (insert == NULL))
        return;

    if ((relation->precision != insert->precision) ||
            (relation->nb_rows   != insert->nb_rows)   ||
            (column < 0) || (column > relation->nb_columns))
        OSL_error("columns cannot be inserted");

    // We use a temporary relation just to reuse existing functions. Cleaner.
    temp = osl_relation_pmalloc(relation->precision, relation->nb_rows,
                                relation->nb_columns + insert->nb_columns);

    for (i = 0; i < relation->nb_rows; i++) {
        for (j = 0; j < column; j++)
            osl_int_assign(relation->precision, &temp->m[i][j], relation->m[i][j]);

        for (j = column; j < column + insert->nb_columns; j++)
            osl_int_assign(relation->precision,
                           &temp->m[i][j], insert->m[i][j - column]);

        for (j = column + insert->nb_columns;
                j < insert->nb_columns + relation->nb_columns; j++)
            osl_int_assign(relation->precision,
                           &temp->m[i][j], relation->m[i][j - insert->nb_columns]);
    }

    osl_relation_free_inside(relation);

    // Replace the inside of relation.
    relation->nb_columns = temp->nb_columns;
    relation->m = temp->m;

    // Free the temp "shell".
    free(temp);
}


/**
 * osl_relation_concat_constraints function:
 * this function builds a new relation from two relations sent as
 * parameters. The new set of constraints is built as the concatenation
 * of the rows of the first elements of the two relation unions r1 and r2.
 * This means, there is no next field in the result.
 * \param[in] r1  The first relation.
 * \param[in] r2  The second relation.
 * \return A pointer to the relation resulting from the concatenation of
 *         the first elements of r1 and r2.
 */
osl_relation_p osl_relation_concat_constraints(
    osl_relation_p r1,
    osl_relation_p r2) {
    osl_relation_p new;

    if (r1 == NULL)
        return osl_relation_clone(r2);

    if (r2 == NULL)
        return osl_relation_clone(r1);

    if (r1->nb_columns != r2->nb_columns)
        OSL_error("incompatible sizes for concatenation");

    if (r1->next || r2->next)
        OSL_warning("relation concatenation is done on the first elements "
                    "of union only");

    new = osl_relation_pmalloc(r1->precision,
                               r1->nb_rows + r2->nb_rows, r1->nb_columns);
    osl_relation_replace_constraints(new, r1, 0);
    osl_relation_replace_constraints(new, r2, r1->nb_rows);

    return new;
}


/**
 * osl_relation_part_equal function:
 * this function returns true if the two relations parts provided as
 * parameters are the same, false otherwise. In the case of relation
 * unions, only the first part of the two relations are tested.
 * \param[in] r1 The first relation.
 * \param[in] r2 The second relation.
 * \return 1 if r1 and r2 are the same (content-wise), 0 otherwise.
 */
int osl_relation_part_equal(osl_relation_p r1, osl_relation_p r2) {
    int i, j;

    if (r1 == r2)
        return 1;

    if (((r1 == NULL) && (r2 != NULL)) ||
            ((r1 != NULL) && (r2 == NULL)))
        return 0;

    if ((r1->type           != r2->type)           ||
            (r1->precision      != r2->precision)      ||
            (r1->nb_rows        != r2->nb_rows)        ||
            (r1->nb_columns     != r2->nb_columns)     ||
            (r1->nb_output_dims != r2->nb_output_dims) ||
            (r1->nb_input_dims  != r2->nb_input_dims)  ||
            (r1->nb_local_dims  != r2->nb_local_dims)  ||
            (r1->nb_parameters  != r2->nb_parameters))
        return 0;

    for (i = 0; i < r1->nb_rows; ++i)
        for (j = 0; j < r1->nb_columns; ++j)
            if (osl_int_ne(r1->precision, r1->m[i][j], r2->m[i][j]))
                return 0;

    return 1;
}


/**
 * osl_relation_equal function:
 * this function returns true if the two relations provided as parameters
 * are the same, false otherwise.
 * \param[in] r1 The first relation.
 * \param[in] r2 The second relation.
 * \return 1 if r1 and r2 are the same (content-wise), 0 otherwise.
 */
int osl_relation_equal(osl_relation_p r1, osl_relation_p r2) {
    while ((r1 != NULL) && (r2 != NULL)) {
        if (!osl_relation_part_equal(r1, r2))
            return 0;

        r1 = r1->next;
        r2 = r2->next;
    }

    if (((r1 == NULL) && (r2 != NULL)) || ((r1 != NULL) && (r2 == NULL)))
        return 0;

    return 1;
}


/**
 * osl_relation_check_attribute internal function:
 * This function checks whether an "actual" value is the same as an
 * "expected" value or not. If the expected value is set to
 * OSL_UNDEFINED, this function sets it to the "actual" value
 * and do not report a difference has been detected.
 * It returns 0 if a difference has been detected, 1 otherwise.
 * \param[in,out] expected Pointer to the expected value (the value is
 *                         modified if it was set to OSL_UNDEFINED).
 * \param[in]     actual   Value we want to check.
 * \return 0 if the values are not the same while the expected value was
 *         not OSL_UNDEFINED, 1 otherwise.
 */
static
int osl_relation_check_attribute(int * expected, int actual) {
    if (*expected != OSL_UNDEFINED) {
        if ((actual != OSL_UNDEFINED) &&
                (actual != *expected)) {
            OSL_warning("unexpected atribute");
            return 0;
        }
    } else {
        *expected = actual;
    }

    return 1;
}


/**
 * osl_relation_check_nb_columns internal function:
 * This function checks that the number of columns of a relation
 * corresponds to some expected properties (setting an expected property to
 * OSL_UNDEFINED makes this function unable to detect a problem).
 * It returns 0 if the number of columns seems incorrect or 1 if no problem
 * has been detected.
 * \param[in] relation  The relation we want to check the number of columns.
 * \param[in] expected_nb_output_dims Expected number of output dimensions.
 * \param[in] expected_nb_input_dims  Expected number of input dimensions.
 * \param[in] expected_nb_parameters  Expected number of parameters.
 * \return 0 if the number of columns seems incorrect, 1 otherwise.
 */
static
int osl_relation_check_nb_columns(osl_relation_p relation,
                                  int expected_nb_output_dims,
                                  int expected_nb_input_dims,
                                  int expected_nb_parameters) {
    int expected_nb_local_dims, expected_nb_columns;

    if ((expected_nb_output_dims != OSL_UNDEFINED) &&
            (expected_nb_input_dims  != OSL_UNDEFINED) &&
            (expected_nb_parameters  != OSL_UNDEFINED)) {

        if (relation->nb_local_dims == OSL_UNDEFINED)
            expected_nb_local_dims = 0;
        else
            expected_nb_local_dims = relation->nb_local_dims;

        expected_nb_columns = expected_nb_output_dims +
                              expected_nb_input_dims  +
                              expected_nb_local_dims  +
                              expected_nb_parameters  +
                              2;

        if (expected_nb_columns != relation->nb_columns) {
            OSL_warning("unexpected number of columns");
            return 0;
        }
    }

    return 1;
}


/**
 * osl_relation_integrity_check function:
 * this function checks that a relation is "well formed" according to some
 * expected properties (setting an expected value to OSL_UNDEFINED means
 * that we do not expect a specific value) and what the relation is supposed
 * to represent. It returns 0 if the check failed or 1 if no problem has been
 * detected.
 * \param[in] relation      The relation we want to check.
 * \param[in] expected_type Semantics about this relation (domain, access...).
 * \param[in] expected_nb_output_dims Expected number of output dimensions.
 * \param[in] expected_nb_input_dims  Expected number of input dimensions.
 * \param[in] expected_nb_parameters  Expected number of parameters.
 * \return 0 if the integrity check fails, 1 otherwise.
 */
int osl_relation_integrity_check(osl_relation_p relation,
                                 int expected_type,
                                 int expected_nb_output_dims,
                                 int expected_nb_input_dims,
                                 int expected_nb_parameters) {
    int i;

    // Check the NULL case.
    if (relation == NULL) {
        if ((expected_nb_output_dims != OSL_UNDEFINED) ||
                (expected_nb_input_dims  != OSL_UNDEFINED) ||
                (expected_nb_parameters  != OSL_UNDEFINED)) {
            OSL_debug("NULL relation with some expected attibutes");
            //return 0;
        }

        return 1;
    }

    // Check the type.
    if (((expected_type != OSL_TYPE_ACCESS) &&
            (expected_type != relation->type)) ||
            ((expected_type == OSL_TYPE_ACCESS) &&
             (!osl_relation_is_access(relation)))) {
        OSL_warning("wrong type");
        osl_relation_dump(stderr, relation);
        return 0;
    }

    // Check that relations have no undefined atributes.
    if ((relation->nb_output_dims == OSL_UNDEFINED) ||
            (relation->nb_input_dims  == OSL_UNDEFINED) ||
            (relation->nb_local_dims  == OSL_UNDEFINED) ||
            (relation->nb_parameters  == OSL_UNDEFINED)) {
        OSL_warning("all attributes should be defined");
        osl_relation_dump(stderr, relation);
        return 0;
    }

    // Check that a context has actually 0 output dimensions.
    if ((relation->type == OSL_TYPE_CONTEXT) &&
            (relation->nb_output_dims != 0)) {
        OSL_warning("context without 0 as number of output dimensions");
        osl_relation_dump(stderr, relation);
        return 0;
    }

    // Check that a domain or a context has actually 0 input dimensions.
    if (((relation->type == OSL_TYPE_DOMAIN) ||
            (relation->type == OSL_TYPE_CONTEXT)) &&
            (relation->nb_input_dims != 0)) {
        OSL_warning("domain or context without 0 input dimensions");
        osl_relation_dump(stderr, relation);
        return 0;
    }

    // Check properties according to expected values (and if expected values
    // are undefined, define them with the first relation part properties).
    if (!osl_relation_check_attribute(&expected_nb_output_dims,
                                      relation->nb_output_dims) ||
            !osl_relation_check_attribute(&expected_nb_input_dims,
                                          relation->nb_input_dims)  ||
            !osl_relation_check_attribute(&expected_nb_parameters,
                                          relation->nb_parameters)) {
        osl_relation_dump(stderr, relation);
        return 0;
    }

    while (relation != NULL) {

        // Attributes (except the number of local dimensions) should be the same
        // in all parts of the union.
        if ((expected_nb_output_dims != relation->nb_output_dims) ||
                (expected_nb_input_dims  != relation->nb_input_dims)  ||
                (expected_nb_parameters  != relation->nb_parameters)) {
            OSL_warning("inconsistent attributes");
            osl_relation_dump(stderr, relation);
            return 0;
        }

        // Check whether the number of columns is OK or not.
        if (!osl_relation_check_nb_columns(relation,
                                           expected_nb_output_dims,
                                           expected_nb_input_dims,
                                           expected_nb_parameters)) {
            osl_relation_dump(stderr, relation);
            return 0;
        }

        // Check the first column. The first column of a relation part should be
        // made of 0 or 1 only.
        if ((relation->nb_rows > 0) && (relation->nb_columns > 0)) {
            for (i = 0; i < relation->nb_rows; i++) {
                if (!osl_int_zero(relation->precision, relation->m[i][0]) &&
                        !osl_int_one(relation->precision, relation->m[i][0])) {
                    OSL_warning("first column of a relation is not "
                                "strictly made of 0 or 1");
                    osl_relation_dump(stderr, relation);
                    return 0;
                }
            }
        }

        // Array accesses must provide the array identifier.
        if ((osl_relation_is_access(relation)) &&
                (osl_relation_get_array_id(relation) == OSL_UNDEFINED)) {
            osl_relation_dump(stderr, relation);
            return 0;
        }

        relation = relation->next;
    }

    return 1;
}


/**
 * osl_relation_set_attributes_one function:
 * this functions sets the attributes of a relation part provided as a
 * parameter. It updates the relation directly.
 * \param[in,out] relation The relation (union part) to set the attributes.
 * \param[in]     nb_output_dims Number of output dimensions.
 * \param[in]     nb_input_dims  Number of input dimensions.
 * \param[in]     nb_local_dims  Number of local dimensions.
 * \param[in]     nb_parameters  Number of parameters.
 */
void osl_relation_set_attributes_one(osl_relation_p relation,
                                     int nb_output_dims, int nb_input_dims,
                                     int nb_local_dims,  int nb_parameters) {
    if (relation != NULL) {
        relation->nb_output_dims = nb_output_dims;
        relation->nb_input_dims  = nb_input_dims;
        relation->nb_local_dims  = nb_local_dims;
        relation->nb_parameters  = nb_parameters;
    }
}


/**
 * osl_relation_set_attributes function:
 * this functions sets the attributes of a relation (union) provided
 * as a parameter. It updates the relation directly.
 * \param[in,out] relation The relation (union) to set the attributes.
 * \param[in]     nb_output_dims Number of output dimensions.
 * \param[in]     nb_input_dims  Number of input dimensions.
 * \param[in]     nb_local_dims  Number of local dimensions.
 * \param[in]     nb_parameters  Number of parameters.
 */
void osl_relation_set_attributes(osl_relation_p relation,
                                 int nb_output_dims, int nb_input_dims,
                                 int nb_local_dims,  int nb_parameters) {
    while (relation != NULL) {
        osl_relation_set_attributes_one(relation,
                                        nb_output_dims, nb_input_dims,
                                        nb_local_dims,  nb_parameters);
        relation = relation->next;
    }
}


/**
 * osl_relation_set_type function:
 * this function sets the type of each relation union part in the relation
 * to the one provided as parameter.
 * \param relation The relation to set the type.
 * \param type     The type.
 */
void osl_relation_set_type(osl_relation_p relation, int type) {

    while (relation != NULL) {
        relation->type = type;
        relation = relation->next;
    }
}


/**
 * osl_relation_get_array_id function:
 * this function returns the array identifier in a relation with access type
 * It returns OSL_UNDEFINED if it is not able to find it (in particular
 * if there are irregularities in the relation).
 * \param[in] relation The relation where to find an array identifier.
 * \return The array identifier in the relation or OSL_UNDEFINED.
 */
int osl_relation_get_array_id(osl_relation_p relation) {
    int i;
    int first = 1;
    int array_id = OSL_UNDEFINED;
    int reference_array_id = OSL_UNDEFINED;
    int nb_array_id;
    int row_id = 0;
    int precision;

    if (relation == NULL)
        return OSL_UNDEFINED;

    if (!osl_relation_is_access(relation)) {
        OSL_warning("asked for an array id of non-array relation");
        return OSL_UNDEFINED;
    }

    while (relation != NULL) {
        precision = relation->precision;

        // There should be room to store the array identifier.
        if ((relation->nb_rows < 1) ||
                (relation->nb_columns < 3)) {
            OSL_warning("no array identifier in an access function");
            return OSL_UNDEFINED;
        }

        // Array identifiers are m[i][#columns -1] / m[i][1], with i the only row
        // where m[i][1] is not 0.
        // - check there is exactly one row such that m[i][1] is not 0,
        // - check the whole ith row if full of 0 except m[i][1] and the id,
        // - check that (m[i][#columns -1] % m[i][1]) == 0,
        // - check that (-m[i][#columns -1] / m[i][1]) > 0.
        nb_array_id = 0;
        for (i = 0; i < relation->nb_rows; i++) {
            if (!osl_int_zero(precision, relation->m[i][1])) {
                nb_array_id ++;
                row_id = i;
            }
        }
        if (nb_array_id == 0) {
            OSL_warning("no array identifier in an access function");
            return OSL_UNDEFINED;
        }
        if (nb_array_id > 1) {
            OSL_warning("several array identifiers in one access function");
            return OSL_UNDEFINED;
        }
        for (i = 0; i < relation->nb_columns - 1; i++) {
            if ((i != 1) && !osl_int_zero(precision, relation->m[row_id][i])) {
                OSL_warning("non integer array identifier");
                return OSL_UNDEFINED;
            }
        }
        if (!osl_int_divisible(precision,
                               relation->m[row_id][relation->nb_columns - 1],
                               relation->m[row_id][1])) {
            OSL_warning("rational array identifier");
            return OSL_UNDEFINED;
        }
        array_id = -osl_int_get_si(precision,
                                   relation->m[row_id][relation->nb_columns - 1]);
        array_id /= osl_int_get_si(precision, relation->m[row_id][1]);
        if (array_id <= 0) {
            OSL_warning("negative or 0 identifier in access function");
            return OSL_UNDEFINED;
        }

        // Unions of accesses are allowed, but they should refer at the same array.
        if (first) {
            reference_array_id = array_id;
            first = 0;
        } else {
            if (reference_array_id != array_id) {
                OSL_warning("inconsistency of array identifiers in an "
                            "union of access relations");
                return OSL_UNDEFINED;
            }
        }

        relation = relation->next;
    }

    return array_id;
}


/**
 * osl_relation_is_access function:
 * this function returns 1 if the relation corresponds to an access relation,
 * whatever its precise type (read, write etc.), 0 otherwise.
 * \param relation The relation to check wheter it is an access relation or not.
 * \return 1 if the relation is an access relation, 0 otherwise.
 */
int osl_relation_is_access(osl_relation_p relation) {

    if (relation == NULL)
        return 0;

    if ((relation->type == OSL_TYPE_ACCESS)    ||
            (relation->type == OSL_TYPE_READ)      ||
            (relation->type == OSL_TYPE_WRITE)     ||
            (relation->type == OSL_TYPE_MAY_WRITE))
        return 1;

    return 0;
}


/**
 * osl_relation_get_attributes function:
 * this function returns, through its parameters, the maximum values of the
 * relation attributes (nb_iterators, nb_parameters etc), depending on its
 * type. HOWEVER, it updates the parameter value iff the attribute is greater
 * than the input parameter value. Hence it may be used to get the
 * attributes as well as to find the maximum attributes for several relations.
 * The array identifier 0 is used when there is no array identifier (AND this
 * is OK), OSL_UNDEFINED is used to report it is impossible to provide the
 * property while it should. This function is not intended for checking, the
 * input relation should be correct.
 * \param[in]     relation      The relation to extract attribute values.
 * \param[in,out] nb_parameters Number of parameter attribute.
 * \param[in,out] nb_iterators  Number of iterators attribute.
 * \param[in,out] nb_scattdims  Number of scattering dimensions attribute.
 * \param[in,out] nb_localdims  Number of local dimensions attribute.
 * \param[in,out] array_id      Maximum array identifier attribute.
 */
void osl_relation_get_attributes(osl_relation_p relation,
                                 int * nb_parameters,
                                 int * nb_iterators,
                                 int * nb_scattdims,
                                 int * nb_localdims,
                                 int * array_id) {
    int type;
    int local_nb_parameters = OSL_UNDEFINED;
    int local_nb_iterators  = OSL_UNDEFINED;
    int local_nb_scattdims  = OSL_UNDEFINED;
    int local_nb_localdims  = OSL_UNDEFINED;
    int local_array_id      = OSL_UNDEFINED;

    while (relation != NULL) {
        if (osl_relation_is_access(relation))
            type = OSL_TYPE_ACCESS;
        else
            type = relation->type;

        // There is some redundancy but I believe the code is cleaner this way.
        switch (type) {
        case OSL_TYPE_CONTEXT:
            local_nb_parameters = relation->nb_parameters;
            local_nb_iterators  = 0;
            local_nb_scattdims  = 0;
            local_nb_localdims  = relation->nb_local_dims;
            local_array_id      = 0;
            break;

        case OSL_TYPE_DOMAIN:
            local_nb_parameters = relation->nb_parameters;
            local_nb_iterators  = relation->nb_output_dims;
            local_nb_scattdims  = 0;
            local_nb_localdims  = relation->nb_local_dims;
            local_array_id      = 0;
            break;

        case OSL_TYPE_SCATTERING:
            local_nb_parameters = relation->nb_parameters;
            local_nb_iterators  = relation->nb_input_dims;
            local_nb_scattdims  = relation->nb_output_dims;
            local_nb_localdims  = relation->nb_local_dims;
            local_array_id      = 0;
            break;

        case OSL_TYPE_ACCESS:
            local_nb_parameters = relation->nb_parameters;
            local_nb_iterators  = relation->nb_input_dims;
            local_nb_scattdims  = 0;
            local_nb_localdims  = relation->nb_local_dims;
            local_array_id      = osl_relation_get_array_id(relation);
            break;

        default:
            local_nb_parameters = relation->nb_parameters;
            local_nb_iterators  = relation->nb_input_dims;
            local_nb_scattdims  = relation->nb_output_dims;
            local_nb_localdims  = relation->nb_local_dims;
            local_array_id      = 0;
        }

        // Update.
        *nb_parameters = OSL_max(*nb_parameters, local_nb_parameters);
        *nb_iterators  = OSL_max(*nb_iterators,  local_nb_iterators);
        *nb_scattdims  = OSL_max(*nb_scattdims,  local_nb_scattdims);
        *nb_localdims  = OSL_max(*nb_localdims,  local_nb_localdims);
        *array_id      = OSL_max(*array_id,      local_array_id);
        relation = relation->next;
    }
}


/**
 * osl_relation_extend_output function:
 * this function extends the number of output dimensions of a given relation. It
 * returns a copy of the input relation with a number of output dimensions
 * extended to "dim" for all its union components. The new output dimensions
 * are simply set equal to 0. The extended number of dimensions must be higher
 * than or equal to the original one (an error will be raised otherwise).
 * \param[in] relation The input relation to extend.
 * \param[in] dim      The number of output dimension to reach.
 * \return A new relation: "relation" extended to "dim" output dims.
 */
osl_relation_p osl_relation_extend_output(osl_relation_p relation, int dim) {
    int i, j;
    int first = 1;
    int offset;
    int precision = relation->precision;
    osl_relation_p extended = NULL, node, previous = NULL;

    while (relation != NULL) {
        if (relation->nb_output_dims > dim)
            OSL_error("Number of output dims is greater than required extension");
        offset = dim - relation->nb_output_dims;

        node = osl_relation_pmalloc(precision,
                                    relation->nb_rows + offset,
                                    relation->nb_columns + offset);

        node->type           = relation->type;
        node->nb_output_dims = OSL_max(relation->nb_output_dims, dim);
        node->nb_input_dims  = relation->nb_input_dims;
        node->nb_local_dims  = relation->nb_local_dims;
        node->nb_parameters  = relation->nb_parameters;

        // Copy of the original relation with some 0 columns for the new dimensions
        // Note that we use the fact that the matrix is initialized with zeros.
        for (i = 0; i < relation->nb_rows; i++) {
            for (j = 0; j <= relation->nb_output_dims; j++)
                osl_int_assign(precision, &node->m[i][j], relation->m[i][j]);

            for (j = relation->nb_output_dims + offset + 1;
                    j < relation->nb_columns + offset; j++)
                osl_int_assign(precision, &node->m[i][j], relation->m[i][j - offset]);
        }

        // New rows dedicated to the new dimensions
        for (i = relation->nb_rows; i < relation->nb_rows + offset; i++) {
            for (j = 0; j < relation->nb_columns + offset; j++) {
                if ((i - relation->nb_rows) == (j - relation->nb_output_dims - 1))
                    osl_int_set_si(precision, &node->m[i][j], -1);
            }
        }

        if (first) {
            first = 0;
            extended = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        relation = relation->next;
    }

    return extended;
}


/**
 * osl_relation_interface function:
 * this function creates an interface structure corresponding to the relation
 * and returns it.
 * \return An interface structure for the relation structure.
 */
osl_interface_p osl_relation_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_RELATION);
    interface->idump  = (osl_idump_f)osl_relation_idump;
    interface->sprint = (osl_sprint_f)osl_relation_sprint;
    interface->sread  = (osl_sread_f)osl_relation_sread;
    interface->malloc = (osl_malloc_f)osl_relation_malloc;
    interface->free   = (osl_free_f)osl_relation_free;
    interface->clone  = (osl_clone_f)osl_relation_clone;
    interface->equal  = (osl_equal_f)osl_relation_equal;

    return interface;
}


/**
 * osl_relation_set_precision function:
 * this function changes the precision of the osl_relation
 * \param[in]     precision Precision wanted for the relation
 * \param[in,out] r         A osl relation to change the precision
 */
void osl_relation_set_precision(int const precision, osl_relation_p r) {
    while (r != NULL) {
        if (precision != r->precision) {
            size_t i;
            size_t j;
            for (i = 0; i < (size_t)r->nb_rows; ++i) {
                for (j = 0; j < (size_t)r->nb_columns; ++j) {
                    osl_int_set_precision(r->precision, precision, &r->m[i][j]);
                }
            }
            r->precision = precision;
        }
        r = r->next;
    }
}


/**
 * osl_relation_set_same_precision function:
 * this function gets the highest precision of the relations
 * and set this precision to the other relation if necessary
 * \param[in,out] a A osl relation to change the precision if necessary
 * \param[in,out] b A osl relation to change the precision if necessary
 */
void osl_relation_set_same_precision(osl_relation_p a, osl_relation_p b) {
    if (a != NULL && b != NULL && a->precision != b->precision) {
        if (a->precision == OSL_PRECISION_MP || b->precision == OSL_PRECISION_MP) {
            osl_relation_set_precision(OSL_PRECISION_MP, a);
            osl_relation_set_precision(OSL_PRECISION_MP, b);
        } else if (a->precision == OSL_PRECISION_DP || b->precision == OSL_PRECISION_DP) {
            osl_relation_set_precision(OSL_PRECISION_DP, a);
            osl_relation_set_precision(OSL_PRECISION_DP, b);
        }
    }
}

/**
 * Removes a union part from the relation union.
 * Does not perform deep relation comparison, only pointer comparison.
 * If the union list does not contain the relation pointed to by #part, does
 * nothing.
 * \param[in,out] relation_list Pointer to the head of relation union list.
 * \param[in]     part          The part to remove (must be in the list).
 */
void osl_relation_remove_part(osl_relation_p *relation_list,
                              osl_relation_p part) {
    osl_relation_p relation, previous;

    if (relation_list == NULL || *relation_list == NULL || part == NULL) {
        return;
    }

    if (*relation_list == part) {
        *relation_list = (*relation_list)->next;
        return;
    }

    previous = *relation_list;
    for (relation = (*relation_list)->next; relation != NULL;
            relation = relation->next) {
        if (relation == part) {
            previous->next = relation->next;
            relation->next = NULL;
            osl_relation_free(relation);
            return;
        }
        previous = relation;
    }
}

