
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                          statement.c                            **
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
#include <osl/util.h>
#include <osl/strings.h>
#include <osl/body.h>
#include <osl/relation.h>
#include <osl/relation_list.h>
#include <osl/names.h>
#include <osl/interface.h>
#include <osl/generic.h>
#include <osl/statement.h>


/*+***************************************************************************
 *                         Structure display functions                       *
 *****************************************************************************/


/**
 * osl_statement_idump function:
 * this function displays an osl_statement_t structure (*statement) into
 * a file (file, possibly stdout) in a way that trends to be understandable.
 * It includes an indentation level (level) in order to work with others
 * dumping functions.
 * \param[in] file      File where the information has to be printed.
 * \param[in] statement The statement whose information has to be printed.
 * \param[in] level     Number of spaces before printing, for each line.
 */
void osl_statement_idump(FILE * file, osl_statement_p statement, int level) {
    int j, first = 1, number = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (statement != NULL)
        fprintf(file, "+-- osl_statement_t (S%d)\n", number);
    else
        fprintf(file, "+-- NULL statement\n");

    while (statement != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_statement_t (S%d)\n", number);
        } else
            first = 0;

        // A blank line.
        for (j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the domain of the statement.
        osl_relation_idump(file, statement->domain, level + 1);

        // Print the scattering of the statement.
        osl_relation_idump(file, statement->scattering, level + 1);

        // Print the array access information of the statement.
        osl_relation_list_idump(file, statement->access, level + 1);

        // Print the original body expression.
        osl_generic_idump(file, statement->extension, level + 1);

        statement = statement->next;
        number++;

        // Next line.
        if (statement != NULL) {
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
 * osl_statement_dump function:
 * this function prints the content of an osl_statement_t structure
 * (*statement) into  a file (file, possibly stdout).
 * \param[in] file      The file where the information has to be printed.
 * \param[in] statement The statement whose information has to be printed.
 */
void osl_statement_dump(FILE * file, osl_statement_p statement) {
    osl_statement_idump(file, statement, 0);
}


/**
 * osl_statement_names function:
 * this function generates as set of names for all the dimensions
 * involved in a given statement.
 * \param[in] statement The statement (list) we have to generate names for.
 * \return A set of generated names for the input statement dimensions.
 */
static
osl_names_p osl_statement_names(osl_statement_p statement) {
    int nb_parameters = OSL_UNDEFINED;
    int nb_iterators  = OSL_UNDEFINED;
    int nb_scattdims  = OSL_UNDEFINED;
    int nb_localdims  = OSL_UNDEFINED;
    int array_id      = OSL_UNDEFINED;

    osl_statement_get_attributes(statement, &nb_parameters, &nb_iterators,
                                 &nb_scattdims,  &nb_localdims, &array_id);

    return osl_names_generate("P", nb_parameters,
                              "i", nb_iterators,
                              "c", nb_scattdims,
                              "l", nb_localdims,
                              "A", array_id);
}


/**
 * osl_statement_pprint function:
 * this function pretty-prints the content of an osl_statement_t structure
 * (*statement) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file      The file where the information has to be printed.
 * \param[in] statement The statement whose information has to be printed.
 * \param[in] names     The names of the constraint columns for comments.
 */
void osl_statement_pprint(FILE * file, osl_statement_p statement,
                          osl_names_p names) {
    size_t nb_relations;
    int number = 1;
    int generated_names = 0;
    int iterators_backedup = 0;
    int nb_ext = 0;
    osl_body_p body = NULL;
    osl_strings_p iterators_backup = NULL;

    // Generate the dimension names if necessary and replace iterators with
    // statement iterators if possible.
    if (names == NULL) {
        generated_names = 1;
        names = osl_statement_names(statement);
    }

    while (statement != NULL) {
        // If possible, replace iterator names with statement iterator names.
        body = (osl_body_p)osl_generic_lookup(statement->extension, OSL_URI_BODY);
        if (body && body->iterators != NULL) {
            iterators_backedup = 1;
            iterators_backup = names->iterators;
            names->iterators = body->iterators;
        }

        nb_relations = 0;

        fprintf(file, "# =============================================== ");
        fprintf(file, "Statement %d\n", number);

        fprintf(file, "# Number of relations describing the statement:\n");

        if (statement->domain != NULL)
            nb_relations ++;
        if (statement->scattering != NULL)
            nb_relations ++;
        nb_relations += osl_relation_list_count(statement->access);

        fprintf(file, "%lu\n\n", nb_relations);

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.1 Domain\n", number);
        osl_relation_pprint(file, statement->domain, names);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.2 Scattering\n", number);
        osl_relation_pprint(file, statement->scattering, names);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.3 Access\n", number);
        osl_relation_list_pprint_elts(file, statement->access, names);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.4 Statement Extensions\n", number);
        fprintf(file, "# Number of Statement Extensions\n");
        nb_ext = osl_generic_number(statement->extension);
        fprintf(file, "%d\n", nb_ext);
        if(nb_ext>0)
            osl_generic_print(file, statement->extension);

        fprintf(file, "\n");

        // If necessary, switch back iterator names.
        if (iterators_backedup) {
            iterators_backedup = 0;
            names->iterators = iterators_backup;
        }

        statement = statement->next;
        number++;
    }

    if (generated_names)
        osl_names_free(names);
}


/**
 * osl_statement_pprint_scoplib function:
 * this function pretty-prints the content of an osl_statement_t structure
 * (*statement) into a file (file, possibly stdout) in the SCoPLib format.
 * \param[in] file      The file where the information has to be printed.
 * \param[in] statement The statement whose information has to be printed.
 * \param[in] names     The names of the constraint columns for comments.
 */
void osl_statement_pprint_scoplib(FILE * file, osl_statement_p statement,
                                  osl_names_p names) {
    int number = 1;
    int generated_names = 0;
    int iterators_backedup = 0;
    osl_body_p body = NULL;
    osl_strings_p iterators_backup = NULL;
    int add_fakeiter;

    // Generate the dimension names if necessary and replace iterators with
    // statement iterators if possible.
    if (names == NULL) {
        generated_names = 1;
        names = osl_statement_names(statement);
    }

    while (statement != NULL) {
        // If possible, replace iterator names with statement iterator names.
        body = (osl_body_p)osl_generic_lookup(statement->extension, OSL_URI_BODY);
        if (body && body->iterators != NULL) {
            iterators_backedup = 1;
            iterators_backup = names->iterators;
            names->iterators = body->iterators;
        }

        add_fakeiter = statement->domain->nb_rows == 0 &&
                       statement->scattering->nb_rows == 1;

        fprintf(file, "# =============================================== ");
        fprintf(file, "Statement %d\n", number);

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.1 Domain\n", number);
        fprintf(file, "# Iteration domain\n");
        osl_relation_pprint_scoplib(file, statement->domain, names, 1, add_fakeiter);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.2 Scattering\n", number);
        fprintf(file,"# Scattering function is provided\n1\n");
        osl_relation_pprint_scoplib(file, statement->scattering, names, 0,
                                    add_fakeiter);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.3 Access\n", number);
        fprintf(file,"# Access informations are provided\n1\n");

        osl_relation_list_pprint_access_array_scoplib(file, statement->access,
                names, add_fakeiter);
        fprintf(file, "\n");

        fprintf(file, "# ---------------------------------------------- ");
        fprintf(file, "%2d.4 Body\n", number);
        if (body != NULL) {
            fprintf(file, "# Statement body is provided\n1\n");
            osl_body_print_scoplib(file, body);
            body = NULL; //re-initialize for next statement
        } else {
            fprintf(file, "# Statement body is not provided\n0\n");
        }

        fprintf(file, "\n");

        // If necessary, switch back iterator names.
        if (iterators_backedup) {
            iterators_backedup = 0;
            names->iterators = iterators_backup;
        }

        statement = statement->next;
        number++;
    }

    if (generated_names)
        osl_names_free(names);
}


/**
 * osl_statement_print function:
 * this function prints the content of an osl_statement_t structure
 * (*statement) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file      The file where the information has to be printed.
 * \param[in] statement The statement whose information has to be printed.
 */
void osl_statement_print(FILE * file, osl_statement_p statement) {

    osl_statement_pprint(file, statement, NULL);
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_statement_dispatch function:
 * this function dispatches the relations from a relation list to the
 * convenient fields of a statement structure: it extracts the domain,
 * the scattering and the access list and store them accordingly in the
 * statement structure provided as a parameter.
 * \param[in,out] stmt The statement where to dispatch the relations.
 * \param[in,out] list The "brute" relation list to sort and dispatch (freed).
 */
static
void osl_statement_dispatch(osl_statement_p stmt, osl_relation_list_p list) {
    osl_relation_list_p domain_list;
    osl_relation_list_p scattering_list;
    size_t nb_domains, nb_scattering, nb_accesses;

    // Domain.
    domain_list = osl_relation_list_filter(list, OSL_TYPE_DOMAIN);
    nb_domains = osl_relation_list_count(domain_list);
    if (nb_domains > 1)
        OSL_error("more than one domain for a statement");

    if (domain_list != NULL) {
        stmt->domain = domain_list->elt;
        domain_list->elt = NULL;
        osl_relation_list_free(domain_list);
    } else {
        stmt->domain = NULL;
    }

    // Scattering.
    scattering_list=osl_relation_list_filter(list,OSL_TYPE_SCATTERING);
    nb_scattering = osl_relation_list_count(scattering_list);
    if (nb_scattering > 1)
        OSL_error("more than one scattering relation for a statement");

    if (scattering_list != NULL) {
        stmt->scattering = scattering_list->elt;
        scattering_list->elt = NULL;
        osl_relation_list_free(scattering_list);
    } else {
        stmt->scattering = NULL;
    }

    // Access.
    stmt->access = osl_relation_list_filter(list, OSL_TYPE_ACCESS);
    nb_accesses = osl_relation_list_count(stmt->access);

    if ((nb_domains + nb_scattering + nb_accesses) !=
            (osl_relation_list_count(list)))
        OSL_error("unexpected relation type to define a statement");

    osl_relation_list_free(list);
}


/**
 * osl_statement_pread function ("precision read"):
 * this function reads an osl_statement_t structure from an input stream
 * (possibly stdin).
 * \param[in] file      The input stream.
 * \param[in] registry  The list of known interfaces (others are ignored).
 * \param[in] precision The precision of the relation elements.
 * \return A pointer to the statement structure that has been read.
 */
osl_statement_p osl_statement_pread(FILE * file, osl_interface_p registry,
                                    int precision) {
    osl_statement_p stmt = osl_statement_malloc();
    osl_relation_list_p list;
    osl_generic_p new = NULL;
    int i, nb_ext = 0;

    if (file) {
        // Read all statement relations.
        list = osl_relation_list_pread(file, precision);

        // Store relations at the right place according to their type.
        osl_statement_dispatch(stmt, list);

        // Read the Extensions
        nb_ext = osl_util_read_int(file, NULL);
        for (i=0; i<nb_ext; i++) {
            new = osl_generic_read_one(file, registry);
            osl_generic_add(&stmt->extension, new);
        }
    }

    return stmt;
}


/**
 * osl_statement_read function:
 * this function is equivalent to osl_statement_pread() except that
 * (1) the precision corresponds to the precision environment variable or
 *     to the highest available precision if it is not defined, and
 * (2) the list of known interface is set to the default one.
 * \see{osl_statement_pread}
 */
osl_statement_p osl_statement_read(FILE * foo) {
    int precision = osl_util_get_precision();
    osl_interface_p registry = osl_interface_get_default_registry();
    osl_statement_p statement = osl_statement_pread(foo, registry, precision);

    osl_interface_free(registry);
    return statement;
}


/*+***************************************************************************
 *                   Memory allocation/deallocation functions                *
 *****************************************************************************/


/**
 * osl_statement_malloc function:
 * this function allocates the memory space for an osl_statement_t
 * structure and sets its fields with default values. Then it returns a pointer
 * to the allocated space.
 * \return A pointer to an empty statement with fields set to default values.
 */
osl_statement_p osl_statement_malloc(void) {
    osl_statement_p statement;

    OSL_malloc(statement, osl_statement_p, sizeof(osl_statement_t));
    statement->domain     = NULL;
    statement->scattering = NULL;
    statement->access     = NULL;
    statement->extension  = NULL;
    statement->next       = NULL;

    return statement;
}


/**
 * osl_statement_free function:
 * this function frees the allocated memory for an osl_statement_t
 * structure.
 * \param[in,out] statement The pointer to the statement we want to free.
 */
void osl_statement_free(osl_statement_p statement) {
    osl_statement_p next;

    while (statement != NULL) {
        next = statement->next;
        osl_relation_free(statement->domain);
        osl_relation_free(statement->scattering);
        osl_relation_list_free(statement->access);
        osl_generic_free(statement->extension);

        free(statement);
        statement = next;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_statement_add function:
 * this function adds a statement "statement" at the end of the statement
 * list pointed by "location".
 * \param[in,out] location  Address of the first element of the statement list.
 * \param[in]     statement The statement to add to the list.
 */
void osl_statement_add(osl_statement_p * location,
                       osl_statement_p   statement) {
    while (*location != NULL)
        location = &((*location)->next);

    *location = statement;
}


/**
 * osl_statement_number function:
 * this function returns the number of statements in the statement list
 * provided as parameter.
 * \param[in] statement The first element of the statement list.
 * \return The number of statements in the statement list.
 */
int osl_statement_number(osl_statement_p statement) {
    int number = 0;

    while (statement != NULL) {
        number++;
        statement = statement->next;
    }
    return number;
}


/**
 * osl_statement_nclone function:
 * This function builds and returns a "hard copy" (not a pointer copy) of the
 * n first elements of an osl_statement_t list.
 * \param statement The pointer to the statement structure we want to clone.
 * \param n         The number of nodes we want to copy (-1 for infinity).
 * \return The clone of the n first nodes of the statement list.
 */
osl_statement_p osl_statement_nclone(osl_statement_p statement, int n) {
    int first = 1, i = 0;
    osl_statement_p clone = NULL, node, previous = NULL;

    while ((statement != NULL) && ((n == -1) || (i < n))) {
        node             = osl_statement_malloc();
        node->domain     = osl_relation_clone(statement->domain);
        node->scattering = osl_relation_clone(statement->scattering);
        node->access     = osl_relation_list_clone(statement->access);
        node->extension  = osl_generic_clone(statement->extension);
        node->next       = NULL;

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        i++;
        statement = statement->next;
    }

    return clone;
}


/**
 * osl_statement_clone function:
 * This functions builds and returns a "hard copy" (not a pointer copy) of an
 * osl_statement_t data structure provided as parameter.
 * \param[in] statement The pointer to the statement we want to clone.
 * \return A pointer to the clone of the statement provided as parameter.
 */
osl_statement_p osl_statement_clone(osl_statement_p statement) {
    return osl_statement_nclone(statement, -1);
}

/// Clone first part of the union, return NULL if input is NULL.
static osl_relation_p osl_relation_clone_one_safe(osl_relation_p relation) {
    if (relation == NULL)
        return NULL;
    return osl_relation_nclone(relation, 1);
}

/**
 * osl_statement_remove_unions function:
 * Create a statement list from a statement containing unions of relations in
 * domain and/or scattering.  This functions constructs multiple new statements
 * and keeps the original statement intact.  Each new statement has exactly one
 * domain union component and exactly one scattering union component.  If the
 * statement does not have the domain relation or the scattering relation, this
 * function returns \c NULL.
 * \param[in] statement A pointer to the statement
 * \return    A pointer to the head of the newly created statement list.
 */
osl_statement_p osl_statement_remove_unions(osl_statement_p statement) {
    osl_relation_p domain, scattering;
    osl_statement_p statement_ptr = NULL, result;
    if (!statement)
        return NULL;

    // Make at least one new statement, even if there are no relations.
    domain = statement->domain;
    result = NULL;
    do {
        scattering = statement->scattering;
        do {
            osl_statement_p new_statement = osl_statement_malloc();
            new_statement->domain = osl_relation_clone_one_safe(domain);
            new_statement->scattering = osl_relation_clone_one_safe(scattering);
            new_statement->access = osl_relation_list_clone(statement->access);
            new_statement->extension = osl_generic_clone(statement->extension);
            if (!result) {
                statement_ptr = new_statement;
                result = new_statement;
            } else {
                statement_ptr->next = new_statement;
                statement_ptr = statement_ptr->next;
            }
            if (scattering == NULL || scattering->next == NULL)
                break;
            scattering = scattering->next;
        } while (1);
        if (domain == NULL || domain->next == NULL)
            break;
        domain = domain->next;
    } while (1);

    return result;
}



/**
 * osl_statement_equal function:
 * this function returns true if the two statements provided as parameters
 * are the same, false otherwise (the usr field is not tested).
 * \param[in] s1 The first statement.
 * \param[in] s2 The second statement.
 * \return 1 if s1 and s2 are the same (content-wise), 0 otherwise.
 */
int osl_statement_equal(osl_statement_p s1, osl_statement_p s2) {

    if (s1 == s2)
        return 1;

    if (((s1->next != NULL) && (s2->next == NULL)) ||
            ((s1->next == NULL) && (s2->next != NULL))) {
        OSL_info("statements are not the same");
        return 0;
    }

    if ((s1->next != NULL) && (s2->next != NULL)) {
        if (!osl_statement_equal(s1->next, s2->next)) {
            OSL_info("number of statements is not the same");
            return 0;
        }
    }

    if (!osl_relation_equal(s1->domain, s2->domain)) {
        OSL_info("statement domains are not the same");
        return 0;
    }

    if (!osl_relation_equal(s1->scattering, s2->scattering)) {
        OSL_info("statement scatterings are not the same");
        return 0;
    }

    if (!osl_relation_list_equal(s1->access, s2->access)) {
        OSL_info("statement accesses are not the same");
        return 0;
    }

    if (!osl_generic_equal(s1->extension, s2->extension)) {
        OSL_info("statement bodies are not the same");
        return 0;
    }

    return 1;
}


/**
 * osl_statement_integrity_check function:
 * this function checks that a statement is "well formed" according to some
 * expected properties (setting an expected value to OSL_UNDEFINED means
 * that we do not expect a specific value). It returns 0 if the check failed
 * or 1 if no problem has been detected.
 * \param[in] statement              The statement we want to check.
 * \param[in] expected_nb_parameters Expected number of parameters.
 * \return 0 if the integrity check fails, 1 otherwise.
 */
int osl_statement_integrity_check(osl_statement_p statement,
                                  int expected_nb_parameters) {
    int expected_nb_iterators;
    osl_body_p body = NULL;

    while (statement != NULL) {
        // Check the domain.
        if (!osl_relation_integrity_check(statement->domain,
                                          OSL_TYPE_DOMAIN,
                                          OSL_UNDEFINED,
                                          0,
                                          expected_nb_parameters)) {
            return 0;
        }

        // Get the number of iterators.
        if (statement->domain != NULL)
            expected_nb_iterators = statement->domain->nb_output_dims;
        else
            expected_nb_iterators = OSL_UNDEFINED;

        // Check the scattering relation.
        if (!osl_relation_integrity_check(statement->scattering,
                                          OSL_TYPE_SCATTERING,
                                          OSL_UNDEFINED,
                                          expected_nb_iterators,
                                          expected_nb_parameters)) {
            return 0;
        }

        // Check the access relations.
        if (!osl_relation_list_integrity_check(statement->access,
                                               OSL_TYPE_ACCESS,
                                               OSL_UNDEFINED,
                                               expected_nb_iterators,
                                               expected_nb_parameters)) {
            return 0;
        }

        // Check the statement body.
        body = (osl_body_p)osl_generic_lookup(statement->extension, OSL_URI_BODY);
        if ((expected_nb_iterators != OSL_UNDEFINED) &&
                body && body->iterators != NULL &&
                ((size_t)expected_nb_iterators != osl_strings_size(body->iterators))) {
            OSL_warning("unexpected number of original iterators");
            return 0;
        }

        statement = statement->next;
    }

    return 1;
}


/**
 * osl_statement_get_nb_iterators function:
 * this function returns the number of surroounding iterators of a given
 * statement.
 * \param statement The statement we want to know the number of iterators.
 * \return The number of surrounding iterators for the statement.
 */
int osl_statement_get_nb_iterators(osl_statement_p statement) {

    if (statement->domain == NULL) {
        OSL_warning("no statement domain, assuming 0 iterators");
        return 0;
    } else {
        return statement->domain->nb_output_dims;
    }
}


/**
 * osl_statement_get_attributes function:
 * this function returns, through its parameters, the maximum values of the
 * relation attributes (nb_iterators, nb_parameters etc) in the statement.
 * HOWEVER, it updates the parameter value iff the attribute is greater than
 * the input parameter value. Hence it may be used to get the attributes as
 * well as to find the maximum attributes for several statement lists. The
 * array identifier 0 is used when there is no array identifier (AND this is
 * OK), OSL_UNDEFINED is used to report it is impossible to provide the
 * property while it should. This function is not intended for checking, the
 * input statement should be correct.
 * \param[in]     statement     The statement to extract attributes values.
 * \param[in,out] nb_parameters Number of parameter attribute.
 * \param[in,out] nb_iterators  Number of iterators attribute.
 * \param[in,out] nb_scattdims  Number of scattering dimensions attribute.
 * \param[in,out] nb_localdims  Number of local dimensions attribute.
 * \param[in,out] array_id      Maximum array identifier attribute.
 */
void osl_statement_get_attributes(osl_statement_p statement,
                                  int * nb_parameters,
                                  int * nb_iterators,
                                  int * nb_scattdims,
                                  int * nb_localdims,
                                  int * array_id) {
    int local_nb_parameters = OSL_UNDEFINED;
    int local_nb_iterators  = OSL_UNDEFINED;
    int local_nb_scattdims  = OSL_UNDEFINED;
    int local_nb_localdims  = OSL_UNDEFINED;
    int local_array_id      = OSL_UNDEFINED;

    while (statement != NULL) {
        osl_relation_get_attributes(statement->domain,
                                    &local_nb_parameters,
                                    &local_nb_iterators,
                                    &local_nb_scattdims,
                                    &local_nb_localdims,
                                    &local_array_id);

        osl_relation_get_attributes(statement->scattering,
                                    &local_nb_parameters,
                                    &local_nb_iterators,
                                    &local_nb_scattdims,
                                    &local_nb_localdims,
                                    &local_array_id);

        osl_relation_list_get_attributes(statement->access,
                                         &local_nb_parameters,
                                         &local_nb_iterators,
                                         &local_nb_scattdims,
                                         &local_nb_localdims,
                                         &local_array_id);
        // Update.
        *nb_parameters = OSL_max(*nb_parameters, local_nb_parameters);
        *nb_iterators  = OSL_max(*nb_iterators,  local_nb_iterators);
        *nb_scattdims  = OSL_max(*nb_scattdims,  local_nb_scattdims);
        *nb_localdims  = OSL_max(*nb_localdims,  local_nb_localdims);
        *array_id      = OSL_max(*array_id,      local_array_id);
        statement = statement->next;
    }
}


/**
 * osl_statement_get_body function:
 * this function returns the body of the statement.
 * \param[in] statement The statement to search the body.
 * \return the body if found, NULL otherwise.
 */
osl_body_p osl_statement_get_body(osl_statement_p statement) {
    osl_body_p body;
    osl_extbody_p ebody;

    if (statement == NULL || statement->extension == NULL) {
        return NULL;
    }

    body = (osl_body_p)osl_generic_lookup(statement->extension, OSL_URI_BODY);
    if (body != NULL)
        return body;
    ebody = (osl_extbody_p)osl_generic_lookup(statement->extension,
            OSL_URI_EXTBODY);
    if (ebody != NULL)
        return ebody->body;
    return NULL;
}
