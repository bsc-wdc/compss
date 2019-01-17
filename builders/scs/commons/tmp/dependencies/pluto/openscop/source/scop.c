
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                            scop.c                               **
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

# include <stdlib.h>
# include <stdio.h>
# include <ctype.h>
# include <string.h>

#include <osl/macros.h>
#include <osl/util.h>
#include <osl/extensions/arrays.h>
#include <osl/extensions/textual.h>
#include <osl/strings.h>
#include <osl/relation.h>
#include <osl/interface.h>
#include <osl/generic.h>
#include <osl/statement.h>
#include <osl/scop.h>


/*+***************************************************************************
 *                         Structure display functions                       *
 *****************************************************************************/


/**
 * osl_scop_idump function:
 * this function displays an osl_scop_t structure (*scop) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param file  The file where the information has to be printed.
 * \param scop  The scop structure whose information has to be printed.
 * \param level Number of spaces before printing, for each line.
 */
void osl_scop_idump(FILE * file, osl_scop_p scop, int level) {
    int j, first = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (scop != NULL)
        fprintf(file, "+-- osl_scop_t\n");
    else
        fprintf(file, "+-- NULL scop\n");

    while (scop != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_scop_t\n");
        } else
            first = 0;

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the version.
        for (j = 0; j < level; j++)
            fprintf(file, "|\t");
        fprintf(file, "|\tVersion: %d\n", scop->version);

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the language.
        for (j = 0; j < level; j++)
            fprintf(file, "|\t");
        fprintf(file, "|\tLanguage: %s\n", scop->language);

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the context of the scop.
        osl_relation_idump(file, scop->context, level+1);

        // Print the parameters.
        osl_generic_idump(file, scop->parameters, level+1);

        // Print the statements.
        osl_statement_idump(file, scop->statement, level+1);

        // Print the registered extension interfaces.
        osl_interface_idump(file, scop->registry, level+1);

        // Print the extensions.
        osl_generic_idump(file, scop->extension, level+1);

        scop = scop->next;

        // Next line.
        if (scop != NULL) {
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
 * osl_scop_dump function:
 * this function prints the content of an osl_scop_t structure (*scop)
 * into a file (file, possibly stdout).
 * \param file The file where the information has to be printed.
 * \param scop The scop structure whose information has to be printed.
 */
void osl_scop_dump(FILE * file, osl_scop_p scop) {
    osl_scop_idump(file, scop, 0);
}


/**
 * osl_scop_names function:
 * this function generates as set of names for all the dimensions
 * involved in a given scop.
 * \param[in] scop The scop (list) we have to generate names for.
 * \return A set of generated names for the input scop dimensions.
 */
osl_names_p osl_scop_names(osl_scop_p scop) {
    int nb_parameters = OSL_UNDEFINED;
    int nb_iterators  = OSL_UNDEFINED;
    int nb_scattdims  = OSL_UNDEFINED;
    int nb_localdims  = OSL_UNDEFINED;
    int array_id      = OSL_UNDEFINED;

    osl_scop_get_attributes(scop, &nb_parameters, &nb_iterators,
                            &nb_scattdims,  &nb_localdims, &array_id);

    return osl_names_generate("P", nb_parameters,
                              "i", nb_iterators,
                              "c", nb_scattdims,
                              "l", nb_localdims,
                              "A", array_id);
}


/**
 * osl_scop_print function:
 * this function prints the content of an osl_scop_t structure (*scop)
 * into a file (file, possibly stdout) in the OpenScop textual format.
 * \param file The file where the information has to be printed.
 * \param scop The scop structure whose information has to be printed.
 */
void osl_scop_print(FILE * file, osl_scop_p scop) {
    int parameters_backedup = 0;
    int arrays_backedup = 0;
    osl_strings_p parameters_backup = NULL;
    osl_strings_p arrays_backup = NULL;
    osl_names_p names;
    osl_arrays_p arrays;

    if (scop == NULL) {
        fprintf(file, "# NULL scop\n");
        return;
    } else {
        fprintf(file, "# [File generated by the OpenScop Library %s]\n",
                OSL_RELEASE);
    }

    if (osl_scop_integrity_check(scop) == 0)
        OSL_warning("OpenScop integrity check failed. Something may go wrong.");

    // Generate the names for the various dimensions.
    names = osl_scop_names(scop);

    while (scop != NULL) {
        // If possible, replace parameter names with scop parameter names.
        if (osl_generic_has_URI(scop->parameters, OSL_URI_STRINGS)) {
            parameters_backedup = 1;
            parameters_backup = names->parameters;
            names->parameters = scop->parameters->data;
        }

        // If possible, replace array names with arrays extension names.
        arrays = osl_generic_lookup(scop->extension, OSL_URI_ARRAYS);
        if (arrays != NULL) {
            arrays_backedup = 1;
            arrays_backup = names->arrays;
            names->arrays = osl_arrays_to_strings(arrays);
        }

        fprintf(file, "\n<"OSL_URI_SCOP">\n\n");
        fprintf(file, "# =============================================== "
                "Global\n");
        fprintf(file, "# Language\n");
        fprintf(file, "%s\n\n", scop->language);

        fprintf(file, "# Context\n");
        osl_relation_pprint(file, scop->context, names);
        fprintf(file, "\n");

        osl_util_print_provided(file,
                                osl_generic_has_URI(scop->parameters, OSL_URI_STRINGS),
                                "Parameters are");
        osl_generic_print(file, scop->parameters);

        fprintf(file, "\n# Number of statements\n");
        fprintf(file, "%d\n\n",osl_statement_number(scop->statement));

        osl_statement_pprint(file, scop->statement, names);

        if (scop->extension) {
            fprintf(file, "# =============================================== "
                    "Extensions\n");
            osl_generic_print(file, scop->extension);
        }
        fprintf(file, "\n</"OSL_URI_SCOP">\n\n");

        // If necessary, switch back parameter names.
        if (parameters_backedup) {
            parameters_backedup = 0;
            names->parameters = parameters_backup;
        }

        // If necessary, switch back array names.
        if (arrays_backedup) {
            arrays_backedup = 0;
            osl_strings_free(names->arrays);
            names->arrays = arrays_backup;
        }

        scop = scop->next;
    }

    osl_names_free(names);
}


/**
 * osl_scop_print_scoplib function:
 * this function prints the content of an osl_scop_t structure (*scop)
 * into a file (file, possibly stdout) in the ScopLib textual format.
 * \param file The file where the information has to be printed.
 * \param scop The scop structure whose information has to be printed.
 */
void osl_scop_print_scoplib(FILE * file, osl_scop_p scop) {
    int parameters_backedup = 0;
    int arrays_backedup = 0;
    osl_strings_p parameters_backup = NULL;
    osl_strings_p arrays_backup = NULL;
    osl_names_p names;
    osl_arrays_p arrays;

    if (scop == NULL) {
        fprintf(file, "# NULL scop\n");
        return;
    } else {
        fprintf(file, "# [File generated by the OpenScop Library %s]\n"
                "# [SCoPLib format]\n",
                OSL_RELEASE);
    }

    if (osl_scop_check_compatible_scoplib(scop) == 0) {
        OSL_error("SCoP integrity check failed. Something may go wrong.");
        exit(1);
    }

    // Generate the names for the various dimensions.
    names = osl_scop_names(scop);

    while (scop != NULL) {
        // If possible, replace parameter names with scop parameter names.
        if (osl_generic_has_URI(scop->parameters, OSL_URI_STRINGS)) {
            parameters_backedup = 1;
            parameters_backup = names->parameters;
            names->parameters = scop->parameters->data;
        }

        // If possible, replace array names with arrays extension names.
        arrays = osl_generic_lookup(scop->extension, OSL_URI_ARRAYS);
        if (arrays != NULL) {
            arrays_backedup = 1;
            arrays_backup = names->arrays;
            names->arrays = osl_arrays_to_strings(arrays);
        }

        fprintf(file, "\nSCoP\n\n");
        fprintf(file, "# =============================================== "
                "Global\n");
        fprintf(file, "# Language\n");
        fprintf(file, "%s\n\n", scop->language);

        fprintf(file, "# Context\n");

        osl_relation_pprint_scoplib(file, scop->context, names, 0, 0);
        fprintf(file, "\n");

        osl_util_print_provided(file,
                                osl_generic_has_URI(scop->parameters, OSL_URI_STRINGS),
                                "Parameters are");

        if (scop->parameters) {
            fprintf(file, "# Parameter names\n");
            osl_strings_print(file, scop->parameters->data);
        }

        fprintf(file, "\n# Number of statements\n");
        fprintf(file, "%d\n\n",osl_statement_number(scop->statement));

        osl_statement_pprint_scoplib(file, scop->statement, names);

        if (scop->extension) {
            fprintf(file, "# =============================================== "
                    "Options\n");
            osl_generic_print_options_scoplib(file, scop->extension);
        }

        // If necessary, switch back parameter names.
        if (parameters_backedup) {
            parameters_backedup = 0;
            names->parameters = parameters_backup;
        }

        // If necessary, switch back array names.
        if (arrays_backedup) {
            arrays_backedup = 0;
            osl_strings_free(names->arrays);
            names->arrays = arrays_backup;
        }

        scop = scop->next;
    }

    osl_names_free(names);
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_scop_pread function ("precision read"):
 * this function reads a list of scop structures from a file (possibly stdin)
 * complying to the OpenScop textual format and returns a pointer to this
 * scop list. If some relation properties (number of input/output/local
 * dimensions and number of parameters) are undefined, it will define them
 * according to the available information.
 * \param[in] file      The file where the scop has to be read.
 * \param[in] registry  The list of known interfaces (others are ignored).
 * \param[in] precision The precision of the relation elements.
 * \return A pointer to the scop structure that has been read.
 */
osl_scop_p osl_scop_pread(FILE * file, osl_interface_p registry,
                          int precision) {
    osl_scop_p list = NULL, current = NULL, scop;
    osl_statement_p stmt = NULL;
    osl_statement_p prev = NULL;
    osl_strings_p language;
    int nb_statements;
    char * tmp;
    int first = 1;
    int i;

    if (file == NULL)
        return NULL;

    while(1) {
        //
        // I. START TAG
        //
        tmp = osl_util_read_uptotag(file, NULL, OSL_URI_SCOP);
        if (tmp == NULL) {
            OSL_debug("no more scop in the file");
            break;
        } else {
            free(tmp);
        }

        scop = osl_scop_malloc();
        scop->registry = osl_interface_clone(registry);

        //
        // II. CONTEXT PART
        //

        // Read the language.
        language = osl_strings_read(file);
        if (osl_strings_size(language) == 0)
            OSL_error("no language (backend) specified");

        if (osl_strings_size(language) > 1)
            OSL_warning("uninterpreted information (after language)");

        if (language != NULL) {
            OSL_strdup(scop->language, language->string[0]);
            osl_strings_free(language);
        }

        // Read the context domain.
        scop->context = osl_relation_pread(file, precision);

        // Read the parameters.
        if (osl_util_read_int(file, NULL) > 0)
            scop->parameters = osl_generic_read_one(file, scop->registry);

        //
        // III. STATEMENT PART
        //

        // Read the number of statements.
        nb_statements = osl_util_read_int(file, NULL);

        for (i = 0; i < nb_statements; i++) {
            // Read each statement.
            stmt = osl_statement_pread(file, scop->registry, precision);
            if (scop->statement == NULL)
                scop->statement = stmt;
            else
                prev->next = stmt;
            prev = stmt;
        }

        //
        // IV. EXTENSION PART (TO THE END TAG)
        //

        // Read up the end tag (if any), and store extensions.
        scop->extension = osl_generic_read(file, scop->registry);

        // Add the new scop to the list.
        if (first) {
            list = scop;
            first = 0;
        } else {
            current->next = scop;
        }
        current = scop;
    }

    if (!osl_scop_integrity_check(list))
        OSL_warning("scop integrity check failed");

    return list;
}


/**
 * osl_scop_read function:
 * this function is equivalent to osl_scop_pread() except that
 * (1) the precision corresponds to the precision environment variable or
 *     to the highest available precision if it is not defined, and
 * (2) the list of known interface is set to the default one.
 * \see{osl_scop_pread}
 */
osl_scop_p osl_scop_read(FILE * foo) {
    int precision = osl_util_get_precision();
    osl_interface_p registry = osl_interface_get_default_registry();
    osl_scop_p scop = osl_scop_pread(foo, registry, precision);

    osl_interface_free(registry);
    return scop;
}


/*+***************************************************************************
 *                   Memory allocation/deallocation functions                *
 *****************************************************************************/


/**
 * osl_scop_malloc function:
 * this function allocates the memory space for a osl_scop_t structure and
 * sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * \return A pointer to an empty scop with fields set to default values.
 */
osl_scop_p osl_scop_malloc(void) {
    osl_scop_p scop;

    OSL_malloc(scop, osl_scop_p, sizeof(osl_scop_t));
    scop->version        = 1;
    scop->language       = NULL;
    scop->context        = NULL;
    scop->parameters     = NULL;
    scop->statement      = NULL;
    scop->registry       = NULL;
    scop->extension      = NULL;
    scop->usr            = NULL;
    scop->next           = NULL;

    return scop;
}


/**
 * osl_scop_free function:
 * This function frees the allocated memory for a osl_scop_t structure.
 * \param scop The pointer to the scop we want to free.
 */
void osl_scop_free(osl_scop_p scop) {
    osl_scop_p tmp;

    while (scop != NULL) {
        if (scop->language != NULL)
            free(scop->language);
        osl_generic_free(scop->parameters);
        osl_relation_free(scop->context);
        osl_statement_free(scop->statement);
        osl_interface_free(scop->registry);
        osl_generic_free(scop->extension);

        tmp = scop->next;
        free(scop);
        scop = tmp;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_scop_add function:
 * this function adds a scop "scop" at the end of the scop list pointed
 * by "location".
 * \param[in,out] location  Address of the first element of the scop list.
 * \param[in]     scop      The scop to add to the list.
 */
void osl_scop_add(osl_scop_p * location, osl_scop_p scop) {
    while (*location != NULL)
        location = &((*location)->next);

    *location = scop;
}


/**
 * osl_scop_number function:
 * this function returns the number of scops in the scop list
 * provided as parameter.
 * \param[in] scop The first element of the scop list.
 * \return The number of scops in the scop list.
 */
size_t osl_scop_number(osl_scop_p scop) {
    size_t number = 0;

    while (scop != NULL) {
        number++;
        scop = scop->next;
    }
    return number;
}


/**
 * osl_scop_clone function:
 * This functions builds and returns a "hard copy" (not a pointer copy)
 * of a osl_statement_t data structure provided as parameter.
 * Note that the usr field is not touched by this function.
 * \param scop The pointer to the scop we want to clone.
 * \return A pointer to the full clone of the scop provided as parameter.
 */
osl_scop_p osl_scop_clone(osl_scop_p scop) {
    osl_scop_p clone = NULL, node, previous = NULL;
    int first = 1;

    while (scop != NULL) {
        node                 = osl_scop_malloc();
        node->version        = scop->version;
        if (scop->language != NULL)
            OSL_strdup(node->language, scop->language);
        node->context        = osl_relation_clone(scop->context);
        node->parameters     = osl_generic_clone(scop->parameters);
        node->statement      = osl_statement_clone(scop->statement);
        node->registry       = osl_interface_clone(scop->registry);
        node->extension      = osl_generic_clone(scop->extension);

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        scop = scop->next;
    }

    return clone;
}

/**
 * osl_scop_remove_unions function:
 * Replace each statement having unions of relations by a list of statements,
 * each of which has exactly one domain relation and one scattering relation.
 * \param[in] scop A SCoP with statements featuring unions of relations.
 * \returns  An identical SCoP without unions of relations.
 */
osl_scop_p osl_scop_remove_unions(osl_scop_p scop) {
    osl_statement_p statement, new_statement, scop_statement_ptr;
    osl_scop_p new_scop, scop_ptr, result = NULL;

    for ( ; scop != NULL; scop = scop->next) {
        statement = scop->statement;
        scop_statement_ptr = NULL;
        new_scop = osl_scop_malloc();

        for ( ; statement != NULL; statement = statement->next) {
            new_statement = osl_statement_remove_unions(statement);
            if (!scop_statement_ptr) {
                scop_statement_ptr = new_statement;
                new_scop->statement = scop_statement_ptr;
            } else {
                scop_statement_ptr->next = new_statement;
                scop_statement_ptr = scop_statement_ptr->next;
            }
        }
        while (scop_statement_ptr && scop_statement_ptr->next != NULL)
            scop_statement_ptr = scop_statement_ptr->next;

        new_scop->context = osl_relation_clone(scop->context);
        new_scop->extension = osl_generic_clone(scop->extension);
        if (scop->language != NULL) {
            new_scop->language = (char *) malloc(strlen(scop->language) + 1);
            new_scop->language = strcpy(new_scop->language, scop->language);
        }
        new_scop->parameters = osl_generic_clone(scop->parameters);
        new_scop->registry = osl_interface_clone(scop->registry);
        new_scop->version = scop->version;
        if (!result) {
            result = new_scop;
            scop_ptr = new_scop;
        } else {
            scop_ptr->next = new_scop;
            scop_ptr = scop_ptr->next;
        }
    }

    return result;
}

/**
 * osl_scop_equal function:
 * this function returns true if the two scops are the same, false
 * otherwise (the usr field is not tested).
 * \param s1 The first scop.
 * \param s2 The second scop.
 * \return 1 if s1 and s2 are the same (content-wise), 0 otherwise.
 */
int osl_scop_equal(osl_scop_p s1, osl_scop_p s2) {

    while ((s1 != NULL) && (s2 != NULL)) {
        if (s1 == s2)
            return 1;

        if (s1->version != s2->version) {
            OSL_info("versions are not the same");
            return 0;
        }

        if (strcmp(s1->language, s2->language) != 0) {
            OSL_info("languages are not the same");
            return 0;
        }

        if (!osl_relation_equal(s1->context, s2->context)) {
            OSL_info("contexts are not the same");
            return 0;
        }

        if (!osl_generic_equal(s1->parameters, s2->parameters)) {
            OSL_info("parameters are not the same");
            return 0;
        }

        if (!osl_statement_equal(s1->statement, s2->statement)) {
            OSL_info("statements are not the same");
            return 0;
        }

        if (!osl_interface_equal(s1->registry, s2->registry)) {
            OSL_info("registries are not the same");
            return 0;
        }

        if (!osl_generic_equal(s1->extension, s2->extension)) {
            OSL_info("extensions are not the same");
            return 0;
        }

        s1 = s1->next;
        s2 = s2->next;
    }

    if (((s1 == NULL) && (s2 != NULL)) || ((s1 != NULL) && (s2 == NULL)))
        return 0;

    return 1;
}


/**
 * osl_scop_integrity_check function:
 * This function checks that a scop is "well formed". It returns 0 if the
 * check failed or 1 if no problem has been detected.
 * \param scop  The scop we want to check.
 * \return 0 if the integrity check fails, 1 otherwise.
 */
int osl_scop_integrity_check(osl_scop_p scop) {
    int expected_nb_parameters;


    while (scop != NULL) {
        // Check the language.
        if ((scop->language != NULL) &&
                (!strcmp(scop->language, "caml")  || !strcmp(scop->language, "Caml") ||
                 !strcmp(scop->language, "ocaml") || !strcmp(scop->language, "OCaml")))
            fprintf(stderr, "[OpenScop] Alert: What ?! Caml ?! Are you sure ?!?!\n");

        // Check the context.
        if (!osl_relation_integrity_check(scop->context,
                                          OSL_TYPE_CONTEXT,
                                          OSL_UNDEFINED,
                                          OSL_UNDEFINED,
                                          OSL_UNDEFINED))
            return 0;

        // Get the number of parameters.
        if (scop->context != NULL)
            expected_nb_parameters = scop->context->nb_parameters;
        else
            expected_nb_parameters = OSL_UNDEFINED;

        // TODO : check the number of parameter strings.

        if (!osl_statement_integrity_check(scop->statement,
                                           expected_nb_parameters))
            return 0;

        scop = scop->next;
    }

    return 1;
}


/**
 * osl_scop_check_compatible_scoplib function:
 * This function checks that a scop is "well formed". It returns 0 if the
 * check failed or 1 if no problem has been detected.
 * \param scop  The scop we want to check.
 * \return 0 if the integrity check fails, 1 otherwise.
 */
int osl_scop_check_compatible_scoplib(osl_scop_p scop) {

    if (!osl_scop_integrity_check(scop))
        return 0;
    if (scop->next != NULL)
        return 0;
    if (scop == NULL || scop->statement == NULL)
        return 1;

    osl_relation_p domain;
    osl_statement_p statement;
    osl_relation_p scattering;
    int precision = scop->statement->scattering->precision;
    int i, j;

    statement = scop->statement;
    while (statement != NULL) {
        scattering = statement->scattering;

        if (scattering->nb_local_dims != 0) {
            OSL_error("Local dims in scattering matrix");
            return 0;
        }

        domain = statement->domain;
        while (domain != NULL) {
            if (domain->nb_local_dims != 0) {
                OSL_error("Local dims in domain matrix");
                return 0;
            }
            domain = domain->next;
        }

        // Check if there is only the -Identity in the output_dims
        // and the lines MUST be in the right order
        for (i = 0 ; i < scattering->nb_rows ; i++) {
            for (j = 0 ; j < scattering->nb_output_dims ; j++) {
                if (i == j) { // -1
                    if (!osl_int_mone(precision, scattering->m[i][j+1])) {
                        OSL_error("Wrong -Identity");
                        return 0;
                    }
                } else { // 0
                    if (!osl_int_zero(precision, scattering->m[i][j+1])) {
                        OSL_error("Wrong -Identity");
                        return 0;
                    }
                }
            }
        }

        statement = statement->next;
    }

    return 1;
}


/**
 * osl_scop_get_nb_parameters function:
 * this function returns the number of global parameters of a given SCoP.
 * \param scop The scop we want to know the number of global parameters.
 * \return The number of global parameters in the scop.
 */
int osl_scop_get_nb_parameters(osl_scop_p scop) {

    if (scop->context == NULL) {
        OSL_debug("no context domain, assuming 0 parameters");
        return 0;
    } else {
        return scop->context->nb_parameters;
    }
}


/**
 * osl_scop_register_extension function:
 * this function registers a list of extension interfaces to a scop, i.e., it
 * adds them to the scop registry. In addition, it will extract extensions
 * corresponding to those interfaces from the textual form of the extensions
 * (if any) and add them to the scop extension list.
 * \param scop      The scop for which an extension has to be registered.
 * \param interface The extension interface to register within the scop.
 */
void osl_scop_register_extension(osl_scop_p scop, osl_interface_p interface) {
    osl_generic_p textual, new;
    char * extension_string;

    if ((interface != NULL) && (scop != NULL)) {
        osl_interface_add(&scop->registry, interface);

        textual = osl_generic_lookup(scop->extension, interface->URI);
        if (textual != NULL) {
            extension_string = ((osl_textual_p)textual->data)->textual;
            new = osl_generic_sread(&extension_string, interface);
            osl_generic_add(&scop->extension, new);
        }
    }
}


/**
 * osl_scop_get_attributes function:
 * this function returns, through its parameters, the maximum values of the
 * relation attributes (nb_iterators, nb_parameters etc) in the scop.
 * HOWEVER, it updates the parameter value iff the attribute is greater than
 * the input parameter value. Hence it may be used to get the attributes as
 * well as to find the maximum attributes for several scop lists. The array
 * identifier 0 is used when there is no array identifier (AND this is OK),
 * OSL_UNDEFINED is used to report it is impossible to provide the property
 * while it should. This function is not intended for checking, the input
 * scop should be correct.
 * \param[in]     scop          The scop to extract attributes values.
 * \param[in,out] nb_parameters Number of parameter attribute.
 * \param[in,out] nb_iterators  Number of iterators attribute.
 * \param[in,out] nb_scattdims  Number of scattering dimensions attribute.
 * \param[in,out] nb_localdims  Number of local dimensions attribute.
 * \param[in,out] array_id      Maximum array identifier attribute.
 */
void osl_scop_get_attributes(osl_scop_p scop,
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

    while (scop != NULL) {
        osl_relation_get_attributes(scop->context,
                                    &local_nb_parameters,
                                    &local_nb_iterators,
                                    &local_nb_scattdims,
                                    &local_nb_localdims,
                                    &local_array_id);

        osl_statement_get_attributes(scop->statement,
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
        scop = scop->next;
    }
}


/**
 * osl_scop_normalize_scattering function:
 * this function modifies a scop such that all scattering relation have
 * the same number of output dimensions (additional output dimensions are
 * set as being equal to zero).
 * \param[in,out] scop The scop to nomalize the scattering functions.
 */
void osl_scop_normalize_scattering(osl_scop_p scop) {
    int max_scattering_dims = 0;
    osl_statement_p statement;
    osl_relation_p extended;

    if ((scop != NULL) && (scop->statement != NULL)) {
        // Get the max number of scattering dimensions.
        statement = scop->statement;
        while (statement != NULL) {
            if (statement->scattering != NULL) {
                max_scattering_dims = OSL_max(max_scattering_dims,
                                              statement->scattering->nb_output_dims);
            }
            statement = statement->next;
        }

        // Normalize.
        statement = scop->statement;
        while (statement != NULL) {
            if (statement->scattering != NULL) {
                extended = osl_relation_extend_output(statement->scattering,
                                                      max_scattering_dims);
                osl_relation_free(statement->scattering);
                statement->scattering = extended;
            }
            statement = statement->next;
        }
    }
}

