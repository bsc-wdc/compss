

/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                   scop.c                                **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: june 7th 2012                    **
 **--- |"-.-"| -------------------------------------------------------**
       |     |
       |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2003-2008 Cedric Bastoul                                     *
*                                                                            *
* This is free software; you can redistribute it and/or modify it under the  *
* terms of the GNU General Public License as published by the Free Software  *
* Foundation; either version 2 of the License, or (at your option) any later *
* version.                                                                   *
*                                                                            *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.                                                          *
*                                                                            *
* You should have received a copy of the GNU General Public License along    *
* with software; if not, write to the Free Software Foundation, Inc.,        *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* CAnDL, the Chunky Dependence Analyzer                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/

/*
 * author Joel Poudroux
 */

#include <stdlib.h>
#include <string.h>
#include <osl/scop.h>
#include <osl/statement.h>
#include <osl/extensions/dependence.h>
#include <osl/extensions/arrays.h>
#include <candl/scop.h>
#include <candl/statement.h>
#include <candl/label_mapping.h>
#include <candl/macros.h>

/**
 * candl_scop_usr_init function:
 * initialize a candl_scop_usr structure
 *
 * May, 2013 extended to each scop in the list,
 */
void candl_scop_usr_init(osl_scop_p scop) {

    while (scop) {
        candl_scop_usr_p scop_usr;

        /* Init the scop_usr structure */
        scop_usr = (candl_scop_usr_p) malloc(sizeof(candl_scop_usr_t));
        scop_usr->size = 0;
        scop_usr->scalars_privatizable = NULL;
        scop_usr->usr_backup           = scop->usr;
        scop->usr = scop_usr;

        candl_statement_usr_init_all(scop);

        scop = scop->next;
    }
}


/**
 * candl_scop_usr_cleanup function:
 */
void candl_scop_usr_cleanup(osl_scop_p scop) {

    while (scop) {
        osl_statement_p statement = scop->statement;
        candl_scop_usr_p scop_usr;
        while (statement != NULL) {
            candl_statement_usr_cleanup(statement);
            statement = statement->next;
        }
        scop_usr = scop->usr;
        if (scop_usr) {
            if (scop_usr->scalars_privatizable)
                free(scop_usr->scalars_privatizable);
            scop->usr = scop_usr->usr_backup;
            free(scop_usr);
        }

        scop = scop->next;
    }
}

/**
 * \brief Create a new scop that with exactly the same relations as the given
 * scop, but with every part of the relation union replaced by a separate
 * statement.  Expects the input scop to have usr fields initalized with
 * candl structures.  Initializes the usr field of each new statement with
 * candl_label_mapping instance having original label set up to the label of
 * the original statement with unions.
 * \param [in] scop  The original scop with relation unions preinitalized for
 * use inside Candl.
 * \return   A new scop with relation unions converted to statements.
 */
osl_scop_p candl_scop_remove_unions(osl_scop_p scop) {
    osl_statement_p statement, new_statement, scop_statement_ptr;
    osl_statement_p stmt_iter;
    osl_scop_p new_scop, scop_ptr = NULL, result = NULL;
    candl_label_mapping_p label_mapping;
    candl_statement_usr_p stmt_usr;
    int counter = 0;

    statement = scop->statement;
    scop_statement_ptr = NULL;
    new_scop = osl_scop_malloc();

    for ( ; statement != NULL; statement = statement->next) {
        new_statement = osl_statement_remove_unions(statement);
        for (stmt_iter = new_statement; stmt_iter != NULL;
                stmt_iter = stmt_iter->next, counter++) {
            stmt_usr = (candl_statement_usr_p) statement->usr;
            label_mapping = candl_label_mapping_malloc();
            label_mapping->original = stmt_usr->label;
            label_mapping->mapped = counter;
            stmt_iter->usr = label_mapping;
        }
        if (!scop_statement_ptr) {
            scop_statement_ptr = new_statement;
            new_scop->statement = scop_statement_ptr;
        } else {
            scop_statement_ptr->next = new_statement;
            scop_statement_ptr = scop_statement_ptr->next;
        }
        while (scop_statement_ptr && scop_statement_ptr->next != NULL)
            scop_statement_ptr = scop_statement_ptr->next;
    }

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

    return result;
}

/**
 * \brief Replace access relations and access-related extensions in the
 * scop with relation unions from those from the scop without relations unions
 * with respect to one-to-may mapping of statements in the scop.
 * \param [in,out] scop     Scop to replace access relation in.
 * \param [in] nounion_scop Scop - source of the acess relations.
 * \param mapping           One-to-many mapping between statement labels in scop
 * and nounion_scop.
 */
void candl_scop_copy_access(osl_scop_p scop,
                            osl_scop_p nounion_scop,
                            candl_label_mapping_p mapping) {
    candl_label_mapping_p m;
    osl_statement_p statement, statement_nounion;
    osl_arrays_p arrays;
    osl_generic_p generic;
    osl_generic_p generic_arrays;

    for (statement = scop->statement; statement != NULL;
            statement = statement->next) {
        if (statement->access)
            osl_relation_list_free(statement->access);
        statement->access = NULL;
    }
    for (m = mapping; m != NULL; m = m->next) {
        statement = candl_statement_find_label(scop->statement, m->original);
        statement_nounion = candl_statement_find_label(nounion_scop->statement,
                            m->mapped);
        if (!statement->access) {
            statement->access = osl_relation_list_clone(statement_nounion->access);
        } else {
#ifndef NDEBUG
            if (!osl_relation_list_equal(statement->access,
                                         statement_nounion->access)) {
                CANDL_error("Could not merge deunified statements back after "
                            "scalar operation.  Not all parts of the statement "
                            "got identical acess relations.");
            }
#endif
        }
    }

    arrays = (osl_arrays_p) osl_generic_lookup(nounion_scop->extension,
             OSL_URI_ARRAYS);
    if (!arrays)
        return;
    arrays = osl_arrays_clone(arrays);
    generic_arrays = osl_generic_shell(arrays, osl_arrays_interface());
    generic_arrays->next = NULL;

    if (!scop->extension) {
        osl_generic_add(&scop->extension, generic_arrays);
    } else if (scop->extension->next == NULL) {
        if (osl_generic_has_URI(scop->extension, OSL_URI_ARRAYS)) {
            scop->extension->next = NULL;
            osl_generic_free(scop->extension);
            scop->extension = generic_arrays;
        } else {
            osl_generic_add(&scop->extension, generic_arrays);
        }
    } else {
        for (generic = scop->extension; generic->next != NULL;
                generic = generic->next) {
            if (osl_generic_has_URI(generic->next, OSL_URI_ARRAYS)) {
                break;
            }
        }

        if (generic->next != NULL) {
            generic_arrays->next = generic->next->next;
            generic->next->next = NULL;
            osl_generic_free(generic->next);
            generic->next = generic_arrays;
        } else {
            osl_generic_add(&scop->extension, generic_arrays);
        }
    }
}

/**
 * \brief Add the given dependences as an osl extension to the scop.
 * \param [in,out] scop    Scop to extend.
 * \param [in] dependence  Precomputed dependence list.
 */
void candl_scop_add_dependence_extension(osl_scop_p scop,
        osl_dependence_p dependence) {
    if (!dependence)
        return;

    osl_dependence_p dep = osl_generic_lookup(scop->extension,
                           OSL_URI_DEPENDENCE);
    if (dep != NULL) {
        osl_generic_remove(&scop->extension, OSL_URI_DEPENDENCE);
        CANDL_info("Deleting old dependences found in the dependence extension.");
    }
    // The commented code is a proper version of addition below it, but the old
    // version is kept for consistency with test bench, sensitive to reordering
    // of extensions.
#if 0
    osl_generic_add(&scop->extension,
                    osl_generic_shell(dependence, osl_dependence_interface()));
#endif
    if (scop->extension == NULL) {
        scop->extension = osl_generic_shell(dependence, osl_dependence_interface());
    } else {
        osl_generic_p generic =
            osl_generic_shell(dependence, osl_dependence_interface());
        generic->next = scop->extension;
        scop->extension = generic;
    }
}

/**
 * \brief Construct label mapping for the statement of the given scop, assuming
 * they are properly intialized with the candl_statement_usr, the usr_backup
 * field of which is pointing to candl_label_mapping with original label.
 * Behavior undefied if these conditions do not hold.
 * \param scop  A scop with preinialized usr field.
 * \return A label mapping list extracted from the scop's usr field.
 */
candl_label_mapping_p candl_scop_label_mapping(osl_scop_p scop) {
    candl_label_mapping_p mapping = NULL;
    osl_statement_p statement = scop->statement;

    //candl_statement_usr_init_all(scop);
    for ( ; statement != NULL; statement = statement->next) {
        candl_statement_usr_p stmt_usr = (candl_statement_usr_p) statement->usr;
        candl_label_mapping_p label_mapping =
            (candl_label_mapping_p) stmt_usr->usr_backup;
        stmt_usr->usr_backup = NULL;
        label_mapping->mapped = stmt_usr->label;
        label_mapping->next = NULL;
        candl_label_mapping_add(&mapping, label_mapping);
    }

    return mapping;
}


