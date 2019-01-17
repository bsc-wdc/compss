
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                        relation_list.c                          **
 **-----------------------------------------------------------------**
 **                   First version: 08/10/2010                     **
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
#include <osl/relation.h>
#include <osl/relation_list.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_relation_list_idump function:
 * Displays a osl_relation_list_t structure (a list of relations) into a
 * file (file, possibly stdout). See osl_relation_print_structure for
 * more details.
 * \param file   File where informations are printed.
 * \param l	 The list of relations whose information has to be printed.
 * \param level  Number of spaces before printing, for each line.
 */
void osl_relation_list_idump(FILE * file, osl_relation_list_p l, int level) {
    int j, first = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file,"|\t");

    if (l != NULL)
        fprintf(file, "+-- osl_relation_list_t\n");
    else
        fprintf(file, "+-- NULL relation list\n");

    while (l != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_relation_list_t\n");
        } else
            first = 0;

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print a relation.
        osl_relation_idump(file, l->elt, level+1);

        l = l->next;

        // Next line.
        if (l != NULL) {
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
 * This function prints the content of a osl_relation_list_t into
 * a file (file, possibly stdout).
 * \param file File where informations are printed.
 * \param list The relation whose information has to be printed.
 */
void osl_relation_list_dump(FILE * file, osl_relation_list_p list) {
    osl_relation_list_idump(file, list, 0);
}


/**
 * osl_relation_list_pprint_elts function:
 * This function pretty-prints the elements of a osl_relation_list_t structure
 * into a file (file, possibly stdout) in the OpenScop format. I.e., it prints
 * only the elements and not the number of elements. It prints an element of the
 * list only if it is not NULL.
 * \param file  File where informations are printed.
 * \param list  The relation list whose information has to be printed.
 * \param[in] names Array of constraint columns names.
 */
void osl_relation_list_pprint_elts(FILE * file, osl_relation_list_p list,
                                   osl_names_p names) {
    size_t i;
    osl_relation_list_p head = list;

    // Count the number of elements in the list with non-NULL content.
    i = osl_relation_list_count(list);

    // Print each element of the relation list.
    if (i > 0) {
        i = 0;
        while (head) {
            if (head->elt != NULL) {
                osl_relation_pprint(file, head->elt, names);
                if (head->next != NULL)
                    fprintf(file, "\n");
                i++;
            }
            head = head->next;
        }
    } else {
        fprintf(file, "# NULL relation list\n");
    }
}


/**
 * osl_relation_list_pprint_access_array_scoplib function:
 * This function pretty-prints the elements of a osl_relation_list_t structure
 * into a file (file, possibly stdout) in the SCoPLib format. I.e., it prints
 * only the elements and not the number of elements. It prints an element of the
 * list only if it is not NULL.
 * \param file  File where informations are printed.
 * \param list  The relation list whose information has to be printed.
 * \param[in] names Array of constraint columns names.
 * \param[in] add_fakeiter True of False
 */
void osl_relation_list_pprint_access_array_scoplib(FILE * file,
        osl_relation_list_p list, osl_names_p names, int add_fakeiter) {
    size_t i;
    int nb_rows_read = 0, nb_columns_read = 0;
    int nb_rows_write = 0, nb_columns_write = 0;
    int nb_rows_may_write = 0, nb_columns_may_write = 0;
    osl_relation_list_p head ;

    // Count the number of elements in the list with non-NULL content.
    i = osl_relation_list_count(list);

    // Print each element of the relation list.
    if (i > 0) {

        // Read/Write arrays size
        head = list;
        while (head) {
            if (head->elt != NULL) {
                if (head->elt->type == OSL_TYPE_READ) {
                    if (head->elt->nb_rows == 1)
                        nb_rows_read++;
                    else
                        nb_rows_read += head->elt->nb_rows - 1; // remove the 'Arr'

                    nb_columns_read = head->elt->nb_columns - head->elt->nb_output_dims;

                } else if (head->elt->type == OSL_TYPE_WRITE) {
                    if (head->elt->nb_rows == 1)
                        nb_rows_write++;
                    else
                        nb_rows_write += head->elt->nb_rows - 1; // remove the 'Arr'

                    nb_columns_write = head->elt->nb_columns - head->elt->nb_output_dims;

                } else if (head->elt->type == OSL_TYPE_MAY_WRITE) {
                    if (head->elt->nb_rows == 1)
                        nb_rows_may_write++;
                    else
                        nb_rows_may_write += head->elt->nb_rows - 1; // remove the 'Arr'

                    nb_columns_may_write = head->elt->nb_columns -
                                           head->elt->nb_output_dims;
                }
            }
            head = head->next;
        }

        if (add_fakeiter) {
            nb_columns_read++;
            nb_columns_write++;
            nb_columns_may_write++;
        }

        fprintf(file, "# Read access informations\n%d %d\n",
                nb_rows_read, nb_columns_read);
        head = list;
        while (head) {
            if (head->elt != NULL && head->elt->type == OSL_TYPE_READ) {
                osl_relation_pprint_scoplib(file, head->elt, names, 0, add_fakeiter);
            }
            head = head->next;
        }

        fprintf(file, "# Write access informations\n%d %d\n",
                nb_rows_write, nb_columns_write);
        head = list;
        while (head) {
            if (head->elt != NULL && head->elt->type == OSL_TYPE_WRITE) {
                osl_relation_pprint_scoplib(file, head->elt, names, 0, add_fakeiter);
            }
            head = head->next;
        }

        if (nb_rows_may_write > 0) {
            fprintf(file, "# May Write access informations\n%d %d\n",
                    nb_rows_may_write, nb_columns_may_write);
            head = list;
            while (head) {
                if (head->elt != NULL && head->elt->type == OSL_TYPE_MAY_WRITE) {
                    osl_relation_pprint_scoplib(file, head->elt, names, 0, add_fakeiter);
                }
                head = head->next;
            }
        }
    } else {
        fprintf(file, "# NULL relation list\n");
    }
}


/**
 * osl_relation_list_pprint function:
 * This function pretty-prints the content of a osl_relation_list_t structure
 * into a file (file, possibly stdout) in the OpenScop format. It prints
 * an element of the list only if it is not NULL.
 * \param[in] file  File where informations are printed.
 * \param[in] list  The relation list whose information has to be printed.
 * \param[in] names Array of constraint columns names.
 */
void osl_relation_list_pprint(FILE * file, osl_relation_list_p list,
                              osl_names_p names) {
    size_t i;

    // Count the number of elements in the list with non-NULL content.
    i = osl_relation_list_count(list);

    // Print it.
    if (i > 1)
        fprintf(file,"# List of %lu elements\n%lu\n", i, i);
    else
        fprintf(file,"# List of %lu element \n%lu\n", i, i);

    // Print each element of the relation list.
    osl_relation_list_pprint_elts(file, list, names);
}


/**
 * osl_relation_list_print function:
 * This function prints the content of a osl_relation_list_t structure
 * into a file (file, possibly stdout) in the OpenScop format. It prints
 * an element of the list only if it is not NULL.
 * \param file  File where informations are printed.
 * \param list  The relation list whose information has to be printed.
 */
void osl_relation_list_print(FILE * file, osl_relation_list_p list) {

    osl_relation_list_pprint(file, list, NULL);
}

/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_relation_list_pread function ("precision read"):
 * this function reads a list of relations into a file (foo,
 * posibly stdin) and returns a pointer this relation list.
 * \param[in] file      The input stream.
 * \param[in] precision The precision of the relation elements.
 * \return A pointer to the relation list structure that has been read.
 */
osl_relation_list_p osl_relation_list_pread(FILE * file, int precision) {
    int i;
    osl_relation_list_p list;
    osl_relation_list_p res;
    int nb_mat;

    // Read the number of relations to read.
    nb_mat = osl_util_read_int(file, NULL);

    if (nb_mat < 0)
        OSL_error("negative number of relations");

    // Allocate the header of the list and start reading each element.
    res = list = osl_relation_list_malloc();
    for (i = 0; i < nb_mat; ++i) {
        list->elt = osl_relation_pread(file, precision);
        if (i < nb_mat - 1)
            list->next = osl_relation_list_malloc();
        list = list->next;
    }

    return res;
}


/**
 * osl_relation_list_read function:
 * this function is equivalent to osl_relation_list_pread() except that
 * the precision corresponds to the precision environment variable or
 * to the highest available precision if it is not defined.
 * \see{osl_relation_list_pread}
 */
osl_relation_list_p osl_relation_list_read(FILE * foo) {
    int precision = osl_util_get_precision();
    return osl_relation_list_pread(foo, precision);
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_relation_list_malloc function:
 * This function allocates the memory space for a osl_relation_list_t
 * structure and sets its fields with default values. Then it returns
 * a pointer to the allocated space.
 * \return A pointer to an empty relation list with fields set to default
 *         values.
 */
osl_relation_list_p osl_relation_list_malloc(void) {
    osl_relation_list_p res;

    OSL_malloc(res, osl_relation_list_p, sizeof(osl_relation_list_t));
    res->elt  = NULL;
    res->next = NULL;

    return res;
}



/**
 * osl_relation_list_free function:
 * This function frees the allocated memory for a osl_relation_list_t
 * structure, and all the relations stored in the list.
 * \param list The pointer to the relation list we want to free.
 */
void osl_relation_list_free(osl_relation_list_p list) {
    osl_relation_list_p tmp;

    if (list == NULL)
        return;

    while (list != NULL) {
        if (list->elt != NULL)
            osl_relation_free(list->elt);
        tmp = list->next;
        free(list);
        list = tmp;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_relation_list_node function:
 * This function builds an osl_relation_list_t node and sets its
 * relation element as a copy of the one provided as parameter.
 * If the relation provided as an argument is NULL, NULL is returned.
 * \param r The pointer to the relation to copy/paste in a list node.
 * \return A pointer to a relation list node containing a copy of "relation".
 */
osl_relation_list_p osl_relation_list_node(osl_relation_p r) {
    osl_relation_list_p new = NULL;

    if (r != NULL) {
        new = osl_relation_list_malloc();
        new->elt = osl_relation_clone(r);
    }
    return new;
}


/**
 * osl_relation_list_clone function:
 * This functions builds and returns a quasi-"hard copy" (not a pointer copy)
 * of a osl_relation_list_t data structure provided as parameter.
 * \param list  The pointer to the relation list we want to copy.
 * \return A pointer to the full copy of the relation list in parameter.
 */
osl_relation_list_p osl_relation_list_clone(osl_relation_list_p list) {

    osl_relation_list_p clone = NULL, node, previous = NULL;
    int first = 1;

    while (list != NULL) {
        node      = osl_relation_list_malloc();
        node->elt = osl_relation_clone(list->elt);

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        list = list->next;
    }

    return clone;
}


/**
 * osl_relation_list_concat function:
 * this function builds a new relation list as the concatenation of the
 * two lists sent as parameters.
 * \param l1  The first relation list.
 * \param l2  The second relation list.
 * \return A pointer to the relation list resulting from the concatenation of
 *         l1 and l2.
 */
osl_relation_list_p osl_relation_list_concat(osl_relation_list_p l1,
        osl_relation_list_p l2) {
    osl_relation_list_p new, end;

    if (l1 == NULL)
        return osl_relation_list_clone(l2);

    if (l2 == NULL)
        return osl_relation_list_clone(l1);

    new = osl_relation_list_clone(l1);
    end = new;
    while (end->next != NULL)
        end = end->next;
    end->next = osl_relation_list_clone(l2);

    return new;
}


/**
 * osl_relation_list_add function:
 * this function adds a relation list at the end of the relation list
 * pointed by l1. No new list is created: this functions links the two
 * input lists. If the first relation list is NULL, it is set to the
 * second relation list.
 * \param[in,out] l1  Pointer to the first relation list.
 * \param[in]     l2  The second relation list.
 */
void osl_relation_list_add(osl_relation_list_p *l1, osl_relation_list_p l2) {
    while (*l1 != NULL)
        l1 = &((*l1)->next);

    *l1 = l2;
}


/**
 * osl_relation_list_push function:
 * this function sees a list of relations as a stack of relations and
 * performs the push operation onto this stack.
 * \param[in,out] head Pointer to the head of the relation stack.
 * \param[in,out] node Relation node to add to the stack. Its next field is
 *                     updated to the previous head of the stack.
 */
void osl_relation_list_push(osl_relation_list_p *head,
                            osl_relation_list_p node) {
    if (node != NULL) {
        node->next = *head;
        *head = node;
    }
}


/**
 * osl_relation_list_pop function:
 * this function sees a list of relations as a stack of relations and
 * performs the pop operation onto this stack.
 * \param[in,out] head Pointer to the head of the relation stack. It is
 *                     updated to the previous element in the stack (NULL
 *                     if there is none).
 * \return The top element of the stack (detached from the list).
 */
osl_relation_list_p osl_relation_list_pop(osl_relation_list_p *head) {
    osl_relation_list_p top = NULL;

    if (*head != NULL) {
        top = *head;
        *head = (*head)->next;
        top->next = NULL;
    }

    return top;
}


/**
 * osl_relation_list_dup function:
 * this function sees a list of relations as a stack of relations and
 * performs the dup operation (duplicate the top element) onto
 * this stack.
 * \param[in,out] head Pointer to the head of the relation stack. It is
 *                     updated to the new element after duplication.
 */
void osl_relation_list_dup(osl_relation_list_p *head) {
    osl_relation_list_p top = osl_relation_list_pop(head);
    osl_relation_list_push(head, osl_relation_list_clone(top));
    osl_relation_list_push(head, top);
}


/**
 * osl_relation_list_drop function:
 * this function sees a list of relations as a stack of relations and
 * performs the drop operation (pop and destroy popped element) onto
 * this stack.
 * \param[in,out] head Pointer to the head of the relation stack. It is
 *                     updated to the previous element in the stack (NULL
 *                     if there is none).
 */
void osl_relation_list_drop(osl_relation_list_p *head) {
    osl_relation_list_p top = osl_relation_list_pop(head);
    osl_relation_list_free(top);
}


/**
 * osl_relation_list_destroy function:
 * this function sees a list of relations as a stack of relations and
 * performs the destroy operation onto this stack, i.e., it completely
 * free it.
 * \param[in,out] head Pointer to the head of the relation stack.
 *                     Updated to NULL.
 */
void osl_relation_list_destroy(osl_relation_list_p *head) {

    while (*head != NULL)
        osl_relation_list_drop(head);
}


/**
 * osl_relation_list_equal function:
 * This function returns true if the two relation lists are the same, false
 * otherwise..
 * \param l1 The first relation list.
 * \param l2 The second relation list.
 * \return 1 if l1 and l2 are the same (content-wise), 0 otherwise.
 */
int osl_relation_list_equal(osl_relation_list_p l1, osl_relation_list_p l2) {
    while ((l1 != NULL) && (l2 != NULL)) {
        if (l1 == l2)
            return 1;

        if (!osl_relation_equal(l1->elt, l2->elt))
            return 0;

        l1 = l1->next;
        l2 = l2->next;
    }

    if (((l1 == NULL) && (l2 != NULL)) || ((l1 != NULL) && (l2 == NULL)))
        return 0;

    return 1;
}


/**
 * osl_relation_integrity_check function:
 * This function checks that a list of relation is "well formed" according to
 * some expected properties (setting an expected value to OSL_UNDEFINED
 * means that we do not expect a specific value) and what the relations are
 * supposed to represent (all relations of a list are supposed to have the
 * same semantics). It returns 0 if the check failed or 1 if no problem has
 * been detected.
 * \param list      The relation list we want to check.
 * \param type      Semantics about this relation (domain, access...).
 * \param expected_nb_output_dims Expected number of output dimensions.
 * \param expected_nb_input_dims  Expected number of input dimensions.
 * \param expected_nb_parameters  Expected number of parameters.
 * \return 0 if the integrity check fails, 1 otherwise.
 */
int osl_relation_list_integrity_check(osl_relation_list_p list,
                                      int type,
                                      int expected_nb_output_dims,
                                      int expected_nb_input_dims,
                                      int expected_nb_parameters) {
    while (list != NULL) {
        // Check the access function.
        if (!osl_relation_integrity_check(list->elt,
                                          type,
                                          expected_nb_output_dims,
                                          expected_nb_input_dims,
                                          expected_nb_parameters)) {
            return 0;
        }

        list = list->next;
    }

    return 1;
}


/**
 * osl_relation_list_set_type function:
 * this function sets the type of each relation in the relation list to the
 * one provided as parameter.
 * \param list The list of relations to set the type.
 * \param type The type.
 */
void osl_relation_list_set_type(osl_relation_list_p list, int type) {

    while (list != NULL) {
        if (list->elt != NULL) {
            list->elt->type = type;
        }
        list = list->next;
    }
}


/**
 * osl_relation_list_filter function:
 * this function returns a copy of the input relation list, restricted to
 * the relations of a given type. The special type OSL_TYPE_ACCESS
 * filters any kind of access (read, write, rdwr etc.).
 * \param list The relation list to copy/filter.
 * \param type The filtering type.
 * \return A copy of the input list with only relation of the given type.
 */
osl_relation_list_p osl_relation_list_filter(osl_relation_list_p list,
        int type) {

    osl_relation_list_p copy = osl_relation_list_clone(list);
    osl_relation_list_p filtered = NULL;
    osl_relation_list_p previous = NULL;
    osl_relation_list_p trash;
    int first = 1;

    while (copy != NULL) {
        if ((copy->elt != NULL) &&
                (((type == OSL_TYPE_ACCESS) &&
                  (osl_relation_is_access(copy->elt))) ||
                 ((type != OSL_TYPE_ACCESS) &&
                  (type == copy->elt->type)))) {
            if (first) {
                filtered = copy;
                first = 0;
            }

            previous = copy;
            copy = copy->next;
        } else {
            trash = copy;
            if (!first)
                previous->next = copy->next;
            copy = copy->next;
            trash->next = NULL;
            osl_relation_list_free(trash);
        }
    }

    return filtered;
}


/**
 * osl_relation_list_count function:
 * this function returns the number of elements with non-NULL content
 * in a relation list.
 * \param list The relation list to count the number of elements.
 * \return The number of nodes with non-NULL content in the relation list.
 */
size_t osl_relation_list_count(osl_relation_list_p list) {
    size_t i = 0;

    while (list != NULL) {
        if (list->elt != NULL)
            i++;
        list = list->next;
    }

    return i;
}


/**
 * osl_relation_list_get_attributes function:
 * this function returns, through its parameters, the maximum values of the
 * relation attributes (nb_iterators, nb_parameters etc) in the relation list,
 * depending on its type. HOWEVER, it updates the parameter value iff the
 * attribute is greater than the input parameter value. Hence it may be used
 * to get the attributes as well as to find the maximum attributes for several
 * relation lists. The array identifier 0 is used when there is no array
 * identifier (AND this is OK), OSL_UNDEFINED is used to report it is
 * impossible to provide the property while it should. This function is not
 * intended for checking, the input relation list should be correct.
 * \param[in]     list          The relation list to extract attribute values.
 * \param[in,out] nb_parameters Number of parameter attribute.
 * \param[in,out] nb_iterators  Number of iterators attribute.
 * \param[in,out] nb_scattdims  Number of scattering dimensions attribute.
 * \param[in,out] nb_localdims  Number of local dimensions attribute.
 * \param[in,out] array_id      Maximum array identifier attribute.
 */
void osl_relation_list_get_attributes(osl_relation_list_p list,
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

    while (list != NULL) {
        osl_relation_get_attributes(list->elt,
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
        list = list->next;
    }
}

