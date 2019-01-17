
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                label_mapping.c                          **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: may 1st 2015                     **
 **--- |"-.-"| -------------------------------------------------------**
 |     |
 |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2003-2015 Cedric Bastoul                                     *
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
* CAnDL, the Chunky Dependence Analyser                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
* File written by Oleksandr Zinenko, Oleksandr.Zinenko@inria.fr              *
*                                                                            *
******************************************************************************/

#include <stdlib.h>
#include <stdio.h>

#include <candl/label_mapping.h>
#include <candl/macros.h>
#include <candl/scop.h>
#include <candl/statement.h>

/**
 * \brief Create a new element of the label mapping between
 * CANDL_LABEL_UNDEFINED elements.
 * \return The newly created mapping.
 */
candl_label_mapping_p candl_label_mapping_malloc() {
    candl_label_mapping_p mapping;
    CANDL_malloc(mapping, candl_label_mapping_p, sizeof(candl_label_mapping_t));
    mapping->original = CANDL_LABEL_UNDEFINED;
    mapping->mapped = CANDL_LABEL_UNDEFINED;
    mapping->next = NULL;
    return mapping;
}

/**
 * \brief Free this element of the mapping and all the following list elements
 * if any.
 * \param Pointer to the list element to free.
 */
void candl_label_mapping_free(candl_label_mapping_p mapping) {
    candl_label_mapping_p ptr;
    while (mapping != NULL) {
        ptr = mapping->next;
        free(mapping);
        mapping = ptr;
    }
}

/**
 * \brief Add a mapping element to the end of the list.  If the head of the list
 * is null (meaning empty list) it will be rewritten with the element.  All
 * sucessors of the element being inserted are maintained in the list.
 * \param [in,out] mapping  A pointer, possibly null, to the head of the list.
 * \param [in]     instance An element to append to the list.
 */
void candl_label_mapping_add(candl_label_mapping_p * mapping,
                             candl_label_mapping_p instance) {
    candl_label_mapping_p ptr;
    if (*mapping == NULL) {
        *mapping = instance;
    } else {
        for (ptr = *mapping; ptr->next != NULL; ptr = ptr->next)
            ;
        ptr->next = instance;
    }
}

/**
 * \brief Add a new mapping between two provided values to the end of the list.
 * If the head of the list is null (meaning empty list) it will be rewritten
 * with the element.
 * \param [in,out] mapping  A pointer, possibly null, to the head of the list.
 * \param [in]     original Source in the mapping element.
 * \param [in]     mapped   Target in the mapping element.
 */
void candl_label_mapping_add_map(candl_label_mapping_p * mapping,
                                 int original, int mapped) {
    candl_label_mapping_p instance;

    instance = candl_label_mapping_malloc();
    instance->original = original;
    instance->mapped = mapped;

    candl_label_mapping_add(mapping, instance);
}

/**
 * \brief Finds the first original value in the mapping that corresponds to the
 * provided mapped value.
 * \param [in] mapping  Head of the mapping list.
 * \param [in] mapped   Value to find correspondance for.
 * \return              The first values in the list that is mapped to the
 * provided value, CANDL_LABEL_UNDEFINED if there is no such value.
 */
int candl_label_mapping_find_original(candl_label_mapping_p mapping,
                                      int mapped) {
    for ( ; mapping != NULL; mapping = mapping->next) {
        if (mapping->mapped == mapped)
            return mapping->original;
    }
    return CANDL_LABEL_UNDEFINED;
}

/**
 * \brief Print the mapping list to the given file.
 * \param [in] f  Destination of the output.
 * \param mapping Head of the mapping list.
 */
void candl_label_mapping_print(FILE *f, candl_label_mapping_p mapping) {
    for ( ; mapping != NULL; mapping = mapping->next) {
        fprintf(f, "%d -> %d\n", mapping->original, mapping->mapped);
    }
}

