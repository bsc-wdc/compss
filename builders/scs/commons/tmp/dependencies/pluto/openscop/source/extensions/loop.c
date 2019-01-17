
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                      extensions/loop.c                          **
 **-----------------------------------------------------------------**
 **                   First version: 03/06/2013                     **
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
#include <osl/interface.h>
#include <osl/extensions/loop.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_loop_idump function:
 * this function displays an osl_loop_t structure (loop) into a file
 * (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 *
 * \param[in] file   The file where the information has to be printed.
 * \param[in] loop   The loop structure to print.
 * \param[in] level  Number of spaces before printing, for each line.
 */
void osl_loop_idump(FILE * file, osl_loop_p loop, int level) {
    int j, first = 1, number=1;
    size_t i;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (loop != NULL)
        fprintf(file, "+-- osl_loop_t\n");
    else
        fprintf(file, "+-- NULL loop\n");

    while (loop != NULL) {
        // Go to the right level.
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");

            fprintf(file, "|   osl_loop_t (node %d)\n", number);
        } else {
            first = 0;
        }

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Display the number of names.
        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--iterator: %s\n", loop->iter);

        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--nb_stmts: %zu\n", loop->nb_stmts);

        // Display the id/name.
        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--stmt_ids:");
        for(i = 0; i < loop->nb_stmts; i++) {
            // Go to the right level.
            fprintf(file, "%2d, ", loop->stmt_ids[i]);
        }
        fprintf(file, "\n");


        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--private_vars: %s\n", loop->private_vars);

        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--directive: %d\n", loop->directive);

        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+--user: %s\n", loop->user);

        loop = loop->next;
        number++;

        // Next line.
        if (loop != NULL) {
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
 * osl_loop_dump function:
 * this function prints the content of an osl_loop_t structure
 * (*loop) into a file (file, possibly stdout).
 *
 * \param[in] file   The file where the information has to be printed.
 * \param[in] loop The loop structure to print.
 */
void osl_loop_dump(FILE * file, osl_loop_p loop) {
    osl_loop_idump(file, loop, 0);
}


/**
 * osl_loop_sprint function:
 * this function prints the content of an osl_loop_t structure
 * (*loop) into a string (returned) in the OpenScop textual format.
 * \param[in] loop The loop structure to print.
 * \return         A string containing the OpenScop dump of the loop structure.
 */
char * osl_loop_sprint(osl_loop_p loop) {
    size_t i;
    int nloop = 0;
    size_t high_water_mark = OSL_MAX_STRING;
    char *string = NULL;
    char buffer[OSL_MAX_STRING];

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    sprintf(buffer, "# Number of loops\n%d\n",osl_loop_count(loop));
    osl_util_safe_strcat(&string, buffer, &high_water_mark);

    while (loop != NULL) {
        sprintf(buffer, "# ===========================================\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Loop number %d \n", ++nloop);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Iterator name\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        sprintf(buffer, "%s\n", loop->iter);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Number of stmts\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        sprintf(buffer, "%zu\n", loop->nb_stmts);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        if (loop->nb_stmts) {
            sprintf(buffer, "# Statement identifiers\n");
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }
        for (i = 0; i < loop->nb_stmts; i++) {
            sprintf(buffer, "%d\n", loop->stmt_ids[i]);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        sprintf(buffer, "# Private variables\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        sprintf(buffer, "%s\n", loop->private_vars);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Directive\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        sprintf(buffer, "%d", loop->directive);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // special case for OSL_LOOP_DIRECTIVE_USER
        if (loop->directive & OSL_LOOP_DIRECTIVE_USER) {
            sprintf(buffer, " %s", loop->user);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }
        sprintf(buffer, "\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        loop = loop->next;
    }

    OSL_realloc(string, char *, (strlen(string) + 1) * sizeof(char));
    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_loop_sread function:
 * this function reads a loop structure from a string complying to the
 * OpenScop textual format and returns a pointer to this loop structure.
 * The input parameter is updated to the position in the input string this
 * function reaches right after reading the comment structure.
 *
 * \param[in,out] input   The input string where to find an loop structure.
 *                        Updated to the position after what has been read.
 * \return                A pointer to the loop structure that has been read.
 */
osl_loop_p osl_loop_sread(char **input) {
    size_t i;
    int nb_loops;
    osl_loop_p head;
    osl_loop_p loop;

    if (input == NULL) {
        OSL_debug("no loop optional tag");
        return NULL;
    }

    // Find the number of names provided.
    nb_loops = osl_util_read_int(NULL, input);
    if(nb_loops == 0)
        return NULL;

    // Allocate the array of id and names.
    head = loop = osl_loop_malloc();

    while (nb_loops != 0) {

        loop->iter = osl_util_read_string(NULL, input);
        loop->nb_stmts = (size_t)osl_util_read_int(NULL, input);

        OSL_malloc(loop->stmt_ids, int *, loop->nb_stmts * sizeof(int));
        for (i = 0; i < loop->nb_stmts; i++)
            loop->stmt_ids[i] = osl_util_read_int(NULL, input);

        loop->private_vars = osl_util_read_line(NULL, input);
        if (!strcmp(loop->private_vars, "(null)")) {
            free(loop->private_vars);
            loop->private_vars=NULL;
        }

        loop->directive = osl_util_read_int(NULL, input);

        // special case for OSL_LOOP_DIRECTIVE_USER
        if (loop->directive & OSL_LOOP_DIRECTIVE_USER) {
            loop->user = osl_util_read_line(NULL, input);
            if (!strcmp(loop->user, "(null)")) {
                free(loop->user);
                loop->user=NULL;
            }
        }

        nb_loops--;
        if (nb_loops != 0) {
            loop->next = osl_loop_malloc ();
            loop = loop->next;
        }
    }

    return head;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_loop_malloc function:
 * this function allocates the memory space for an osl_loop_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 *
 * \return  A pointer to an empty loop structure with fields set to
 *          default values.
 */
osl_loop_p osl_loop_malloc(void) {
    osl_loop_p loop;

    OSL_malloc(loop, osl_loop_p, sizeof(osl_loop_t));
    loop->iter          = NULL;
    loop->nb_stmts      = 0;
    loop->stmt_ids      = NULL;
    loop->private_vars  = NULL;
    loop->directive     = 0;
    loop->user          = NULL;
    loop->next          = NULL;

    return loop;
}


/**
 * osl_loop_free function:
 * this function frees the allocated memory for an loop structure.
 *
 * \param[in,out] loop The pointer to the loop structure we want to free.
 */
void osl_loop_free(osl_loop_p loop) {

    while (loop != NULL) {
        osl_loop_p tmp = loop;

        if (loop->iter) free(loop->iter);
        if (loop->stmt_ids) free(loop->stmt_ids);
        if (loop->private_vars) free(loop->private_vars);
        if (loop->user) free(loop->user);

        loop = loop->next;

        free(tmp);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_loop_clone_one function:
 * this function builds and returns a "hard copy" (not a pointer copy) of
 * "one" (and not the whole list) osl_loop_t data structure.
 *
 * \param[in] loop  The pointer to the loop structure to clone.
 * \return          A pointer to the clone of the loop structure.
 */
osl_loop_p osl_loop_clone_one(osl_loop_p loop) {
    size_t i;
    osl_loop_p clone;

    if (loop == NULL)
        return NULL;

    clone = osl_loop_malloc();
    OSL_strdup(clone->iter, loop->iter);
    clone->nb_stmts = loop->nb_stmts;
    OSL_malloc(clone->stmt_ids, int *, loop->nb_stmts * sizeof(int));

    for (i = 0; i < loop->nb_stmts; i++) {
        clone->stmt_ids[i] = loop->stmt_ids[i];
    }

    clone->directive = loop->directive;

    if(loop->private_vars != NULL)
        OSL_strdup(clone->private_vars, loop->private_vars);

    if(loop->user != NULL)
        OSL_strdup(clone->user, loop->user);

    return clone;
}

/**
 * osl_loop_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of a
 * list of osl_loop_t data structures.
 *
 * \param[in] loop  The pointer to the list of loop structure to clone.
 * \return          A pointer to the clone of list of the loop structure.
 */
osl_loop_p osl_loop_clone(osl_loop_p loop) {
    osl_loop_p clone = NULL;
    osl_loop_p head  = NULL;

    if (loop == NULL)
        return NULL;

    while (loop) {

        if (clone==NULL) {
            head = clone = osl_loop_clone_one(loop);
        } else {
            clone->next  = osl_loop_clone_one(loop);
            clone = clone->next;
        }

        loop = loop->next;
    }

    return head;
}

/**
 * osl_loop_equal_one function:
 * this function returns true if the two loop structures are the same
 * (content-wise), false otherwise. This functions considers two loop
 * structures as equal if the order of the array names differ, however
 * the identifiers and names must be the same.
 *
 * \param[in] a1 The first loop structure.
 * \param[in] a2 The second loop structure.
 * \return       1 if a1 and a2 are the same (content-wise), 0 otherwise.
 */
int osl_loop_equal_one(osl_loop_p a1, osl_loop_p a2) {
    size_t i, j, found;

    if (a1 == a2)
        return 1;

    if (((a1 == NULL) && (a2 != NULL)) || ((a1 != NULL) && (a2 == NULL))) {
        //OSL_info("loops are not the same (compare with NULL)");
        return 0;
    }

    // Check whether the number of names is the same.
    if (a1->nb_stmts != a2->nb_stmts) {
        //OSL_info("loops are not the same (nb_stmts)");
        return 0;
    }

    if (strcmp(a1->iter, a2->iter)) {
        //OSL_info("loops are not the same (iter name)");
        return 0;
    }
    // We accept a different order of the names, as long as the identifiers
    // are the same.
    for (i = 0; i < a1->nb_stmts; i++) {
        found = 0;
        for (j = 0; j < a2->nb_stmts; j++) {
            if (a1->stmt_ids[i] == a2->stmt_ids[j]) {
                found = 1;
                break;
            }
        }
        if (found != 1) {
            //OSL_info("loop are not the same (stmt ids)");
            return 0;
        }
    }

    //TODO: necessarily same ???
    if (a1->private_vars != a2->private_vars) { // NULL check
        if (strcmp(a1->private_vars, a2->private_vars)) {
            //OSL_info("loops are not the same (private vars)");
            return 0;
        }
    }

    //TODO: necessarily same ???
    if (a1->directive != a2->directive) {
        //OSL_info("loops are not the same (directive)");
        return 0;
    }

    if (a1->user != a2->user) { // NULL check
        if (strcmp(a1->user, a2->user)) {
            return 0;
        }
    }

    return 1;
}

/**
 * osl_loop_equal function:
 * this function returns true if the two loop lists are the same
 * (content-wise), false otherwise. Two lists are equal if one contains
 * all the elements of the other and vice versa. The exact order of the
 * nodes is not taken into account by this function.
 *
 * \param[in] a1  The first loop list.
 * \param[in] a2  The second loop list.
 * \return        1 if a1 and a2 are the same (content-wise), 0 otherwise.
 */
int osl_loop_equal(osl_loop_p a1, osl_loop_p a2) {
    int found = 0;

    if (a1 == a2)
        return 1;

    if (((a1 == NULL) && (a2 != NULL)) || ((a1 != NULL) && (a2 == NULL))) {
        OSL_info("lists of loops are not the same (compare with NULL)");
        return 0;
    }

    if (osl_loop_count(a1) != osl_loop_count(a2)) {
        OSL_info("list of loops are not the same");
        return 0;
    }

    while (a1) {
        found = 0;
        osl_loop_p temp = a2;

        while (temp) {
            if(osl_loop_equal_one(a1, temp)==1) {
                found= 1;
                break;
            }
            temp = temp->next;
        }

        if(found!=1) {
            OSL_info("list of loops are not the same");
            return 0;
        }
        a1 = a1->next;
    }

    return 1;
}


/**
 * osl_loop_interface function:
 * this function creates an interface structure corresponding to the loop
 * extension and returns it.
 *
 * \return  An interface structure for the loop extension.
 */
osl_interface_p osl_loop_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_LOOP);
    interface->idump  = (osl_idump_f)osl_loop_idump;
    interface->sprint = (osl_sprint_f)osl_loop_sprint;
    interface->sread  = (osl_sread_f)osl_loop_sread;
    interface->malloc = (osl_malloc_f)osl_loop_malloc;
    interface->free   = (osl_free_f)osl_loop_free;
    interface->clone  = (osl_clone_f)osl_loop_clone;
    interface->equal  = (osl_equal_f)osl_loop_equal;

    return interface;
}


/**
 * osl_loop_add function:
 * this function adds a loop structure at the end of the list
 *
 * \param[in,out] ll  Pointer to a list of loops.
 * \param[in] loop    Pointer to the loop structure to be added.
 */
void osl_loop_add(osl_loop_p loop, osl_loop_p *ll) {

    while (*ll != NULL)
        ll = &(*ll)->next;

    *ll = loop;
}


/**
 * osl_loop_count:
 * this function returns the number of elements in the list
 *
 * \param[in] ll  Pointer to a list of loops.
 * \return        Number of elements in the list
 */
int osl_loop_count(osl_loop_p ll) {
    int count = 0;
    while (ll) {
        count++;
        ll = ll->next;
    }

    return count;
}
