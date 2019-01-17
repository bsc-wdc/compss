
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/irregular.c                      **
 **-----------------------------------------------------------------**
 **                   First version: 07/12/2010                     **
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

#include <osl/macros.h>
#include <osl/util.h>
#include <osl/strings.h>
#include <osl/interface.h>
#include <osl/extensions/irregular.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_irregular_idump function:
 * this function displays an osl_irregular_t structure (*irregular) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param file    The file where the information has to be printed.
 * \param irregular The irregular structure whose information has to be printed.
 * \param level   Number of spaces before printing, for each line.
 */
void osl_irregular_idump(FILE * file, osl_irregular_p irregular, int level) {
    int i,j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (irregular != NULL)
        fprintf(file, "+-- osl_irregular_t\n");
    else
        fprintf(file, "+-- NULL irregular\n");

    if (irregular != NULL) {
        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the irregular contents.

        // Print statements
        for (i = 0; i < irregular->nb_statements; i++) {
            fprintf(file, "statement%d's predicats : ", i);
            for(j = 0; j < irregular->nb_predicates[i]; j++)
                fprintf(file, "%d ", irregular->predicates[i][j]);
            fprintf(file, "\n");
        }
        // Print predicats
        // controls :
        for (i = 0; i < irregular->nb_control; i++) {
            fprintf(file, "predicat%d's\niterators : ", i);
            for(j = 0; j < irregular->nb_iterators[i]; j++)
                fprintf(file, "%s ", irregular->iterators[i][j]);
            fprintf(file, "\ncontrol body: %s\n", irregular->body[i]);
        }
        // exits :
        for(i = irregular->nb_control;
                i < irregular->nb_control + irregular->nb_exit; i++) {
            fprintf(file, "predicat%d's\niterators : ", i);
            for(j = 0; j < irregular->nb_iterators[i]; j++)
                fprintf(file, "%s ", irregular->iterators[i][j]);
            fprintf(file, "\nexit body: %s\n", irregular->body[i]);
        }
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_irregular_dump function:
 * this function prints the content of an osl_irregular_t structure
 * (*irregular) into a file (file, possibly stdout).
 * \param file    The file where the information has to be printed.
 * \param irregular The irregular structure whose information has to be printed.
 */
void osl_irregular_dump(FILE * file, osl_irregular_p irregular) {
    osl_irregular_idump(file, irregular, 0);
}


/**
 * osl_irregular_sprint function:
 * this function prints the content of an osl_irregular_t structure
 * (*irregular) into a string (returned) in the OpenScop textual format.
 * \param  irregular The irregular structure whose information has to be printed.
 * \return A string containing the OpenScop dump of the irregular structure.
 */
char * osl_irregular_sprint(osl_irregular_p irregular) {
    size_t high_water_mark = OSL_MAX_STRING;
    int i,j;
    char * string = NULL;
    char * buffer;

    if (irregular != NULL) {
        OSL_malloc(string, char *, high_water_mark * sizeof(char));
        OSL_malloc(buffer, char *, OSL_MAX_STRING * sizeof(char));
        string[0] = '\0';

        // Print the begin tag.
        sprintf(buffer, OSL_TAG_IRREGULAR_START);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Print the content.
        sprintf(buffer, "\n%d\n", irregular->nb_statements);
        for(i=0; i<irregular->nb_statements; i++) {
            sprintf(buffer, "%s%d ", buffer, irregular->nb_predicates[i]);
            for(j=0; j<irregular->nb_predicates[i]; j++) {
                sprintf(buffer, "%s%d ", buffer, irregular->predicates[i][j]);
            }
            sprintf(buffer, "%s\n", buffer);
        }
        // Print the predicates.
        // controls:
        sprintf(buffer, "%s%d\n", buffer, irregular->nb_control);
        sprintf(buffer, "%s%d\n", buffer, irregular->nb_exit);
        for(i=0; i<irregular->nb_control; i++) {
            sprintf(buffer, "%s%d ", buffer, irregular->nb_iterators[i]);
            for(j=0; j<irregular->nb_iterators[i]; j++)
                sprintf(buffer, "%s%s ", buffer, irregular->iterators[i][j]);
            sprintf(buffer, "%s\n%s\n", buffer, irregular->body[i]);
        }
        // exits:
        for(i=0; i<irregular->nb_exit; i++) {
            sprintf(buffer, "%s%d ", buffer, irregular->nb_iterators[
             irregular->nb_control + i]);
            for(j=0; j<irregular->nb_iterators[irregular->nb_control + i]; j++)
                sprintf(buffer, "%s%s ", buffer, irregular->iterators[
                            irregular->nb_control+i][j]);
            sprintf(buffer, "%s\n%s\n", buffer, irregular->body[
             irregular->nb_control + i]);
        }

        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Print the end tag.
        sprintf(buffer, OSL_TAG_IRREGULAR_STOP"\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Keep only the memory space we need.
        OSL_realloc(string, char *, (strlen(string) + 1) * sizeof(char));
        free(buffer);
    }

    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/

/**
 * osl_irregular_sread function:
 * this function reads a irregular structure from a string complying to the
 * OpenScop textual format and returns a pointer to this irregular structure.
 * The string should contain only one textual format of a irregular structure.
 * \param  extensions The input string where to find a irregular structure.
 * \return A pointer to the irregular structure that has been read.
 */
osl_irregular_p osl_irregular_sread(char ** extensions_fixme) {
    char * content,*tok;
    int i,j;
    osl_irregular_p irregular;

    // FIXME: this is a quick and dirty thing to accept char ** instead
    //        of char * in the parameter: really do it and update the
    //        pointer to after what has been read.
    content = *extensions_fixme;

    if (content == NULL) {
        OSL_debug("no irregular optional tag");
        return NULL;
    }

    if (strlen(content) > OSL_MAX_STRING)
        OSL_error("irregular too long");

    irregular = osl_irregular_malloc();

    // nb statements
    tok = strtok(content," \n");
    irregular->nb_statements = atoi(tok);
    OSL_malloc(irregular->predicates, int **,
               sizeof(int*) * irregular->nb_statements);
    OSL_malloc(irregular->nb_predicates, int *,
               sizeof(int) * irregular->nb_statements);

    // get predicats
    for(i = 0; i < irregular->nb_statements; i++) {
        // nb conditions
        tok = strtok(NULL," \n");
        irregular->nb_predicates[i] = atoi(tok);
        OSL_malloc(irregular->predicates[i], int *,
                   sizeof(int) * irregular->nb_predicates[i]);
        for(j = 0; j < irregular->nb_predicates[i]; j++) {
            tok = strtok(NULL, " \n");
            irregular->predicates[i][j] = atoi(tok);
        }
    }
    // Get nb predicat
    // control and exits :
    tok = strtok(NULL, " \n");
    irregular->nb_control=atoi(tok);
    tok = strtok(NULL, " \n");
    irregular->nb_exit = atoi(tok);

    int nb_predicates = irregular->nb_control + irregular->nb_exit;

    OSL_malloc(irregular->iterators, char ***,
               sizeof(char **) * nb_predicates);
    OSL_malloc(irregular->nb_iterators, int *, sizeof(int) * nb_predicates);
    OSL_malloc(irregular->body, char **, sizeof(char *) * nb_predicates);

    for(i = 0; i < nb_predicates; i++) {
        // Get number of iterators
        tok = strtok(NULL, " \n");
        irregular->nb_iterators[i] = atoi(tok);
        OSL_malloc(irregular->iterators[i], char **,
                   sizeof(char *) * irregular->nb_iterators[i]);

        // Get iterators
        for(j = 0; j < irregular->nb_iterators[i]; j++)
            OSL_strdup(irregular->iterators[i][j], strtok(NULL, " \n"));
        // Get predicat string
        OSL_strdup(irregular->body[i], strtok(NULL, "\n"));
    }

    return irregular;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_irregular_malloc function:
 * This function allocates the memory space for an osl_irregular_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty irregular structure with fields set to
 *         default values.
 */
osl_irregular_p osl_irregular_malloc(void) {
    osl_irregular_p irregular;

    OSL_malloc(irregular, osl_irregular_p,
               sizeof(osl_irregular_t));
    irregular->nb_statements = 0;
    irregular->predicates = NULL;
    irregular->nb_predicates = NULL;
    irregular->nb_control = 0;
    irregular->nb_exit = 0;
    irregular->nb_iterators = NULL;
    irregular->iterators = NULL;
    irregular->body = NULL;

    return irregular;
}


/**
 * osl_irregular_free function:
 * This function frees the allocated memory for an osl_irregular_t
 * structure.
 * \param irregular The pointer to the irregular structure we want to free.
 */
void osl_irregular_free(osl_irregular_p irregular) {
    int i, j, nb_predicates;

    if (irregular != NULL) {
        for(i = 0; i < irregular->nb_statements; i++)
            free(irregular->predicates[i]);

        if(irregular->predicates != NULL)
            free(irregular->predicates);

        nb_predicates = irregular->nb_control + irregular->nb_exit;
        for(i = 0; i < nb_predicates; i++) {
            for(j = 0; j < irregular->nb_iterators[i]; j++)
                free(irregular->iterators[i][j]);
            free(irregular->iterators[i]);
            free(irregular->body[i]);
        }
        if(irregular->iterators != NULL)
            free(irregular->iterators);
        if(irregular->nb_iterators != NULL)
            free(irregular->nb_iterators);
        if(irregular->body != NULL)
            free(irregular->body);
        if(irregular->nb_predicates != NULL)
            free(irregular->nb_predicates);
        free(irregular);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_irregular_clone function:
 * This function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_irregular_t data structure.
 * \param irregular The pointer to the irregular structure we want to copy.
 * \return A pointer to the copy of the irregular structure.
 */
osl_irregular_p osl_irregular_clone(osl_irregular_p irregular) {
    int i,j;
    osl_irregular_p copy;

    if (irregular == NULL)
        return NULL;

    copy = osl_irregular_malloc();
    copy->nb_statements = irregular->nb_statements;
    copy->nb_predicates = (int *)malloc(sizeof(int)*copy->nb_statements);
    if (copy->nb_predicates == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    copy->predicates = (int **)malloc(sizeof(int*)*copy->nb_statements);
    if (copy->predicates == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<copy->nb_statements; i++) {
        copy->nb_predicates[i]=irregular->nb_predicates[i];
        copy->predicates[i] = (int *)malloc(sizeof(int)*copy->nb_predicates[i]);
        if (copy->predicates[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<copy->nb_predicates[i]; j++)
            copy->predicates[i][j] = irregular->predicates[i][j];
    }

    copy->nb_control  = irregular->nb_control;
    copy->nb_exit     = irregular->nb_exit;
    int nb_predicates = irregular->nb_control + irregular->nb_exit;
    copy->nb_iterators = (int *)malloc(sizeof(int)*nb_predicates);
    if (copy->nb_iterators == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    copy->iterators = (char ***)malloc(sizeof(char**)*nb_predicates);
    if (copy->iterators == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    copy->body = (char **)malloc(sizeof(char*)*nb_predicates);
    if (copy->body == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<nb_predicates; i++) {
        copy->nb_iterators[i] = irregular->nb_iterators[i];
        copy->iterators[i] = (char**)malloc(sizeof(char*)*copy->nb_iterators[i]);
        if (copy->iterators[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<copy->nb_iterators[i]; j++)
            OSL_strdup(copy->iterators[i][j], irregular->iterators[i][j]);
        OSL_strdup(copy->iterators[i][j], irregular->body[i]);
    }

    return copy;
}


/**
 * osl_irregular_equal function:
 * this function returns true if the two irregular structures are the same
 * (content-wise), false otherwise. This functions considers two irregular
 * \param c1  The first irregular structure.
 * \param c2  The second irregular structure.
 * \return 1 if c1 and c2 are the same (content-wise), 0 otherwise.
 */
int
osl_irregular_equal(osl_irregular_p c1, osl_irregular_p c2) {
    int i,j,bool = 0;
    if (c1 == c2)
        return 1;

    if (((c1 == NULL) && (c2 != NULL)) || ((c1 != NULL) && (c2 == NULL)))
        return 0;

    if(c1->nb_statements != c2->nb_statements ||
            c1->nb_control    != c2->nb_control    ||
            c1->nb_exit       != c2->nb_exit)
        return 0;
    i=0;
    while(bool == 0 && i < c1->nb_statements) {
        bool = c1->nb_predicates[i] != c2->nb_predicates[i] ? 1 : 0;
        i++;
    }
    if(bool != 0)
        return 0;

    i = 0;
    while(bool == 0 && i < c1->nb_control + c1->nb_exit) {
        bool += c1->nb_iterators[i] != c2->nb_iterators[i] ? 1 : 0;
        bool += strcmp(c1->body[i],c2->body[i]);
        j = 0;
        while(bool == 0 && j < c1->nb_iterators[i]) {
            bool += strcmp(c1->iterators[i][j],c2->iterators[i][j]);
            j++;
        }
        i++;
    }
    if(bool != 0)
        return 0;
    return 1;
}

osl_irregular_p osl_irregular_add_control(
    osl_irregular_p irregular,
    char** iterators,
    int nb_iterators,
    char* body) {
    int i,j;
    osl_irregular_p result=osl_irregular_malloc();

    result->nb_control    = irregular->nb_control + 1;
    result->nb_exit       = irregular->nb_exit;
    result->nb_statements = irregular->nb_statements;
    int nb_predicates     = result->nb_control + result->nb_exit;

    result->iterators = (char***)malloc(sizeof(char**)*nb_predicates);
    result->nb_iterators = (int*)malloc(sizeof(int)*nb_predicates);
    result->body = (char**)malloc(sizeof(char*)*nb_predicates);
    if (result->iterators == NULL ||
            result->nb_iterators == NULL ||
            result->body == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    //copy controls
    for(i=0; i<irregular->nb_control; i++) {
        result->nb_iterators[i] = irregular->nb_iterators[i];
        OSL_strdup(result->body[i], irregular->body[i]);
        result->iterators[i] = (char**)malloc(sizeof(char*)  *
                                              irregular->nb_iterators[i]);
        if (result->iterators[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<irregular->nb_iterators[i]; j++)
            OSL_strdup(result->iterators[i][j], irregular->iterators[i][j]);
    }
    //add controls
    result->iterators[irregular->nb_control] = (char**)malloc(sizeof(char*)*nb_iterators);
    if (result->iterators[irregular->nb_control] == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<nb_iterators; i++)
        OSL_strdup(result->iterators[irregular->nb_control][i], iterators[i]);
    result->nb_iterators[irregular->nb_control] = nb_iterators;
    OSL_strdup(result->body[irregular->nb_control], body);
    //copy exits
    for(i=result->nb_control; i<nb_predicates; i++) {
        result->nb_iterators[i] = irregular->nb_iterators[i-1];
        OSL_strdup(result->body[i], irregular->body[i-1]);
        result->iterators[i] = (char**)malloc(sizeof(char*)  *
                                              irregular->nb_iterators[i-1]);
        if (result->iterators[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<irregular->nb_iterators[i-1]; j++)
            OSL_strdup(result->iterators[i][j], irregular->iterators[i-1][j]);
    }
    // copy statements
    result->nb_predicates = (int*)malloc(sizeof(int)*irregular->nb_statements);
    result->predicates = (int**)malloc(sizeof(int*)*irregular->nb_statements);
    if (result->nb_predicates == NULL || result->predicates == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<irregular->nb_statements; i++) {
        result->predicates[i] = (int*)malloc(sizeof(int)*irregular->nb_predicates[i]);
        if (result->predicates[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        result->nb_predicates[i] = irregular->nb_predicates[i];
        for(j=0; j<irregular->nb_predicates[i]; j++)
            result->predicates[i][j]=irregular->predicates[i][j];
    }
    return result;
}


osl_irregular_p osl_irregular_add_exit(
    osl_irregular_p irregular,
    char** iterators,
    int nb_iterators,
    char* body) {
    int i,j;
    osl_irregular_p result=osl_irregular_malloc();

    result->nb_control    = irregular->nb_control;
    result->nb_exit       = irregular->nb_exit + 1;
    result->nb_statements = irregular->nb_statements;
    int nb_predicates     = result->nb_control + result->nb_exit;

    result->iterators = (char***)malloc(sizeof(char**)*nb_predicates);
    result->nb_iterators = (int*)malloc(sizeof(int)*nb_predicates);
    result->body = (char**)malloc(sizeof(char*)*nb_predicates);
    if (result->iterators == NULL ||
            result->nb_iterators == NULL ||
            result->body == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    //copy controls and exits
    for(i=0; i<nb_predicates - 1; i++) {
        result->nb_iterators[i] = irregular->nb_iterators[i];
        OSL_strdup(result->body[i], irregular->body[i]);
        result->iterators[i] = (char**)malloc(sizeof(char*)  *
                                              irregular->nb_iterators[i]);
        if (result->iterators[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<irregular->nb_iterators[i]; j++)
            OSL_strdup(result->iterators[i][j], irregular->iterators[i][j]);
    }
    //add exit
    result->iterators[nb_predicates-1] = (char**)malloc(sizeof(char*)*nb_iterators);
    if (result->iterators[nb_predicates-1] == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }

    for(i=0; i<nb_iterators; i++)
        OSL_strdup(result->iterators[nb_predicates-1][i], iterators[i]);
    result->nb_iterators[nb_predicates-1] = nb_iterators;
    OSL_strdup(result->body[nb_predicates-1], body);
    // copy statements
    result->nb_predicates = (int*)malloc(sizeof(int)*irregular->nb_statements);
    result->predicates = (int**)malloc(sizeof(int*)*irregular->nb_statements);
    if (result->nb_predicates == NULL || result->predicates == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<irregular->nb_statements; i++) {
        result->predicates[i] = (int*)malloc(sizeof(int)*irregular->nb_predicates[i]);
        if (result->predicates[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        result->nb_predicates[i] = irregular->nb_predicates[i];
        for(j=0; j<irregular->nb_predicates[i]; j++)
            result->predicates[i][j]=irregular->predicates[i][j];
    }
    return result;
}


osl_irregular_p osl_irregular_add_predicates(
    osl_irregular_p irregular,
    int* predicates,
    int nb_add_predicates) {
    int i,j;
    osl_irregular_p result=osl_irregular_malloc();

    result->nb_control    = irregular->nb_control;
    result->nb_exit       = irregular->nb_exit;
    result->nb_statements = irregular->nb_statements+1;
    int nb_predicates     = result->nb_control + result->nb_exit;

    result->iterators = (char***)malloc(sizeof(char**)*nb_predicates);
    result->nb_iterators = (int*)malloc(sizeof(int)*nb_predicates);
    result->body = (char**)malloc(sizeof(char*)*nb_predicates);
    if (result->iterators == NULL ||
            result->nb_iterators == NULL ||
            result->body == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    //copy controls and exits
    for(i=0; i<nb_predicates; i++) {
        result->nb_iterators[i] = irregular->nb_iterators[i];
        OSL_strdup(result->body[i], irregular->body[i]);
        result->iterators[i] = (char**)malloc(sizeof(char*)  *
                                              irregular->nb_iterators[i]);
        if (result->iterators[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        for(j=0; j<irregular->nb_iterators[i]; j++)
            OSL_strdup(result->iterators[i][j], irregular->iterators[i][j]);
    }
    //copy statements
    result->nb_predicates = (int*)malloc(sizeof(int)*result->nb_statements);
    result->predicates = (int**)malloc(sizeof(int*)*result->nb_statements);
    if (result->nb_predicates == NULL ||
            result->predicates == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<irregular->nb_statements; i++) {
        result->predicates[i] = (int*)malloc(sizeof(int)*irregular->nb_predicates[i]);
        if (result->predicates[i] == NULL) {
            fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
            exit(1);
        }
        result->nb_predicates[i] = irregular->nb_predicates[i];
        for(j=0; j<irregular->nb_predicates[i]; j++)
            result->predicates[i][j]=irregular->predicates[i][j];
    }
    //add statement
    result->predicates[irregular->nb_statements] = (int*)malloc(sizeof(int)*nb_add_predicates);
    if (result->predicates[irregular->nb_statements] == NULL) {
        fprintf(stderr, "[OpenScop] Error: memory overflow.\n");
        exit(1);
    }
    for(i=0; i<nb_add_predicates; i++)
        result->predicates[irregular->nb_statements][i] = predicates[i];
    result->nb_predicates[irregular->nb_statements] = nb_add_predicates;

    return result;


}


/**
 * osl_irregular_interface function:
 * this function creates an interface structure corresponding to the irregular
 * extension and returns it).
 * \return An interface structure for the irregular extension.
 */
osl_interface_p osl_irregular_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_IRREGULAR);
    interface->idump  = (osl_idump_f)osl_irregular_idump;
    interface->sprint = (osl_sprint_f)osl_irregular_sprint;
    interface->sread  = (osl_sread_f)osl_irregular_sread;
    interface->malloc = (osl_malloc_f)osl_irregular_malloc;
    interface->free   = (osl_free_f)osl_irregular_free;
    interface->clone  = (osl_clone_f)osl_irregular_clone;
    interface->equal  = (osl_equal_f)osl_irregular_equal;

    return interface;
}


