
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                   options.h                             **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: september 8th 2003               **
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
* CAnDL, the Chunky Dependence Analyser                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/

#ifndef CANDL_OPTIONS_H
# define CANDL_OPTIONS_H

# include <stdio.h>

# if defined(__cplusplus)
extern "C"
{
# endif

/**
 * candl_options structure:
 * this structure contains all the informations on the state of Candl options.
 */
struct candl_options {
    /* OPTIONS FOR DEPENDENCE COMPUTATION */
    int waw;       /**< 1 if write after write (output) dependences matter. */
    int raw;       /**< 1 if read  after write (flow)   dependences matter. */
    int war;       /**< 1 if write after read  (anti)   dependences matter. */
    int rar;       /**< 1 if read  after read  (input)  dependences matter. */
    int commute;   /**< 1 to use commutativity to simplify dependences. */
    int fullcheck; /**< 1 to compute all dependence violations. */
    int scalar_renaming; /**< 1 to enable scalar renaming. */
    int scalar_privatization; /**< 1 to enable scalar privatization. */
    int scalar_expansion; /**< 1 to enable scalar privatization. */
    int lastwriter; /**< 1 to compute last writer */
    int verbose; /**< 1 to enable verbose output. */
    int outscop; /**< 1 to print the scop with dependences. */
    int autocorrect; /**< 1 to correct violations. fullcheck is set to 1 and
                     * the -test is required.
                    */
    int unions;    /**< 1 to consider unions of relations */
    /* UNDOCUMENTED OPTIONS FOR THE AUTHOR ONLY */
    int view;      /**< 1 to call dot and gv to visualize the graphs. */
    int structure; /**< 1 to print internal dependence structure. */
    int prune_dups; /**< 1 to use experimental dependence pruning algorithm. */
};

typedef struct candl_options  candl_options_t;
typedef struct candl_options* candl_options_p;

/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/
void candl_options_print(FILE *, candl_options_p);

/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/
void candl_options_free(candl_options_p);

/******************************************************************************
 *                               Reading function                             *
 ******************************************************************************/
void candl_options_read(int, char **, FILE **, FILE **, FILE**, candl_options_p*);

/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/
candl_options_p candl_options_malloc(void);

#if defined(__cplusplus)
}
#endif
#endif /* define CANDL_OPTIONS_H */
