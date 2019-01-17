
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                  dependence.h                           **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: september 18th 2003              **
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

#ifndef CANDL_DEPENDENCE_H
# define CANDL_DEPENDENCE_H

# include <stdio.h>
# include <candl/options.h>

# define CANDL_ARRAY_BUFF_SIZE 2048
# define CANDL_VAR_UNDEF       1
# define CANDL_VAR_IS_DEF      2
# define CANDL_VAR_IS_USED     3
# define CANDL_VAR_IS_DEF_USED 4

# if defined(__cplusplus)
extern "C"
{
# endif

struct osl_relation;
struct osl_statement;
struct osl_scop;
struct osl_dependence;
struct candl_label_mapping;

#ifdef CANDL_SUPPORTS_ISL
struct osl_dependence* candl_dependence_isl_simplify(struct osl_dependence*,
        struct osl_scop*);
# endif

/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void                   candl_dependence_pprint(FILE*, struct osl_dependence*);
void                   candl_dependence_view(struct osl_dependence*);

/******************************************************************************
 *                             Processing functions                           *
 ******************************************************************************/
int                    candl_dependence_gcd_test(struct osl_statement*,
        struct osl_statement*,
        struct osl_relation*, int);
int                    candl_dependence_check(struct osl_scop*,
        struct osl_dependence*,
        candl_options_p);
struct osl_dependence* candl_dependence_single(struct osl_scop*, candl_options_p);
struct osl_dependence* candl_dependence(struct osl_scop*,
                                        candl_options_p);
void                   candl_dependence_add_extension(struct osl_scop*,
        candl_options_p);
void                   candl_dependence_remap(struct osl_dependence*,
        struct osl_scop*,
        struct candl_label_mapping*);

/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
int                    candl_dependence_init_fields(struct osl_scop*,
        struct osl_dependence*);

/******************************************************************************
 *                          Scalar analysis functions                         *
 ******************************************************************************/
int                    candl_dependence_var_is_scalar(struct osl_scop*, int);
struct osl_statement** candl_dependence_refvar_chain(struct osl_scop*,
        struct osl_statement*, int, int);
int                    candl_dependence_var_is_ref(struct osl_statement*, int);
int                    candl_dependence_check_domain_is_included(
    struct osl_statement*,
    struct osl_statement*,
    struct osl_relation*, int);
int                    candl_dependence_scalar_is_privatizable_at(
    struct osl_scop*, int, int);
int                    candl_dependence_is_loop_carried(struct osl_dependence*, int);
int                    candl_dependence_is_loop_independent(struct osl_dependence*);
void                   candl_dependence_prune_scalar_waw(struct osl_scop*,
        candl_options_p,
        struct osl_dependence**);
void                   candl_dependence_prune_with_privatization(
    struct osl_scop*,
    candl_options_p,
    struct osl_dependence**);
int                    candl_dependence_scalar_renaming(struct osl_scop*,
        candl_options_p,
        struct osl_dependence**);
int                    candl_dependence_analyze_scalars(struct osl_scop*,
        candl_options_p);

/******************************************************************************
 *                          Miscellaneous functions                           *
 ******************************************************************************/
struct osl_relation*   candl_dependence_get_relation_ref_source_in_dep(
    struct osl_dependence*);
struct osl_relation*   candl_dependence_get_relation_ref_target_in_dep(
    struct osl_dependence*);
int                    candl_num_dependences(struct osl_dependence*);
void                   candl_compute_last_writer(struct osl_dependence*,
        struct osl_scop*);
struct osl_dependence* candl_dependence_prune_transitively_covered(
    struct osl_dependence*);

# if defined(__cplusplus)
}
# endif
#endif /* define CANDL_DEPENDENCE_H */

