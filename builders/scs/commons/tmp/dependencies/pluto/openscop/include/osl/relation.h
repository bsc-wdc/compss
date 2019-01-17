
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                          relation.h                             **
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


#ifndef OSL_RELATION_H
# define OSL_RELATION_H

# include <stdio.h>
# include <osl/int.h>
# include <osl/names.h>
# include <osl/vector.h>

# if defined(__cplusplus)
extern "C"
{
# endif

# define OSL_URI_RELATION "relation"

/**
 * The osl_relation_t structure stores a union of relations. It is a
 * NULL-terminated linked list of relations. Each relation is described
 * using a matrix where each row represents a linear constraint. The entries
 * of each row are organised in the following order:
 * - An equality/inequality tag: 0 means the row corresponds to an
 *   equality constraint == 0, 1 means it is an inequality >= 0.
 * - The coefficients of the output dimensions.
 * - The coefficients of the input dimensions (0 for a set).
 * - The coefficients of the local (existentially quantified) dimensions.
 * - The coefficients of the parameters.
 * - The coefficient of the constant.
 * Thus we have the following invariant: nb_columns =
 * 1 + nb_output_dims + nb_input_dims + dims + nb_parameters + 1.
 * Moreover we use the following conventions:
 * - Sets (e.g., iteration domains) are the images of relations with a
 *   zero-dimensional domain, hence the number of input dimensions is 0.
 * - The first output dimension of any access relations corresponds to
 *   the name of the array.
 * The type field may provide some semantics about the relation, it may be:
 * - Undefined : OSL_UNDEFINED,
 * - An iteration domain : OSL_TYPE_DOMAIN,
 * - A scattering relation : OSL_TYPE_SCATTERING,
 * - An access relation : OSL_TYPE_ACCESS.
 */
struct osl_relation {
    int type;                   /**< Semantics about the relation */
    int precision;              /**< Precision of relation matrix elements*/
    int nb_rows;                /**< Number of rows */
    int nb_columns;	      /**< Number of columns */
    int nb_output_dims;         /**< Number of output dimensions */
    int nb_input_dims;          /**< Number of input dimensions */
    int nb_local_dims;          /**< Number of local (existentially
                                   quantified) dimensions */
    int nb_parameters;          /**< Number of parameters */
    osl_int_t** m;              /**< An array of pointers to the beginning
			           of each row of the relation matrix */
    void* usr;                  /**< User-managed field, untouched by osl */
    struct osl_relation * next; /**< Pointer to the next relation in the
                                   union of relations (NULL if none) */
};
typedef struct osl_relation   osl_relation_t;
typedef struct osl_relation * osl_relation_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void           osl_relation_idump(FILE *, osl_relation_p, int);
void           osl_relation_dump(FILE *, osl_relation_p);
char *         osl_relation_expression(osl_relation_p relation,
                                       int row, char ** names);
char *         osl_relation_spprint_polylib(osl_relation_p, osl_names_p);
char *         osl_relation_spprint(osl_relation_p, osl_names_p);
void           osl_relation_pprint(FILE *, osl_relation_p, osl_names_p);
char *         osl_relation_sprint(osl_relation_p);
void           osl_relation_print(FILE *, osl_relation_p);

// SCoPLib Compatibility
char *         osl_relation_spprint_polylib_scoplib(osl_relation_p,
        osl_names_p, int, int);
char *         osl_relation_spprint_scoplib(osl_relation_p, osl_names_p,
        int, int);
void           osl_relation_pprint_scoplib(FILE *, osl_relation_p,
        osl_names_p, int, int);

/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_relation_p osl_relation_pread(FILE *, int);
osl_relation_p osl_relation_read(FILE *);
osl_relation_p osl_relation_psread(char **, int);
osl_relation_p osl_relation_psread_polylib(char **, int);
osl_relation_p osl_relation_sread(char **);
osl_relation_p osl_relation_sread_polylib(char **);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_relation_p osl_relation_pmalloc(int, int, int);
osl_relation_p osl_relation_malloc(int, int);
void           osl_relation_free_inside(osl_relation_p);
void           osl_relation_free(osl_relation_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
int             osl_relation_nb_components(osl_relation_p relation);
osl_relation_p  osl_relation_nclone(osl_relation_p, int);
osl_relation_p  osl_relation_clone_nconstraints(osl_relation_p, int);
osl_relation_p  osl_relation_clone(osl_relation_p);
void            osl_relation_add(osl_relation_p *, osl_relation_p);
void            osl_relation_remove_part(osl_relation_p *, osl_relation_p);
osl_relation_p  osl_relation_union(osl_relation_p, osl_relation_p);
void            osl_relation_replace_vector(osl_relation_p, osl_vector_p, int);
void            osl_relation_insert_vector(osl_relation_p, osl_vector_p, int);
osl_relation_p  osl_relation_concat_vector(osl_relation_p, osl_vector_p);
void            osl_relation_insert_blank_row(osl_relation_p, int);
void            osl_relation_insert_blank_column(osl_relation_p, int);
void            osl_relation_add_vector(osl_relation_p, osl_vector_p, int);
void            osl_relation_sub_vector(osl_relation_p, osl_vector_p, int);
osl_relation_p  osl_relation_from_vector(osl_vector_p);
void            osl_relation_replace_constraints(osl_relation_p,
        osl_relation_p, int);
void            osl_relation_insert_constraints(osl_relation_p,
        osl_relation_p, int);
void            osl_relation_swap_constraints(osl_relation_p, int, int);
void            osl_relation_remove_row(osl_relation_p, int);
void            osl_relation_remove_column(osl_relation_p, int);
void            osl_relation_insert_columns(osl_relation_p, osl_relation_p,int);
osl_relation_p  osl_relation_concat_constraints(osl_relation_p, osl_relation_p);
int             osl_relation_part_equal(osl_relation_p, osl_relation_p);
int             osl_relation_equal(osl_relation_p, osl_relation_p);
int             osl_relation_integrity_check(osl_relation_p, int, int, int,int);
void            osl_relation_set_attributes_one(osl_relation_p,
        int, int, int, int);
void            osl_relation_set_attributes(osl_relation_p, int, int, int, int);
void            osl_relation_set_type(osl_relation_p, int);
int             osl_relation_get_array_id(osl_relation_p);
int             osl_relation_is_access(osl_relation_p);
void            osl_relation_get_attributes(osl_relation_p,
        int *, int *, int *, int *, int *);
osl_relation_p  osl_relation_extend_output(osl_relation_p, int);
osl_interface_p osl_relation_interface(void);
void            osl_relation_set_precision(int const, osl_relation_p);
void            osl_relation_set_same_precision(osl_relation_p, osl_relation_p);

# if defined(__cplusplus)
}
# endif
#endif /* define OSL_RELATION_H */
