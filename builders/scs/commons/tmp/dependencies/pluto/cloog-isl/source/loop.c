
/**-------------------------------------------------------------------**
 **                              CLooG                                **
 **-------------------------------------------------------------------**
 **                             loop.c                                **
 **-------------------------------------------------------------------**
 **                  First version: october 26th 2001                 **
 **-------------------------------------------------------------------**/


/******************************************************************************
 *               CLooG : the Chunky Loop Generator (experimental)             *
 ******************************************************************************
 *                                                                            *
 * Copyright (C) 2001-2005 Cedric Bastoul                                     *
 *                                                                            *
 * This library is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU Lesser General Public                 *
 * License as published by the Free Software Foundation; either               *
 * version 2.1 of the License, or (at your option) any later version.         *
 *                                                                            *
 * This library is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU          *
 * Lesser General Public License for more details.                            *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public           *
 * License along with this library; if not, write to the Free Software        *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,                         *
 * Boston, MA  02110-1301  USA                                                *
 *                                                                            *
 * CLooG, the Chunky Loop Generator                                           *
 * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
 *                                                                            *
 ******************************************************************************/
/* CAUTION: the english used for comments is probably the worst you ever read,
 *          please feel free to correct and improve it !
 */

# include <stdlib.h>
# include <stdio.h>
# include "../include/cloog/cloog.h"

#define ALLOC(type) (type*)malloc(sizeof(type))


/******************************************************************************
 *                             Memory leaks hunting                           *
 ******************************************************************************/


/**
 * These functions and global variables are devoted to memory leaks hunting: we
 * want to know at each moment how many CloogLoop structures had been allocated
 * (cloog_loop_allocated) and how many had been freed (cloog_loop_freed).
 * Each time a CloogLoog structure is allocated, a call to the function
 * cloog_loop_leak_up() must be carried out, and respectively
 * cloog_loop_leak_down() when a CloogLoop structure is freed. The special
 * variable cloog_loop_max gives the maximal number of CloogLoop structures
 * simultaneously alive (i.e. allocated and non-freed) in memory.
 * - July 3rd->11th 2003: first version (memory leaks hunt and correction).
 */


static void cloog_loop_leak_up(CloogState *state) {
    state->loop_allocated++;
    if ((state->loop_allocated - state->loop_freed) > state->loop_max)
        state->loop_max = state->loop_allocated - state->loop_freed;
}


static void cloog_loop_leak_down(CloogState *state) {
    state->loop_freed++;
}


/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * cloog_loop_print_structure function:
 * Displays a loop structure in a way that trends to be understandable without
 * falling in a deep depression or, for the lucky ones, getting a headache...
 * Written by Olivier Chorier, Luc Marchaud, Pierre Martin and Romain Tartiere.
 * - April 24th 2005: Initial version.
 * - May   21rd 2005: - New parameter `F' for destination file (ie stdout),
 *                    - Minor tweaks.
 * - May   26th 2005: Memory leak hunt.
 * - June   2nd 2005: (Ced) Integration and minor fixes.
 * -June  22nd 2005: (Ced) Adaptation for GMP.
 */
void cloog_loop_print_structure(FILE * file, CloogLoop * loop, int level) {
    int i, j, first=1 ;

    if (loop) {
        /* Go to the right level. */
        for (i=0; i<level; i++)
            fprintf(file,"|\t") ;

        fprintf(file,"+-- CloogLoop\n") ;
    }

    /* For each loop. */
    while (loop) {
        if (!first) {
            /* Go to the right level. */
            for (i=0; i<level; i++)
                fprintf(file,"|\t") ;

            fprintf(file,"|   CloogLoop\n") ;
        } else
            first = 0 ;

        /* A blank line. */
        for(j=0; j<=level+1; j++)
            fprintf(file,"|\t") ;
        fprintf(file,"\n") ;

        /* Print the domain. */
        cloog_domain_print_structure(file, loop->domain, level+1, "CloogDomain");

        /* Print the stride. */
        for(j=0; j<=level; j++)
            fprintf(file,"|\t") ;
        if (loop->stride) {
            fprintf(file, "Stride: ");
            cloog_int_print(file, loop->stride->stride);
            fprintf(file, "\n");
            fprintf(file, "Offset: ");
            cloog_int_print(file, loop->stride->offset);
            fprintf(file, "\n");
        }

        /* A blank line. */
        for(j=0; j<=level+1; j++)
            fprintf(file,"|\t") ;
        fprintf(file,"\n") ;

        /* Print the block. */
        cloog_block_print_structure(file,loop->block,level+1) ;

        /* A blank line. */
        for (i=0; i<=level+1; i++)
            fprintf(file,"|\t") ;
        fprintf(file,"\n") ;

        /* Print inner if any. */
        if (loop->inner)
            cloog_loop_print_structure(file,loop->inner,level+1) ;

        /* And let's go for the next one. */
        loop = loop->next ;

        /* One more time something that is here only for a better look. */
        if (!loop) {
            /* Two blank lines if this is the end of the linked list. */
            for (j=0; j<2; j++) {
                for (i=0; i<=level; i++)
                    fprintf(file,"|\t") ;

                fprintf(file,"\n") ;
            }
        } else {
            /* A special blank line if the is a next loop. */
            for (i=0; i<=level; i++)
                fprintf(file,"|\t") ;
            fprintf(file,"V\n") ;
        }
    }
}


/**
 * cloog_loop_print function:
 * This function prints the content of a CloogLoop structure (start) into a
 * file (file, possibly stdout).
 * - June 2nd 2005: Now this very old function (probably as old as CLooG) is
 *                  only a frontend to cloog_loop_print_structure, with a quite
 *                  better human-readable representation.
 */
void cloog_loop_print(FILE * file, CloogLoop * loop) {
    cloog_loop_print_structure(file,loop,0) ;
}


/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/


/**
 * cloog_loop_free function:
 * This function frees the allocated memory for a CloogLoop structure (loop),
 * and frees its inner loops and its next loops.
 * - June 22nd 2005: Adaptation for GMP.
 */
void cloog_loop_free(CloogLoop * loop) {
    CloogLoop * next ;

    while (loop != NULL) {
        cloog_loop_leak_down(loop->state);

        next = loop->next ;
        cloog_domain_free(loop->domain) ;
        cloog_domain_free(loop->unsimplified);
        cloog_block_free(loop->block) ;
        if (loop->inner != NULL)
            cloog_loop_free(loop->inner) ;

        cloog_stride_free(loop->stride);
        free(loop) ;
        loop = next ;
    }
}


/**
 * cloog_loop_free_parts function:
 * This function frees the allocated memory for some parts of a CloogLoop
 * structure (loop), each other argument is a boolean having to be set to 1 if
 * we want to free the corresponding part, 0 otherwise. This function applies
 * the same freeing policy to its inner ans next loops recursively.
 * - July  3rd 2003: first version.
 * - June 22nd 2005: Adaptation for GMP.
 */
void cloog_loop_free_parts(loop, domain, block, inner, next)
CloogLoop * loop ;
int domain, block, inner, next ;
{
    CloogLoop * follow ;

    while (loop != NULL) {
        cloog_loop_leak_down(loop->state);
        follow = loop->next ;

        if (domain)
            cloog_domain_free(loop->domain) ;

        if (block)
            cloog_block_free(loop->block) ;

        if ((inner) && (loop->inner != NULL))
            cloog_loop_free_parts(loop->inner,domain,block,inner,1) ;

        cloog_domain_free(loop->unsimplified);
        cloog_stride_free(loop->stride);
        free(loop) ;
        if (next)
            loop = follow ;
        else
            loop = NULL ;
    }
}


/******************************************************************************
 *                              Reading functions                             *
 ******************************************************************************/


/**
 * Construct a CloogLoop structure from a given iteration domain
 * and statement number.
 */
CloogLoop *cloog_loop_from_domain(CloogState *state, CloogDomain *domain,
                                  int number) {
    int nb_iterators;
    CloogLoop * loop ;
    CloogStatement * statement ;

    /* Memory allocation and information reading for the first domain: */
    loop = cloog_loop_malloc(state);
    /* domain. */
    loop->domain = domain;
    if (loop->domain != NULL)
        nb_iterators = cloog_domain_dimension(loop->domain);
    else
        nb_iterators = 0 ;
    /* included statement block. */
    statement = cloog_statement_alloc(state, number + 1);
    loop->block = cloog_block_alloc(statement, 0, NULL, nb_iterators);

    return loop ;
}


/**
 * cloog_loop_read function:
 * This function reads loop data from a file (foo, possibly stdin) and
 * returns a pointer to a CloogLoop structure containing the read information.
 * This function can be used only for input file reading, when one loop is
 * associated with one statement.
 * - number is the statement block number carried by the loop (-1 if none).
 * - nb_parameters is the number of parameters.
 **
 * - September 9th 2002: first version.
 * - April    16th 2005: adaptation to new CloogStatement struct (with number).
 * - June     11th 2005: adaptation to new CloogBlock structure.
 * - June     22nd 2005: Adaptation for GMP.
 */
CloogLoop *cloog_loop_read(CloogState *state,
                           FILE *foo, int number, int nb_parameters) {
    int op1, op2, op3;
    char s[MAX_STRING];
    CloogDomain *domain;

    domain = cloog_domain_union_read(state, foo, nb_parameters);

    /* To read that stupid "0 0 0" line. */
    while (fgets(s,MAX_STRING,foo) == 0) ;
    while ((*s=='#' || *s=='\n') || (sscanf(s," %d %d %d",&op1,&op2,&op3)<3))
        if (fgets(s, MAX_STRING, foo))
            continue;

    return cloog_loop_from_domain(state, domain, number);
}


/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * cloog_loop_malloc function:
 * This function allocates the memory space for a CloogLoop structure and
 * sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * - November 21th 2005: first version.
 */
CloogLoop *cloog_loop_malloc(CloogState *state) {
    CloogLoop * loop ;

    /* Memory allocation for the CloogLoop structure. */
    loop = (CloogLoop *)malloc(sizeof(CloogLoop)) ;
    if (loop == NULL)
        cloog_die("memory overflow.\n");
    cloog_loop_leak_up(state);


    /* We set the various fields with default values. */
    loop->state    = state;
    loop->domain = NULL ;
    loop->unsimplified = NULL;
    loop->block  = NULL ;
    loop->usr    = NULL;
    loop->inner  = NULL ;
    loop->next   = NULL ;
    loop->otl = 0;
    loop->stride = NULL;

    return loop ;
}


/**
 * cloog_loop_alloc function:
 * This function allocates the memory space for a CloogLoop structure and
 * sets its fields with those given as input. Then it returns a pointer to the
 * allocated space.
 * - October  27th 2001: first version.
 * - June     22nd 2005: Adaptation for GMP.
 * - November 21th 2005: use of cloog_loop_malloc.
 */
CloogLoop *cloog_loop_alloc(CloogState *state,
                            CloogDomain *domain, int otl, CloogStride *stride,
                            CloogBlock *block, CloogLoop *inner, CloogLoop *next) {
    CloogLoop * loop ;

    loop = cloog_loop_malloc(state);

    loop->domain = domain ;
    loop->block  = block ;
    loop->inner  = inner ;
    loop->next   = next ;
    loop->otl = otl;
    loop->stride = cloog_stride_copy(stride);

    return(loop) ;
}


/**
 * cloog_loop_add function:
 * This function adds a CloogLoop structure (loop) at a given place (now) of a
 * NULL terminated list of CloogLoop structures. The beginning of this list
 * is (start). This function updates (now) to (loop), and updates (start) if the
 * added element is the first one -that is when (start) is NULL-.
 * - October 28th 2001: first version.
 */
void cloog_loop_add(CloogLoop ** start, CloogLoop ** now, CloogLoop * loop) {
    if (*start == NULL) {
        *start = loop ;
        *now = *start ;
    } else {
        (*now)->next = loop ;
        *now = (*now)->next ;
    }
}


/**
 * cloog_loop_add function:
 * This function adds a CloogLoop structure (loop) at a given place (now) of a
 * NULL terminated list of CloogLoop structures. The beginning of this list
 * is (start). This function updates (now) to the end of the loop list (loop),
 * and updates (start) if the added element is the first one -that is when
 * (start) is NULL-.
 * - September 9th 2005: first version.
 */
void cloog_loop_add_list(CloogLoop ** start, CloogLoop ** now, CloogLoop * loop) {
    if (*start == NULL) {
        *start = loop ;
        *now = *start ;
    } else {
        (*now)->next = loop ;
        *now = (*now)->next ;
    }

    while ((*now)->next != NULL)
        *now = (*now)->next ;
}


/**
 * cloog_loop_copy function:
 * This function returns a copy of the CloogLoop structure given as input. In
 * fact, there is just new allocations for the CloogLoop structures, but their
 * contents are the same.
 * - October 28th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 */
CloogLoop * cloog_loop_copy(CloogLoop * source) {
    CloogLoop * loop ;
    CloogBlock * block ;
    CloogDomain * domain ;

    loop = NULL ;
    if (source != NULL) {
        domain = cloog_domain_copy(source->domain) ;
        block  = cloog_block_copy(source->block) ;
        loop   = cloog_loop_alloc(source->state, domain, source->otl,
                                  source->stride, block, NULL,  NULL);
        loop->usr = source->usr;
        loop->inner = cloog_loop_copy(source->inner) ;
        loop->next = cloog_loop_copy(source->next) ;
    }
    return(loop) ;
}


/**
 * cloog_loop_add_disjoint function:
 * This function adds some CloogLoop structures at a given place (now) of a
 * NULL terminated list of CloogLoop structures. The beginning of this list
 * is (start). (loop) can be an union of polyhedra, this function separates the
 * union into a list of *disjoint* polyhedra then adds the list. This function
 * updates (now) to the end of the list and updates (start) if first added
 * element is the first of the principal list -that is when (start) is NULL-.
 * (loop) can be freed by this function, basically when its domain is actually
 * a union of polyhedra, but don't worry, all the useful data are now stored
 * inside the list (start). We do not use PolyLib's Domain_Disjoint function,
 * since the number of union components is often higher (thus code size too).
 * - October   28th 2001: first version.
 * - November  14th 2001: bug correction (this one was hard to find !).
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 * - October   27th 2005: (debug) included blocks were not copied for new loops.
 */
void cloog_loop_add_disjoint(start, now, loop)
CloogLoop ** start, ** now, * loop ;
{
    CloogLoop * sep, * inner ;
    CloogDomain *domain, *seen, *temp, *rest;
    CloogBlock * block ;

    if (cloog_domain_isconvex(loop->domain))
        cloog_loop_add(start,now,loop) ;
    else {
        domain = cloog_domain_simplify_union(loop->domain);
        loop->domain = NULL ;

        /* We separate the first element of the rest of the union. */
        domain = cloog_domain_cut_first(domain, &rest);

        /* This first element is the first of the list of disjoint polyhedra. */
        sep = cloog_loop_alloc(loop->state, domain, 0, NULL,
                               loop->block, loop->inner, NULL);
        cloog_loop_add(start,now,sep) ;

        seen = cloog_domain_copy(domain);
        while (!cloog_domain_isempty(domain = rest)) {
            temp = cloog_domain_cut_first(domain, &rest);
            domain = cloog_domain_difference(temp, seen);
            cloog_domain_free(temp);

            if (cloog_domain_isempty(domain)) {
                cloog_domain_free(domain);
                continue;
            }

            /* Each new loop will have its own life, for instance we can free its
             * inner loop and included block. Then each one must have its own copy
             * of both 'inner' and 'block'.
             */
            inner = cloog_loop_copy(loop->inner) ;
            block = cloog_block_copy(loop->block) ;

            sep = cloog_loop_alloc(loop->state, cloog_domain_copy(domain),
                                   0, NULL, block, inner, NULL);
            /* domain can be an union too. If so: recursion. */
            if (cloog_domain_isconvex(domain))
                cloog_loop_add(start,now,sep) ;
            else
                cloog_loop_add_disjoint(start,now,sep) ;

            if (cloog_domain_isempty(rest)) {
                cloog_domain_free(domain);
                break;
            }

            seen = cloog_domain_union(seen, domain);
        }
        cloog_domain_free(rest);
        cloog_domain_free(seen);
        cloog_loop_free_parts(loop,0,0,0,0) ;
    }
}


/**
 * cloog_loop_disjoint function:
 * This function returns a list of loops such that each loop with non-convex
 * domain in the input list (loop) is separated into several loops where the
 * domains are the components of the union of *disjoint* polyhedra equivalent
 * to the original non-convex domain. See cloog_loop_add_disjoint comments
 * for more details.
 * - September 16th 2005: first version.
 */
CloogLoop * cloog_loop_disjoint(CloogLoop * loop) {
    CloogLoop *res=NULL, * now=NULL, * next ;

    /* Because this is often the case, don't waste time ! */
    if (loop && !loop->next && cloog_domain_isconvex(loop->domain))
        return loop ;

    while (loop != NULL) {
        next = loop->next ;
        loop->next = NULL ;
        cloog_loop_add_disjoint(&res,&now,loop) ;
        loop = next ;
    }

    return res ;
}


/**
 * cloog_loop_restrict function:
 * This function returns the (loop) in the context of (context): it makes the
 * intersection between the (loop) domain and the (context), then it returns
 * a pointer to a new loop, with this intersection as domain.
 **
 * - October 27th 2001: first version.
 * - June    15th 2005: a memory leak fixed (domain was not freed when empty).
 * - June    22nd 2005: Adaptation for GMP.
 */
CloogLoop *cloog_loop_restrict(CloogLoop *loop, CloogDomain *context) {
    int new_dimension ;
    CloogDomain * domain, * extended_context, * new_domain ;
    CloogLoop * new_loop ;

    domain = loop->domain ;
    if (cloog_domain_dimension(domain) > cloog_domain_dimension(context)) {
        new_dimension = cloog_domain_dimension(domain);
        extended_context = cloog_domain_extend(context, new_dimension);
        new_domain = cloog_domain_intersection(extended_context,loop->domain) ;
        cloog_domain_free(extended_context) ;
    } else
        new_domain = cloog_domain_intersection(context,loop->domain) ;

    if (cloog_domain_isempty(new_domain)) {
        cloog_domain_free(new_domain) ;
        return(NULL) ;
    } else {
        new_loop = cloog_loop_alloc(loop->state, new_domain,
                                    0, NULL, loop->block, loop->inner, NULL);
        return(new_loop) ;
    }
}


/**
 * Call cloog_loop_restrict on each loop in the list "loop" and return
 * the concatenated result.
 */
CloogLoop *cloog_loop_restrict_all(CloogLoop *loop, CloogDomain *context) {
    CloogLoop *next;
    CloogLoop *res = NULL;
    CloogLoop **res_next = &res;

    for (; loop; loop = next) {
        next = loop->next;

        *res_next = cloog_loop_restrict(loop, context);
        if (*res_next) {
            res_next = &(*res_next)->next;
            cloog_loop_free_parts(loop, 1, 0, 0, 0);
        } else {
            loop->next = NULL;
            cloog_loop_free(loop);
        }
    }

    return res;
}


/**
 * Restrict the domains of the inner loops of each loop l in the given
 * list of loops to the domain of the loop l.  If the domains of all
 * inner loops of a given loop l turn out to be empty, then remove l
 * from the list.
 */
CloogLoop *cloog_loop_restrict_inner(CloogLoop *loop) {
    CloogLoop *next;
    CloogLoop *res;
    CloogLoop **res_next = &res;

    for (; loop; loop = next) {
        next = loop->next;

        loop->inner = cloog_loop_restrict_all(loop->inner, loop->domain);
        if (loop->inner) {
            *res_next = loop;
            res_next = &(*res_next)->next;
        } else {
            loop->next = NULL;
            cloog_loop_free(loop);
        }
    }

    *res_next = NULL;

    return res;
}

/**
 * cloog_loop_project function:
 * This function returns the projection of (loop) on the (level) first
 * dimensions (outer loops). It makes the projection of the (loop) domain,
 * then it returns a pointer to a new loop, with this projection as domain.
 **
 * - October   27th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 */
CloogLoop * cloog_loop_project(CloogLoop * loop, int level) {
    CloogDomain * new_domain ;
    CloogLoop * new_loop, * copy ;

    copy = cloog_loop_alloc(loop->state, loop->domain, loop->otl, loop->stride,
                            loop->block, loop->inner, NULL);

    if (cloog_domain_dimension(loop->domain) == level)
        new_domain = cloog_domain_copy(loop->domain) ;
    else
        new_domain = cloog_domain_project(loop->domain, level);

    new_loop = cloog_loop_alloc(loop->state, new_domain, 0, NULL,
                                NULL, copy, NULL);

    return(new_loop) ;
}


/**
 * Call cloog_loop_project on each loop in the list "loop" and return
 * the concatenated result.
 */
CloogLoop *cloog_loop_project_all(CloogLoop *loop, int level) {
    CloogLoop *next;
    CloogLoop *res = NULL;
    CloogLoop **res_next = &res;

    for (; loop; loop = next) {
        next = loop->next;

        *res_next = cloog_loop_project(loop, level);
        res_next = &(*res_next)->next;
        cloog_loop_free_parts(loop, 0, 0, 0, 0);
    }

    return res;
}


/**
 * cloog_loop_concat function:
 * This function returns a pointer to the concatenation of the
 * CloogLoop lists given as input.
 * - October 28th 2001: first version.
 */
CloogLoop * cloog_loop_concat(CloogLoop * a, CloogLoop * b) {
    CloogLoop * loop, * temp ;

    loop = a  ;
    temp = loop ;
    if (loop != NULL) {
        while (temp->next != NULL)
            temp = temp->next ;
        temp->next = b ;
    } else
        loop = b ;

    return(loop) ;
}


/**
 * cloog_loop_combine:
 * Combine consecutive loops with identical domains into
 * a single loop with the concatenation of their inner loops
 * as inner loop.
 */
CloogLoop *cloog_loop_combine(CloogLoop *loop) {
    CloogLoop *first, *second;

    for (first = loop; first; first = first->next) {
        while (first->next) {
            if (!cloog_domain_lazy_equal(first->domain, first->next->domain))
                break;
            second = first->next;
            first->inner = cloog_loop_concat(first->inner, second->inner);
            first->next = second->next;
            cloog_loop_free_parts(second, 1, 0, 0, 0);
        }
    }

    return loop;
}

/**
 * Remove loops from list that have an empty domain.
 */
CloogLoop *cloog_loop_remove_empty_domain_loops(CloogLoop *loop) {
    CloogLoop *l, *res, *next, **res_next;

    res = NULL;
    res_next = &res;
    for (l = loop; l; l = next) {
        next = l->next;
        if (cloog_domain_isempty(l->domain))
            cloog_loop_free_parts(l, 1, 1, 1, 0);
        else {
            *res_next = l;
            res_next = &(*res_next)->next;
        }
    }
    *res_next = NULL;

    return res;
}

CloogLoop *cloog_loop_decompose_inner(CloogLoop *loop,
                                      int level, int scalar, int *scaldims, int nb_scattdims);

/* For each loop with only one inner loop, replace the domain
 * of the loop with the projection of the domain of the inner
 * loop.  To increase the number of loops with a single inner
 * we first decompose the inner loops into strongly connected
 * components.
 */
CloogLoop *cloog_loop_specialize(CloogLoop *loop,
                                 int level, int scalar, int *scaldims, int nb_scattdims) {
    int dim;
    CloogDomain *domain;
    CloogLoop *l;

    loop = cloog_loop_decompose_inner(loop, level, scalar,
                                      scaldims, nb_scattdims);

    for (l = loop; l; l = l->next) {
        if (l->inner->next)
            continue;
        if (!cloog_domain_isconvex(l->inner->domain))
            continue;

        dim = cloog_domain_dimension(l->domain);
        domain = cloog_domain_project(l->inner->domain, dim);
        if (cloog_domain_isconvex(domain)) {
            cloog_domain_free(l->domain);
            l->domain = domain;
        } else {
            cloog_domain_free(domain);
        }
    }

    return cloog_loop_remove_empty_domain_loops(loop);
}

/* For each loop with only one inner loop, propagate the bounds from
 * the inner loop domain to the outer loop domain.  This is especially
 * useful if the inner loop domain has a non-trivial stride which
 * results in an update of the lower bound.
 */
CloogLoop *cloog_loop_propagate_lower_bound(CloogLoop *loop, int level) {
    int dim;
    CloogDomain *domain, *t;
    CloogLoop *l;

    for (l = loop; l; l = l->next) {
        if (l->inner->next)
            continue;
        if (!cloog_domain_isconvex(l->inner->domain))
            continue;

        dim = cloog_domain_dimension(l->domain);
        domain = cloog_domain_project(l->inner->domain, dim);
        if (cloog_domain_isconvex(domain)) {
            t = cloog_domain_intersection(domain, l->domain);
            cloog_domain_free(l->domain);
            l->domain = t;
        }
        cloog_domain_free(domain);
    }

    return loop;
}

/**
 * cloog_loop_separate function:
 * This function implements the Quillere algorithm for separation of multiple
 * loops: for a given set of polyhedra (loop), it computes a set of disjoint
 * polyhedra such that the unions of these sets are equal, and returns this set.
 * - October   28th 2001: first version.
 * - November  14th 2001: elimination of some unused blocks.
 * - August    13th 2002: (debug) in the case of union of polyhedra for one
 *                        loop, redundant constraints are fired.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 * - October   16th 2005: Removal of the non-shared constraint elimination when
 *                        there is only one loop in the list (seems to work
 *                        without now, DomainSimplify may have been improved).
 *                        The problem was visible with test/iftest2.cloog.
 */
CloogLoop * cloog_loop_separate(CloogLoop * loop) {
    int lazy_equal=0, disjoint = 0;
    CloogLoop * new_loop, * new_inner, * res, * now, * temp, * Q,
              * inner, * old /*, * previous, * next*/  ;
    CloogDomain *UQ, *domain;

    if (loop == NULL)
        return NULL ;

    loop = cloog_loop_combine(loop);

    if (loop->next == NULL)
        return cloog_loop_disjoint(loop) ;

    UQ     = cloog_domain_copy(loop->domain) ;
    domain = cloog_domain_copy(loop->domain) ;
    res    = cloog_loop_alloc(loop->state, domain, 0, NULL,
                              loop->block, loop->inner, NULL);

    old = loop ;
    while((loop = loop->next) != NULL) {
        temp = NULL ;

        /* For all Q, add Q-loop associated with the blocks of Q alone,
         * and Q inter loop associated with the blocks of Q and loop.
         */
        for (Q = res; Q; Q = Q->next) {
            /* Add (Q inter loop). */
            if ((disjoint = cloog_domain_lazy_disjoint(Q->domain,loop->domain)))
                domain = NULL ;
            else {
                if ((lazy_equal = cloog_domain_lazy_equal(Q->domain,loop->domain)))
                    domain = cloog_domain_copy(Q->domain) ;
                else
                    domain = cloog_domain_intersection(Q->domain,loop->domain) ;

                if (!cloog_domain_isempty(domain)) {
                    new_inner = cloog_loop_concat(cloog_loop_copy(Q->inner),
                                                  cloog_loop_copy(loop->inner)) ;
                    new_loop = cloog_loop_alloc(loop->state, domain, 0, NULL,
                                                NULL, new_inner, NULL);
                    cloog_loop_add_disjoint(&temp,&now,new_loop) ;
                } else {
                    disjoint = 1;
                    cloog_domain_free(domain);
                }
            }

            /* Add (Q - loop). */
            if (disjoint)
                domain = cloog_domain_copy(Q->domain) ;
            else {
                if (lazy_equal)
                    domain = cloog_domain_empty(Q->domain);
                else
                    domain = cloog_domain_difference(Q->domain,loop->domain) ;
            }

            if (!cloog_domain_isempty(domain)) {
                new_loop = cloog_loop_alloc(loop->state, domain, 0, NULL,
                                            NULL, Q->inner, NULL);
                cloog_loop_add_disjoint(&temp,&now,new_loop) ;
            } else {
                cloog_domain_free(domain) ;
                /* If Q->inner is no more useful, we can free it. */
                inner = Q->inner ;
                Q->inner = NULL ;
                cloog_loop_free(inner) ;
            }
        }

        /* Add loop-UQ associated with the blocks of loop alone.*/
        if (cloog_domain_lazy_disjoint(loop->domain,UQ))
            domain = cloog_domain_copy(loop->domain) ;
        else {
            if (cloog_domain_lazy_equal(loop->domain,UQ))
                domain = cloog_domain_empty(UQ);
            else
                domain = cloog_domain_difference(loop->domain,UQ) ;
        }

        if (!cloog_domain_isempty(domain)) {
            new_loop = cloog_loop_alloc(loop->state, domain, 0, NULL,
                                        NULL, loop->inner, NULL);
            cloog_loop_add_disjoint(&temp,&now,new_loop) ;
        } else {
            cloog_domain_free(domain) ;
            /* If loop->inner is no more useful, we can free it. */
            cloog_loop_free(loop->inner) ;
        }

        loop->inner = NULL ;

        if (loop->next != NULL)
            UQ = cloog_domain_union(UQ, cloog_domain_copy(loop->domain));
        else
            cloog_domain_free(UQ);

        cloog_loop_free_parts(res,1,0,0,1) ;

        res = temp ;
    }
    cloog_loop_free_parts(old,1,0,0,1) ;

    return(res) ;
}


static CloogDomain *bounding_domain(CloogDomain *dom, CloogOptions *options) {
    if (options->sh)
        return cloog_domain_simple_convex(dom);
    else
        return cloog_domain_convex(dom);
}


/**
 * cloog_loop_merge function:
 * This function is the 'soft' version of loop_separate if we are looking for
 * a code much simpler (and less efficicient).  This function returns the new
 * CloogLoop list.
 * - October 29th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 */
CloogLoop *cloog_loop_merge(CloogLoop *loop, int level, CloogOptions *options) {
    CloogLoop *res, *new_inner, *old;
    CloogDomain *new_domain, *temp;

    if (loop == NULL)
        return loop;

    if (loop->next == NULL && cloog_domain_isconvex(loop->domain))
        return loop;

    old = loop;
    temp = loop->domain;
    loop->domain = NULL;
    new_inner = loop->inner;

    for (loop = loop->next; loop; loop = loop->next) {
        temp = cloog_domain_union(temp, loop->domain);
        loop->domain = NULL;
        new_inner = cloog_loop_concat(new_inner, loop->inner);
    }

    new_domain = bounding_domain(temp, options);

    if (level > 0 && !cloog_domain_is_bounded(new_domain, level) &&
            cloog_domain_is_bounded(temp, level)) {
        CloogDomain *splitter, *t2;

        cloog_domain_free(new_domain);
        splitter = cloog_domain_bound_splitter(temp, level);

        res = NULL;
        while (!cloog_domain_isconvex(splitter)) {
            CloogDomain *first, *rest;
            first = cloog_domain_cut_first(splitter, &rest);
            splitter = rest;
            t2 = cloog_domain_intersection(first, temp);
            cloog_domain_free(first);

            new_domain = bounding_domain(t2, options);
            cloog_domain_free(t2);

            if (cloog_domain_isempty(new_domain)) {
                cloog_domain_free(new_domain);
                continue;
            }
            res = cloog_loop_alloc(old->state, new_domain, 0, NULL,
                                   NULL, cloog_loop_copy(new_inner), res);
        }

        t2 = cloog_domain_intersection(splitter, temp);
        cloog_domain_free(splitter);

        new_domain = bounding_domain(t2, options);
        cloog_domain_free(t2);

        if (cloog_domain_isempty(new_domain)) {
            cloog_domain_free(new_domain);
            cloog_loop_free(new_inner);
        } else
            res = cloog_loop_alloc(old->state, new_domain, 0, NULL,
                                   NULL, new_inner, res);
    } else {
        res = cloog_loop_alloc(old->state, new_domain, 0, NULL,
                               NULL, new_inner, NULL);
    }
    cloog_domain_free(temp);

    cloog_loop_free_parts(old, 0, 0, 0, 1);

    return res;
}


static int cloog_loop_count(CloogLoop *loop) {
    int nb_loops;

    for (nb_loops = 0; loop; loop = loop->next)
        nb_loops++;

    return  nb_loops;
}


/**
 * cloog_loop_sort function:
 * Adaptation from LoopGen 0.4 by F. Quillere. This function sorts a list of
 * parameterized disjoint polyhedra, in order to not have lexicographic order
 * violation (see Quillere paper).
 * - September 16th 2005: inclusion of cloog_loop_number (October 29th 2001).
 */
CloogLoop *cloog_loop_sort(CloogLoop *loop, int level) {
    CloogLoop *res, *now, **loop_array;
    CloogDomain **doms;
    int i, nb_loops=0, * permut ;

    /* There is no need to sort the parameter domains. */
    if (!level)
        return loop;

    /* We will need to know how many loops are in the list. */
    nb_loops = cloog_loop_count(loop);

    /* If there is only one loop, it's the end. */
    if (nb_loops == 1)
        return(loop) ;

    /* We have to allocate memory for some useful components:
     * - loop_array: the loop array,
     * - doms: the array of domains to sort,
     * - permut: will give us a possible sort (maybe not the only one).
     */
    loop_array = (CloogLoop **)malloc(nb_loops*sizeof(CloogLoop *)) ;
    doms = (CloogDomain **)malloc(nb_loops*sizeof(CloogDomain *));
    permut = (int *)malloc(nb_loops*sizeof(int)) ;

    /* We fill up the loop and domain arrays. */
    for (i=0; i<nb_loops; i++,loop=loop->next) {
        loop_array[i] = loop ;
        doms[i] = loop_array[i]->domain;
    }

    /* cloog_domain_sort will fill up permut. */
    cloog_domain_sort(doms, nb_loops, level, permut);

    /* With permut and loop_array we build the sorted list. */
    res = NULL ;
    for (i=0; i<nb_loops; i++) {
        /* To avoid pointer looping... loop_add will rebuild the list. */
        loop_array[permut[i]-1]->next = NULL ;
        cloog_loop_add(&res,&now,loop_array[permut[i]-1]) ;
    }

    free(permut) ;
    free(doms);
    free(loop_array) ;

    return res;
}


/**
 * cloog_loop_nest function:
 * This function changes the loop list in such a way that we have no more than
 * one dimension added by level. It returns an equivalent loop list with
 * this property.
 * - October 29th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 * - November  21th 2005: (debug) now OK when cloog_loop_restrict returns NULL.
 */
CloogLoop *cloog_loop_nest(CloogLoop *loop, CloogDomain *context, int level) {
    int l ;
    CloogLoop * p, * temp, * res, * now, * next ;
    CloogDomain * new_domain ;

    loop = cloog_loop_disjoint(loop);

    res = NULL ;
    /* Each domain is changed by its intersection with the context. */
    while (loop != NULL) {
        p = cloog_loop_restrict(loop, context);
        next = loop->next ;

        if (p != NULL) {
            cloog_loop_free_parts(loop,1,0,0,0) ;

            temp = cloog_loop_alloc(p->state, p->domain, 0, NULL,
                                    p->block, p->inner, NULL);

            /* If the intersection dimension is too big, we make projections smaller
             * and smaller, and each projection includes the preceding projection
             * (thus, in the target list, dimensions are added one by one).
             */
            if (cloog_domain_dimension(p->domain) >= level)
                for (l = cloog_domain_dimension(p->domain); l >= level; l--) {
                    new_domain = cloog_domain_project(p->domain, l);
                    temp = cloog_loop_alloc(p->state, new_domain, 0, NULL,
                                            NULL, temp, NULL);
                }

            /* p is no more useful (but its content yes !). */
            cloog_loop_free_parts(p,0,0,0,0) ;

            cloog_loop_add(&res,&now,temp) ;
        } else
            cloog_loop_free_parts(loop,1,1,1,0) ;

        loop = next ;
    }

    return(res) ;
}


/* Check if the domains of the inner loops impose a stride constraint
 * on the given level.
 * The core of the search is implemented in cloog_domain_list_stride.
 * Here, we simply construct a list of domains to pass to this function
 * and if a stride is found, we adjust the lower bounds by calling
 * cloog_domain_stride_lower_bound.
 */
static int cloog_loop_variable_offset_stride(CloogLoop *loop, int level) {
    CloogDomainList *list = NULL;
    CloogLoop *inner;
    CloogStride *stride;

    for (inner = loop->inner; inner; inner = inner->next) {
        CloogDomainList *entry = ALLOC(CloogDomainList);
        entry->domain = cloog_domain_copy(inner->domain);
        entry->next = list;
        list = entry;
    }

    stride = cloog_domain_list_stride(list, level);

    cloog_domain_list_free(list);

    if (!stride)
        return 0;

    loop->stride = stride;
    loop->domain = cloog_domain_stride_lower_bound(loop->domain, level, stride);

    return 1;
}


/**
 * cloog_loop_stride function:
 * This function will find the stride of a loop for the iterator at the column
 * number 'level' in the constraint matrix. It will update the lower bound of
 * the iterator accordingly. Basically, the function will try to find in the
 * inner loops a common condition on this iterator for the inner loop iterators
 * to be integral. For instance, let us consider a loop with the iterator i,
 * the iteration domain -4<=i<=n, and its two inner loops with the iterator j.
 * The first inner loop has the constraint 3j=i, and the second one has the
 * constraint 6j=i. Then the common constraint on i for j to be integral is
 * i%3=0, the stride for i is 3. Lastly, we have to find the new lower bound
 * for i: the first value satisfying the common constraint: -3. At the end, the
 * iteration domain for i is -3<=i<=n and the stride for i is 3.
 *
 * The algorithm implemented in this function only allows for strides
 * on loops with a lower bound that has a constant remainder on division
 * by the stride.  Before initiating this procedure, we first check
 * if we can find a stride with a lower bound with a variable offset in
 * cloog_loop_variable_offset_stride.
 *
 * - loop is the loop including the iteration domain of the considered iterator,
 * - level is the column number of the iterator in the matrix of contraints.
 **
 * - June 29th 2003: first version (work in progress since June 26th 2003).
 * - July 14th 2003: simpler version.
 * - June 22nd 2005: Adaptation for GMP (from S. Verdoolaege's 0.12.1 version).
 */
void cloog_loop_stride(CloogLoop * loop, int level) {
    int first_search ;
    cloog_int_t stride, ref_offset, offset, potential;
    CloogLoop * inner ;

    if (!cloog_domain_can_stride(loop->domain, level))
        return;

    if (cloog_loop_variable_offset_stride(loop, level))
        return;

    cloog_int_init(stride);
    cloog_int_init(ref_offset);
    cloog_int_init(offset);
    cloog_int_init(potential);

    cloog_int_set_si(ref_offset, 0);
    cloog_int_set_si(offset, 0);

    /* Default stride. */
    cloog_int_set_si(stride, 1);
    first_search = 1 ;
    inner = loop->inner ;

    while (inner != NULL) {
        /* If the minimun stride has not been found yet, find the stride. */
        if ((first_search) || (!cloog_int_is_one(stride))) {
            cloog_domain_stride(inner->domain, level, &potential, &offset);
            if (!cloog_int_is_one(potential) && (!first_search)) {
                /* Offsets must be the same for common stride. */
                cloog_int_gcd(stride, potential, stride);
                if (!cloog_int_is_zero(stride)) {
                    cloog_int_fdiv_r(offset, offset, stride);
                    cloog_int_fdiv_r(ref_offset, ref_offset, stride);
                }
                if (cloog_int_ne(offset,ref_offset))
                    cloog_int_set_si(stride, 1);
            } else {
                cloog_int_set(stride, potential);
                cloog_int_set(ref_offset, offset);
            }

            first_search = 0 ;
        }

        inner = inner->next ;
    }

    if (cloog_int_is_zero(stride))
        cloog_int_set_si(stride, 1);

    /* Update the values if necessary. */
    if (!cloog_int_is_one(stride)) {
        /* Update the stride value. */
        if (!cloog_int_is_zero(offset))
            cloog_int_sub(offset, stride, offset);
        loop->stride = cloog_stride_alloc(stride, offset);
        loop->domain = cloog_domain_stride_lower_bound(loop->domain, level,
                       loop->stride);
    }

    cloog_int_clear(stride);
    cloog_int_clear(ref_offset);
    cloog_int_clear(offset);
    cloog_int_clear(potential);
}


void cloog_loop_otl(CloogLoop *loop, int level) {
    if (cloog_domain_is_otl(loop->domain, level))
        loop->otl = 1;
}


/**
 * cloog_loop_stop function:
 * This function implements the 'stop' option : each domain of each loop
 * in the list 'loop' is replaced by 'context'. 'context' should be the
 * domain of the outer loop. By using this method, there are no more dimensions
 * to scan and the simplification step will automaticaly remove the domains
 * since they are the same as the corresponding contexts. The effect of this
 * function is to stop the code generation at the level this function is called,
 * the resulting code do not consider the next dimensions.
 * - January 11th 2005: first version.
 */
CloogLoop * cloog_loop_stop(CloogLoop * loop, CloogDomain * context) {
    if (loop == NULL)
        return NULL ;
    else {
        cloog_domain_free(loop->domain) ;
        loop->domain = cloog_domain_copy(context) ;
        loop->next = cloog_loop_stop(loop->next, context) ;
    }

    return loop ;
}


static int level_is_constant(int level, int scalar, int *scaldims, int nb_scattdims) {
    return level && (level+scalar <= nb_scattdims) && (scaldims[level+scalar-1]);
}


/**
 * Compare the constant dimensions of loops 'l1' and 'l2' starting at 'scalar'
 * and return -1 if the vector of constant dimensions of 'l1' is smaller
 * than that of 'l2', 0 if they are the same and +1 if that of 'l1' is
 * greater than that of 'l2'.
 * This function should be called on the innermost loop (the loop
 * containing a block).
 * \param l1 Loop to be compared with l2.
 * \param l2 Loop to be compared with l1.
 * \param level Current non-scalar dimension.
 * \param scaldims Boolean array saying whether a dimension is scalar or not.
 * \param nb_scattdims Size of the scaldims array.
 * \param scalar Current scalar dimension.
 * \return -1 if (l1 < l2), 0 if (l1 == l2) and +1 if (l1 > l2)
 */
int cloog_loop_constant_cmp(CloogLoop *l1, CloogLoop *l2, int level,
                            int *scaldims, int nb_scattdims, int scalar) {
    CloogBlock *b1, *b2;
    b1 = l1->block;
    b2 = l2->block;
    while (level_is_constant(level, scalar, scaldims, nb_scattdims)) {
        int cmp = cloog_int_cmp(b1->scaldims[scalar], b2->scaldims[scalar]);
        if (cmp)
            return cmp;
        scalar++;
    }
    return 0;
}


/**
 * cloog_loop_scalar_gt function:
 * This function returns 1 if loop 'l1' is greater than loop 'l2' for the
 * scalar dimension vector that begins at dimension 'scalar', 0 otherwise. What
 * we want to know is whether a loop is scheduled before another one or not.
 * This function solves the problem when the considered dimension for scheduling
 * is a scalar dimension. Since there may be a succession of scalar dimensions,
 * this function will reason about the vector of scalar dimension that begins
 * at dimension 'level+scalar' and finish to the first non-scalar dimension.
 * \param l1 Loop to be compared with l2.
 * \param l2 Loop to be compared with l1.
 * \param level Current non-scalar dimension.
 * \param scaldims Boolean array saying whether a dimension is scalar or not.
 * \param nb_scattdims Size of the scaldims array.
 * \param scalar Current scalar dimension.
 * \return 1 if (l1 > l2), 0 otherwise.
 **
 * - September 9th 2005: first version.
 * - October  15nd 2007: now "greater than" instead of "greater or equal".
 */
int cloog_loop_scalar_gt(l1, l2, level, scaldims, nb_scattdims, scalar)
CloogLoop * l1, * l2 ;
int level, * scaldims, nb_scattdims, scalar ;
{
    return cloog_loop_constant_cmp(l1, l2, level, scaldims, nb_scattdims, scalar) > 0;
}


/**
 * cloog_loop_scalar_eq function:
 * This function returns 1 if loop 'l1' is equal to loop 'l2' for the scalar
 * dimension vector that begins at dimension 'scalar', 0 otherwise. What we want
 * to know is whether two loops are scheduled for the same time or not.
 * This function solves the problem when the considered dimension for scheduling
 * is a scalar dimension. Since there may be a succession of scalar dimensions,
 * this function will reason about the vector of scalar dimension that begins
 * at dimension 'level+scalar' and finish to the first non-scalar dimension.
 * - l1 and l2 are the loops to compare,
 * - level is the current non-scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - scalar is the current scalar dimension.
 **
 * - September 9th 2005 : first version.
 */
int cloog_loop_scalar_eq(l1, l2, level, scaldims, nb_scattdims, scalar)
CloogLoop * l1, * l2 ;
int level, * scaldims, nb_scattdims, scalar ;
{
    return cloog_loop_constant_cmp(l1, l2, level, scaldims, nb_scattdims, scalar) == 0;
}


/**
 * cloog_loop_scalar_sort function:
 * This function sorts a linked list of loops (loop) with respect to the
 * scalar dimension vector that begins at dimension 'scalar'. Since there may
 * be a succession of scalar dimensions, this function will reason about the
 * vector of scalar dimension that begins at dimension 'level+scalar' and
 * finish to the first non-scalar dimension.
 * \param loop Loop list to sort.
 * \param level Current non-scalar dimension.
 * \param scaldims Boolean array saying whether a dimension is scalar or not.
 * \param nb_scattdims Size of the scaldims array.
 * \param scalar Current scalar dimension.
 * \return A pointer to the sorted list.
 **
 * - July      2nd 2005: first developments.
 * - September 2nd 2005: first version.
 * - October  15nd 2007: complete rewrite to remove bugs, now a bubble sort.
 */
CloogLoop * cloog_loop_scalar_sort(loop, level, scaldims, nb_scattdims, scalar)
CloogLoop * loop ;
int level, * scaldims, nb_scattdims, scalar ;
{
    int ok ;
    CloogLoop **current;

    do {
        ok = 1;
        for (current = &loop; (*current)->next; current = &(*current)->next) {
            CloogLoop *next = (*current)->next;
            if (cloog_loop_scalar_gt(*current,next,level,scaldims,nb_scattdims,scalar)) {
                ok = 0;
                (*current)->next = next->next;
                next->next = *current;
                *current = next;
            }
        }
    } while (!ok);

    return loop ;
}


/**
 * cloog_loop_generate_backtrack function:
 * adaptation from LoopGen 0.4 by F. Quillere. This function implements the
 * backtrack of the Quillere et al. algorithm (see the Quillere paper).
 * It eliminates unused iterations of the current level for the new one. See the
 * example called linearity-1-1 example with and without this part for an idea.
 * - October 26th 2001: first version in cloog_loop_generate_general.
 * - July    31th 2002: (debug) no more parasite loops (REALLY hard !).
 * - October 30th 2005: extraction from cloog_loop_generate_general.
 */
CloogLoop *cloog_loop_generate_backtrack(CloogLoop *loop,
        int level, CloogOptions *options) {
    CloogDomain * domain ;
    CloogLoop * now, * now2, * next, * next2, * end, * temp, * l, * inner,
              * new_loop ;

    temp = loop ;
    loop = NULL ;

    while (temp != NULL) {
        l = NULL ;
        inner = temp->inner ;

        while (inner != NULL) {
            next = inner->next ;
            /* This 'if' and its first part is the debug of july 31th 2002. */
            if (inner->block != NULL) {
                end = cloog_loop_alloc(temp->state, inner->domain, 0, NULL,
                                       inner->block, NULL, NULL);
                domain = cloog_domain_copy(temp->domain) ;
                new_loop = cloog_loop_alloc(temp->state, domain, 0, NULL,
                                            NULL, end, NULL);
            } else
                new_loop = cloog_loop_project(inner, level);

            cloog_loop_free_parts(inner,0,0,0,0) ;
            cloog_loop_add(&l,&now2,new_loop) ;
            inner = next ;
        }

        temp->inner = NULL ;

        if (l != NULL) {
            l = cloog_loop_separate(l) ;
            l = cloog_loop_sort(l, level);
            while (l != NULL) {
                l->stride = cloog_stride_copy(l->stride);
                cloog_loop_add(&loop,&now,l) ;
                l = l->next ;
            }
        }
        next2 = temp->next ;
        cloog_loop_free_parts(temp,1,0,0,0) ;
        temp = next2 ;
    }

    return loop ;
}


/**
 * Return 1 if we need to continue recursing to the specified level.
 */
int cloog_loop_more(CloogLoop *loop, int level, int scalar, int nb_scattdims) {
    return level + scalar <= nb_scattdims ||
           cloog_domain_dimension(loop->domain) >= level;
}

/**
 * Return 1 if the domains of all loops in the given linked list
 * have a fixed value at the given level.
 * In principle, there would be no need to check that the fixed value is
 * the same for each of these loops because this function is only
 * called on a component.  However, not all backends perform a proper
 * decomposition into components.
 */
int cloog_loop_is_constant(CloogLoop *loop, int level) {
    cloog_int_t c1, c2;
    int r = 1;

    cloog_int_init(c1);
    cloog_int_init(c2);

    if (!cloog_domain_lazy_isconstant(loop->domain, level - 1, &c1))
        r = 0;

    for (loop = loop->next; r && loop; loop = loop->next) {
        if (!cloog_domain_lazy_isconstant(loop->domain, level - 1, &c2))
            r = 0;
        else if (cloog_int_ne(c1, c2))
            r = 0;
    }

    cloog_int_clear(c1);
    cloog_int_clear(c2);

    return r;
}

/**
 * Assuming all domains in the given linked list of loop
 * have a fixed values at level, return a single loop with
 * a domain corresponding to this fixed value and with as
 * list of inner loops the concatenation of all inner loops
 * in the original list.
 */
CloogLoop *cloog_loop_constant(CloogLoop *loop, int level) {
    CloogLoop *res, *inner, *tmp;
    CloogDomain *domain, *t;

    if (!loop)
        return loop;

    inner = loop->inner;
    domain = loop->domain;
    for (tmp = loop->next; tmp; tmp = tmp->next) {
        inner = cloog_loop_concat(inner, tmp->inner);
        domain = cloog_domain_union(domain, tmp->domain);
    }

    domain = cloog_domain_simple_convex(t = domain);
    cloog_domain_free(t);

    res = cloog_loop_alloc(loop->state, domain, 0, NULL, NULL, inner, NULL);

    cloog_loop_free_parts(loop, 0, 0, 0, 1);

    return res;
}


/* Unroll the given loop at the given level, provided it is allowed
 * by cloog_domain_can_unroll.
 * If so, we return a list of loops, one for each iteration of the original
 * loop.  Otherwise, we simply return the original loop.
 */
static CloogLoop *loop_unroll(CloogLoop *loop, int level) {
    int can_unroll;
    cloog_int_t i;
    cloog_int_t n;
    CloogConstraint *lb;
    CloogLoop *res = NULL;
    CloogLoop **next_res = &res;
    CloogDomain *domain;
    CloogLoop *inner;

    cloog_int_init(n);
    can_unroll = cloog_domain_can_unroll(loop->domain, level, &n, &lb);
    if (!can_unroll) {
        cloog_int_clear(n);
        return loop;
    }

    cloog_int_init(i);

    for (cloog_int_set_si(i, 0); cloog_int_lt(i, n); cloog_int_add_ui(i, i, 1)) {
        domain = cloog_domain_copy(loop->domain);
        domain = cloog_domain_fixed_offset(domain, level, lb, i);
        inner = cloog_loop_copy(loop->inner);
        inner = cloog_loop_restrict_all(inner, domain);
        if (!inner) {
            cloog_domain_free(domain);
            continue;
        }
        *next_res = cloog_loop_alloc(loop->state, domain, 1, NULL, NULL,
                                     inner, NULL);
        next_res = &(*next_res)->next;
    }

    cloog_int_clear(i);
    cloog_int_clear(n);
    cloog_constraint_release(lb);

    cloog_loop_free(loop);

    return res;
}


/* Unroll all loops in the given list at the given level, provided
 * they can be unrolled.
 */
CloogLoop *cloog_loop_unroll(CloogLoop *loop, int level) {
    CloogLoop *now, *next;
    CloogLoop *res = NULL;
    CloogLoop **next_res = &res;

    for (now = loop; now; now = next) {
        next = now->next;
        now->next = NULL;

        *next_res = loop_unroll(now, level);

        while (*next_res)
            next_res = &(*next_res)->next;
    }

    return res;
}

CloogLoop *cloog_loop_generate_restricted_or_stop(CloogLoop *loop,
        CloogDomain *context,
        int level, int scalar, int *scaldims, int nb_scattdims,
        CloogOptions *options);

CloogLoop *cloog_loop_recurse(CloogLoop *loop,
                              int level, int scalar, int *scaldims, int nb_scattdims,
                              int constant, CloogOptions *options);


/**
 * Recurse on the inner loops of the given single loop.
 *
 * - loop is the loop for which we have to generate scanning code,
 * - level is the current non-scalar dimension,
 * - scalar is the current scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - constant is true if the loop is known to be executed at most once
 * - options are the general code generation options.
 */
static CloogLoop *loop_recurse(CloogLoop *loop,
                               int level, int scalar, int *scaldims, int nb_scattdims,
                               int constant, CloogOptions *options) {
    CloogLoop *inner, *into, *end, *next, *l, *now;
    CloogDomain *domain;

    if (level && options->strides && !constant)
        cloog_loop_stride(loop, level);

    if (!constant &&
            options->first_unroll >= 0 && level + scalar >= options->first_unroll) {
        loop = cloog_loop_unroll(loop, level);
        if (loop->next)
            return cloog_loop_recurse(loop, level, scalar, scaldims,
                                      nb_scattdims, 1, options);
    }

    if (level && options->otl)
        cloog_loop_otl(loop, level);
    inner = loop->inner;
    domain = cloog_domain_copy(loop->domain);
    domain = cloog_domain_add_stride_constraint(domain, loop->stride);
    into = NULL ;
    while (inner != NULL) {
        /* 4b. -ced- recurse for each sub-list of non terminal loops. */
        if (cloog_loop_more(inner, level + 1, scalar, nb_scattdims)) {
            end = inner;
            while ((end->next != NULL) &&
                    cloog_loop_more(end->next, level + 1, scalar, nb_scattdims))
                end = end->next ;

            next = end->next ;
            end->next = NULL ;

            l = cloog_loop_generate_restricted_or_stop(inner, domain,
                    level + 1, scalar, scaldims, nb_scattdims, options);

            if (l != NULL)
                cloog_loop_add_list(&into,&now,l) ;

            inner = next ;
        } else {
            cloog_loop_add(&into,&now,inner) ;
            inner = inner->next ;
        }
    }

    cloog_domain_free(domain);
    loop->inner = into;
    return loop;
}


/**
 * Recurse on the inner loops of each of the loops in the loop list.
 *
 * - loop is the loop list for which we have to generate scanning code,
 * - level is the current non-scalar dimension,
 * - scalar is the current scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - constant is true if the loop is known to be executed at most once
 * - options are the general code generation options.
 */
CloogLoop *cloog_loop_recurse(CloogLoop *loop,
                              int level, int scalar, int *scaldims, int nb_scattdims,
                              int constant, CloogOptions *options) {
    CloogLoop *now, *next;
    CloogLoop *res = NULL;
    CloogLoop **next_res = &res;

    for (now = loop; now; now = next) {
        next = now->next;
        now->next = NULL;

        *next_res = loop_recurse(now, level, scalar, scaldims, nb_scattdims,
                                 constant, options);

        while (*next_res)
            next_res = &(*next_res)->next;
    }

    return res;
}


/* Get the max across all 'first' depths for statements in this
 * stmt list, and the max across all 'last' depths */
void cloog_statement_get_fl(CloogStatement *s, int *f, int *l,
                            CloogOptions *options) {
    if (s == NULL) return;

    int fs, ls;

    if (options->fs != NULL && options->ls != NULL) {
        fs = options->fs[s->number-1];
        ls = options->ls[s->number-1];
        *f = (fs > *f)? fs: *f;
        *l = (ls > *l)? ls: *l;
    } else {
        *f = -1;
        *l = -1;
    }

    cloog_statement_get_fl(s->next, f, l, options);
}

/* Get the max across all 'first' depths for statements under
 * this loop, and the max across all 'last' depths */
void cloog_loop_get_fl(CloogLoop *loop, int *f, int *l,
                       CloogOptions *options) {
    if (loop == NULL)   return;

    CloogBlock *block = loop->block;

    if (block != NULL && block->statement != NULL) {
        cloog_statement_get_fl(block->statement, f, l, options);
    }

    cloog_loop_get_fl(loop->inner, f, l, options);
    cloog_loop_get_fl(loop->next, f, l, options);
}

/**
 * cloog_loop_generate_general function:
 * Adaptation from LoopGen 0.4 by F. Quillere. This function implements the
 * Quillere algorithm for polyhedron scanning from step 3 to 5.
 * (see the Quillere paper).
 * - loop is the loop for which we have to generate a scanning code,
 * - level is the current non-scalar dimension,
 * - scalar is the current scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - options are the general code generation options.
 **
 * - October 26th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      22nd 2005: Adaptation for GMP.
 * - September  2nd 2005: The function have been cutted out in two pieces:
 *                        cloog_loop_generate and this one, in order to handle
 *                        the scalar dimension case more efficiently with
 *                        cloog_loop_generate_scalar.
 * - November  15th 2005: (debug) the result of the cloog_loop_generate call may
 *                        be a list of polyhedra (especially if stop option is
 *                        used): cloog_loop_add_list instead of cloog_loop_add.
 * - May 31, 2012: statement-wise first and last depth for loop separation
 *   if options->fs and option->ls arrays are set, it will override what has
 *   been supplied via "-f/-l"
 *
 */
CloogLoop *cloog_loop_generate_general(CloogLoop *loop,
                                       int level, int scalar, int *scaldims, int nb_scattdims,
                                       CloogOptions *options) {
    CloogLoop *res, *now, *temp, *l, *new_loop, *next;
    int separate = 0;
    int constant = 0;

    int first = -1;
    int last = -1;

    now = NULL;

    /* Get the -f and -l for each statement */
    cloog_loop_get_fl(loop, &first, &last, options);

    /* If stmt-wise options are not set or set inconsistently, use -f/-l ones globally */
    if (first <= 0 || last < first) {
        first = options->f;
        last = options->l;
    }

    /* 3. Separate all projections into disjoint polyhedra. */
    if (level > 0 && cloog_loop_is_constant(loop, level)) {
        res = cloog_loop_constant(loop, level);
        constant = 1;
    } else if ((first > level+scalar) || (first < 0)) {
        res = cloog_loop_merge(loop, level, options);
    } else {
        res = cloog_loop_separate(loop);
        separate = 1;
    }

    /* 3b. -correction- sort the loops to determine their textual order. */
    res = cloog_loop_sort(res, level);

    res = cloog_loop_restrict_inner(res);

    if (separate)
        res = cloog_loop_specialize(res, level, scalar, scaldims, nb_scattdims);

    /* 4. Recurse for each loop with the current domain as context. */
    temp = res ;
    res = NULL ;
    if (!level || (level+scalar < last) || (last < 0))
        res = cloog_loop_recurse(temp, level, scalar, scaldims, nb_scattdims,
                                 constant, options);
    else
        while (temp != NULL) {
            next = temp->next ;
            l = cloog_loop_nest(temp->inner, temp->domain, level+1);
            new_loop = cloog_loop_alloc(temp->state, temp->domain, 0, NULL,
                                        NULL, l, NULL);
            temp->inner = NULL ;
            temp->next = NULL ;
            cloog_loop_free_parts(temp,0,0,0,0) ;
            cloog_loop_add(&res,&now,new_loop) ;
            temp = next ;
        }

    if (options->strides)
        res = cloog_loop_propagate_lower_bound(res, level);

    /* 5. eliminate unused iterations of the current level for the new one. See
     *    the example called linearity-1-1 example with and without this part
     *    for an idea.
     */
    if (options->backtrack && level &&
            ((level+scalar < last) || (last < 0)) &&
            ((first <= level+scalar) && !(first < 0)))
        res = cloog_loop_generate_backtrack(res, level, options);

    /* Pray for my new paper to be accepted somewhere since the following stuff
     * is really amazing :-) !
     * Far long later: The paper has been accepted to PACT 2004 :-))). But there
     * are still some bugs and I have no time to fix them. Thus now you have to
     * pray for me to get an academic position for that really amazing stuff :-) !
     * Later again: OK, I get my academic position, but still I have not enough
     * time to fix and clean this part... Pray again :-) !!!
     */
    /* res = cloog_loop_unisolate(res,level) ;*/

    return(res) ;
}


CloogLoop *cloog_loop_generate_restricted(CloogLoop *loop,
        int level, int scalar, int *scaldims, int nb_scattdims,
        CloogOptions *options);


/**
 * cloog_loop_generate_scalar function:
 * This function applies the simplified code generation scheme in the trivial
 * case of scalar dimensions. When dealing with scalar dimensions, there is
 * no need of costly polyhedral operations for separation or sorting: sorting
 * is a question of comparing scalar vectors and separation amounts to consider
 * only loops with the same scalar vector for the next step of the code
 * generation process. This function achieves the separation/sorting process
 * for the vector of scalar dimension that begins at dimension 'level+scalar'
 * and finish to the first non-scalar dimension.
 * - loop is the loop for which we have to generate a scanning code,
 * - level is the current non-scalar dimension,
 * - scalar is the current scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - options are the general code generation options.
 **
 * - September  2nd 2005: First version.
 */
CloogLoop *cloog_loop_generate_scalar(CloogLoop *loop,
                                      int level, int scalar, int *scaldims, int nb_scattdims,
                                      CloogOptions *options) {
    CloogLoop * res, * now, * temp, * l, * end, * next, * ref ;
    int scalar_new;

    /* We sort the loop list with respect to the current scalar vector. */
    res = cloog_loop_scalar_sort(loop,level,scaldims,nb_scattdims,scalar) ;

    scalar_new = scalar + scaldims[level + scalar - 1];

    temp = res ;
    res = NULL ;
    while (temp != NULL) {
        /* Then we will appy the general code generation process to each sub-list
         * of loops with the same scalar vector.
         */
        end = temp ;
        ref = temp ;

        while((end->next != NULL) &&
                cloog_loop_more(end->next, level, scalar_new, nb_scattdims) &&
                cloog_loop_scalar_eq(ref,end->next,level,scaldims,nb_scattdims,scalar))
            end = end->next ;

        next = end->next ;
        end->next = NULL ;

        /* For the next dimension, scalar value is updated by adding the scalar
         * vector size, which is stored at scaldims[level+scalar-1].
         */
        if (cloog_loop_more(temp, level, scalar_new, nb_scattdims)) {
            l = cloog_loop_generate_restricted(temp, level, scalar_new,
                                               scaldims, nb_scattdims, options);

            if (l != NULL)
                cloog_loop_add_list(&res, &now, l);
        } else
            cloog_loop_add(&res, &now, temp);

        temp = next ;
    }

    return res ;
}


/* Compare loop with the next loop based on their constant dimensions.
 * The result is < 0, == 0 or > 0 depending on whether the constant
 * dimensions of loop are lexicographically smaller, equal or greater
 * than those of loop->next.
 * If loop is the last in the list, then it is assumed to be smaller
 * than the "next" one.
 */
static int cloog_loop_next_scal_cmp(CloogLoop *loop) {
    int i;
    int nb_scaldims;

    if (!loop->next)
        return -1;

    nb_scaldims = loop->block->nb_scaldims;
    if (loop->next->block->nb_scaldims < nb_scaldims)
        nb_scaldims = loop->next->block->nb_scaldims;

    for (i = 0; i < nb_scaldims; ++i) {
        int cmp = cloog_int_cmp(loop->block->scaldims[i],
                                loop->next->block->scaldims[i]);
        if (cmp)
            return cmp;
    }
    return loop->block->nb_scaldims - loop->next->block->nb_scaldims;
}


/* Check whether the globally constant dimensions of a and b
 * have the same value for all globally constant dimensions
 * that are situated before any (locally) non-constant dimension.
 */
static int cloog_loop_equal_prefix(CloogLoop *a, CloogLoop *b,
                                   int *scaldims, int nb_scattdims) {
    int i;
    int cst = 0;
    int dim = 0;

    for (i = 0; i < nb_scattdims; ++i) {
        if (!scaldims[i]) {
            dim++;
            continue;
        }
        if (!cloog_int_eq(a->block->scaldims[cst], b->block->scaldims[cst]))
            break;
        cst++;
    }
    for (i = i + 1; i < nb_scattdims; ++i) {
        if (scaldims[i])
            continue;
        if (!cloog_domain_lazy_isconstant(a->domain, dim, NULL))
            return 0;
        /* No need to check that dim is also constant in b and that the
         * constant values are equal.  That will happen during the check
         * whether the two domains are equal.
         */
        dim++;
    }
    return 1;
}


/* Try to block adjacent loops in the loop list "loop".
 * We only attempt blocking if the constant dimensions of the loops
 * in the least are (not necessarily strictly) increasing.
 * Then we look for a sublist such that the first (begin) has constant
 * dimensions strictly larger than the previous loop in the complete
 * list and such that the loop (end) after the last loop in the sublist
 * has constant dimensions strictly larger than the last loop in the sublist.
 * Furthermore, all loops in the sublist should have the same domain
 * (with globally constant dimensions removed) and the difference
 * (if any) in constant dimensions may only occur after all the
 * (locally) constant dimensions.
 * If we find such a sublist, then the blocks of all but the first
 * are merged into the block of the first.
 *
 * Note that this function can only be called before the global
 * blocklist has been created because it may otherwise modify and destroy
 * elements on that list.
 */
CloogLoop *cloog_loop_block(CloogLoop *loop, int *scaldims, int nb_scattdims) {
    CloogLoop *begin, *end, *l;
    int begin_after_previous;
    int end_after_previous;

    if (!loop->next)
        return loop;
    for (begin = loop; begin; begin = begin->next) {
        if (!begin->block || !begin->block->scaldims)
            return loop;
        if (cloog_loop_next_scal_cmp(begin) > 0)
            return loop;
    }

    begin_after_previous = 1;
    for (begin = loop; begin; begin = begin->next) {
        if (!begin_after_previous) {
            begin_after_previous = cloog_loop_next_scal_cmp(begin) < 0;
            continue;
        }

        end_after_previous = cloog_loop_next_scal_cmp(begin) < 0;
        for (end = begin->next; end; end = end->next) {
            if (!cloog_loop_equal_prefix(begin, end, scaldims, nb_scattdims))
                break;
            if (!cloog_domain_lazy_equal(begin->domain, end->domain))
                break;
            end_after_previous = cloog_loop_next_scal_cmp(end) < 0;
        }
        if (end != begin->next && end_after_previous) {
            for (l = begin->next; l != end; l = begin->next) {
                cloog_block_merge(begin->block, l->block);
                begin->next = l->next;
                cloog_loop_free_parts(l, 1, 0, 1, 0);
            }
        }

        begin_after_previous = cloog_loop_next_scal_cmp(begin) < 0;
    }

    return loop;
}


/**
 * Check whether for any fixed iteration of the outer loops,
 * there is an iteration of loop1 that is lexicographically greater
 * than an iteration of loop2.
 * Return 1 if there exists (or may exist) such a pair.
 * Return 0 if all iterations of loop1 are lexicographically smaller
 * than the iterations of loop2.
 * If no iteration is lexicographically greater, but if there are
 * iterations that are equal to iterations of loop2, then return "def".
 * This is useful for ensuring that such statements are not reordered.
 * Some users, including the test_run target in test, expect
 * the statements at a given point to be run in the original order.
 * Passing the value "0" for "def" would allow such statements to be reordered
 * and would allow for the detection of more components.
 */
int cloog_loop_follows(CloogLoop *loop1, CloogLoop *loop2,
                       int level, int scalar, int *scaldims, int nb_scattdims, int def) {
    int dim1, dim2;

    dim1 = cloog_domain_dimension(loop1->domain);
    dim2 = cloog_domain_dimension(loop2->domain);
    while ((level <= dim1 && level <= dim2) ||
            level_is_constant(level, scalar, scaldims, nb_scattdims)) {
        if (level_is_constant(level, scalar, scaldims, nb_scattdims)) {
            int cmp = cloog_loop_constant_cmp(loop1, loop2, level, scaldims,
                                              nb_scattdims, scalar);
            if (cmp > 0)
                return 1;
            if (cmp < 0)
                return 0;
            scalar += scaldims[level + scalar - 1];
        } else {
            int follows = cloog_domain_follows(loop1->domain, loop2->domain,
                                               level);
            if (follows > 0)
                return 1;
            if (follows < 0)
                return 0;
            level++;
        }
    }

    return def;
}


/* Structure for representing the nodes in the graph being traversed
 * using Tarjan's algorithm.
 * index represents the order in which nodes are visited.
 * min_index is the index of the root of a (sub)component.
 * on_stack indicates whether the node is currently on the stack.
 */
struct cloog_loop_sort_node {
    int index;
    int min_index;
    int on_stack;
};
/* Structure for representing the graph being traversed
 * using Tarjan's algorithm.
 * len is the number of nodes
 * node is an array of nodes
 * stack contains the nodes on the path from the root to the current node
 * sp is the stack pointer
 * index is the index of the last node visited
 * order contains the elements of the components separated by -1
 * op represents the current position in order
 */
struct cloog_loop_sort {
    int len;
    struct cloog_loop_sort_node *node;
    int *stack;
    int sp;
    int index;
    int *order;
    int op;
};

/* Allocate and initialize cloog_loop_sort structure.
 */
static struct cloog_loop_sort *cloog_loop_sort_alloc(int len) {
    struct cloog_loop_sort *s;
    int i;

    s = (struct cloog_loop_sort *)malloc(sizeof(struct cloog_loop_sort));
    assert(s);
    s->len = len;
    s->node = (struct cloog_loop_sort_node *)
              malloc(len * sizeof(struct cloog_loop_sort_node));
    assert(s->node);
    for (i = 0; i < len; ++i)
        s->node[i].index = -1;
    s->stack = (int *)malloc(len * sizeof(int));
    assert(s->stack);
    s->order = (int *)malloc(2 * len * sizeof(int));
    assert(s->order);

    s->sp = 0;
    s->index = 0;
    s->op = 0;

    return s;
}

/* Free cloog_loop_sort structure.
 */
static void cloog_loop_sort_free(struct cloog_loop_sort *s) {
    free(s->node);
    free(s->stack);
    free(s->order);
    free(s);
}


/* Check whether for any fixed iteration of the outer loops,
 * there is an iteration of loop1 that is lexicographically greater
 * than an iteration of loop2, where the iteration domains are
 * available in the inner loops of the arguments.
 *
 * By using this functions to detect components, we ensure that
 * two CloogLoops appear in the same component if some iterations of
 * each loop should be executed before some iterations of the other loop.
 * Since we also want two CloogLoops that have exactly the same
 * iteration domain at the current level to be placed in the same component,
 * we first check if these domains are indeed the same.
 */
static int inner_loop_follows(CloogLoop *loop1, CloogLoop *loop2,
                              int level, int scalar, int *scaldims, int nb_scattdims, int def) {
    int f;

    f = cloog_domain_lazy_equal(loop1->domain, loop2->domain);
    if (!f)
        f = cloog_loop_follows(loop1->inner, loop2->inner,
                               level, scalar, scaldims, nb_scattdims, def);

    return f;
}


/* Perform Tarjan's algorithm for computing the strongly connected components
 * in the graph with the individual CloogLoops as vertices.
 * Two CloopLoops appear in the same component if they both (indirectly)
 * "follow" each other, where the following relation is determined
 * by the follows function.
 */
static void cloog_loop_components_tarjan(struct cloog_loop_sort *s,
        CloogLoop **loop_array, int i, int level, int scalar, int *scaldims,
        int nb_scattdims,
        int (*follows)(CloogLoop *loop1, CloogLoop *loop2,
                       int level, int scalar, int *scaldims, int nb_scattdims, int def)) {
    int j;

    s->node[i].index = s->index;
    s->node[i].min_index = s->index;
    s->node[i].on_stack = 1;
    s->index++;
    s->stack[s->sp++] = i;

    for (j = s->len - 1; j >= 0; --j) {
        int f;

        if (j == i)
            continue;
        if (s->node[j].index >= 0 &&
                (!s->node[j].on_stack ||
                 s->node[j].index > s->node[i].min_index))
            continue;

        f = follows(loop_array[i], loop_array[j],
                    level, scalar, scaldims, nb_scattdims, i > j);
        if (!f)
            continue;

        if (s->node[j].index < 0) {
            cloog_loop_components_tarjan(s, loop_array, j, level, scalar,
                                         scaldims, nb_scattdims, follows);
            if (s->node[j].min_index < s->node[i].min_index)
                s->node[i].min_index = s->node[j].min_index;
        } else if (s->node[j].index < s->node[i].min_index)
            s->node[i].min_index = s->node[j].index;
    }

    if (s->node[i].index != s->node[i].min_index)
        return;

    do {
        j = s->stack[--s->sp];
        s->node[j].on_stack = 0;
        s->order[s->op++] = j;
    } while (j != i);
    s->order[s->op++] = -1;
}


static int qsort_index_cmp(const void *p1, const void *p2) {
    return *(int *)p1 - *(int *)p2;
}

/* Sort the elements of the component starting at list.
 * The list is terminated by a -1.
 */
static void sort_component(int *list) {
    int len;

    for (len = 0; list[len] != -1; ++len)
        ;

    qsort(list, len, sizeof(int), qsort_index_cmp);
}

/* Given an array of indices "list" into the "loop_array" array,
 * terminated by -1, construct a linked list of the corresponding
 * entries and put the result in *res.
 * The value returned is the number of CloogLoops in the (linked) list
 */
static int extract_component(CloogLoop **loop_array, int *list, CloogLoop **res) {
    int i = 0;

    sort_component(list);
    while (list[i] != -1) {
        *res = loop_array[list[i]];
        res = &(*res)->next;
        ++i;
    }
    *res = NULL;

    return i;
}


/**
 * Call cloog_loop_generate_scalar or cloog_loop_generate_general
 * on each of the strongly connected components in the list of CloogLoops
 * pointed to by "loop".
 *
 * We use Tarjan's algorithm to find the strongly connected components.
 * Note that this algorithm also topologically sorts the components.
 *
 * The components are treated separately to avoid spurious separations.
 * The concatentation of the results may contain successive loops
 * with the same bounds, so we try to combine such loops.
 */
CloogLoop *cloog_loop_generate_components(CloogLoop *loop,
        int level, int scalar, int *scaldims, int nb_scattdims,
        CloogOptions *options) {
    int i, nb_loops;
    CloogLoop *tmp;
    CloogLoop *res, **res_next;
    CloogLoop **loop_array;
    struct cloog_loop_sort *s;

    if (level == 0 || !loop->next)
        return cloog_loop_generate_general(loop, level, scalar,
                                           scaldims, nb_scattdims, options);

    nb_loops = cloog_loop_count(loop);

    loop_array = (CloogLoop **)malloc(nb_loops * sizeof(CloogLoop *));
    assert(loop_array);

    for (i = 0, tmp = loop; i < nb_loops; i++, tmp = tmp->next)
        loop_array[i] = tmp;

    s = cloog_loop_sort_alloc(nb_loops);
    for (i = nb_loops - 1; i >= 0; --i) {
        if (s->node[i].index >= 0)
            continue;
        cloog_loop_components_tarjan(s, loop_array, i, level, scalar, scaldims,
                                     nb_scattdims, &inner_loop_follows);
    }

    i = 0;
    res = NULL;
    res_next = &res;
    while (nb_loops) {
        int n = extract_component(loop_array, &s->order[i], &tmp);
        i += n + 1;
        nb_loops -= n;
        *res_next = cloog_loop_generate_general(tmp, level, scalar,
                                                scaldims, nb_scattdims, options);
        while (*res_next)
            res_next = &(*res_next)->next;
    }

    cloog_loop_sort_free(s);

    free(loop_array);

    res = cloog_loop_combine(res);

    return res;
}


/* For each loop in the list "loop", decompose the list of
 * inner loops into strongly connected components and put
 * the components into separate loops at the top level.
 */
CloogLoop *cloog_loop_decompose_inner(CloogLoop *loop,
                                      int level, int scalar, int *scaldims, int nb_scattdims) {
    CloogLoop *l, *tmp;
    CloogLoop **loop_array;
    int i, n_loops, max_loops = 0;
    struct cloog_loop_sort *s;

    for (l = loop; l; l = l->next) {
        n_loops = cloog_loop_count(l->inner);
        if (max_loops < n_loops)
            max_loops = n_loops;
    }

    if (max_loops <= 1)
        return loop;

    loop_array = (CloogLoop **)malloc(max_loops * sizeof(CloogLoop *));
    assert(loop_array);

    for (l = loop; l; l = l->next) {
        int n;

        for (i = 0, tmp = l->inner; tmp; i++, tmp = tmp->next)
            loop_array[i] = tmp;
        n_loops = i;
        if (n_loops <= 1)
            continue;

        s = cloog_loop_sort_alloc(n_loops);
        for (i = n_loops - 1; i >= 0; --i) {
            if (s->node[i].index >= 0)
                continue;
            cloog_loop_components_tarjan(s, loop_array, i, level, scalar,
                                         scaldims, nb_scattdims, &cloog_loop_follows);
        }

        n = extract_component(loop_array, s->order, &l->inner);
        n_loops -= n;
        i = n + 1;
        while (n_loops) {
            CloogLoop *inner;

            n = extract_component(loop_array, &s->order[i], &inner);
            n_loops -= n;
            i += n + 1;
            tmp = cloog_loop_alloc(l->state, cloog_domain_copy(l->domain),
                                   l->otl, l->stride, l->block, inner, l->next);
            l->next = tmp;
            l = tmp;
        }

        cloog_loop_sort_free(s);
    }

    free(loop_array);

    return loop;
}


CloogLoop *cloog_loop_generate_restricted(CloogLoop *loop,
        int level, int scalar, int *scaldims, int nb_scattdims,
        CloogOptions *options) {
    /* To save both time and memory, we switch here depending on whether the
     * current dimension is scalar (simplified processing) or not (general
     * processing).
     */
    if (level_is_constant(level, scalar, scaldims, nb_scattdims))
        return cloog_loop_generate_scalar(loop, level, scalar,
                                          scaldims, nb_scattdims, options);
    /*
     * 2. Compute the projection of each polyhedron onto the outermost
     *    loop variable and the parameters.
     */
    loop = cloog_loop_project_all(loop, level);

    return cloog_loop_generate_components(loop, level, scalar, scaldims,
                                          nb_scattdims, options);
}


CloogLoop *cloog_loop_generate_restricted_or_stop(CloogLoop *loop,
        CloogDomain *context,
        int level, int scalar, int *scaldims, int nb_scattdims,
        CloogOptions *options) {
    /* If the user asked to stop code generation at this level, let's stop. */
    if ((options->stop >= 0) && (level+scalar >= options->stop+1))
        return cloog_loop_stop(loop,context) ;

    return cloog_loop_generate_restricted(loop, level, scalar, scaldims,
                                          nb_scattdims, options);
}


/**
 * cloog_loop_generate function:
 * Adaptation from LoopGen 0.4 by F. Quillere. This function implements the
 * Quillere algorithm for polyhedron scanning from step 1 to 2.
 * (see the Quillere paper).
 * - loop is the loop for which we have to generate a scanning code,
 * - context is the context of the current loop (constraints on parameter and/or
 *   on outer loop counters),
 * - level is the current non-scalar dimension,
 * - scalar is the current scalar dimension,
 * - scaldims is the boolean array saying whether a dimension is scalar or not,
 * - nb_scattdims is the size of the scaldims array,
 * - options are the general code generation options.
 **
 * - October 26th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - June      15th 2005: a memory leak fixed (loop was not entirely freed when
 *                        the result of cloog_loop_restrict was NULL).
 * - June      22nd 2005: Adaptation for GMP.
 * - September  2nd 2005: The function have been cutted out in two pieces:
 *                        cloog_loop_generate and this one, in order to handle
 *                        the scalar dimension case more efficiently with
 *                        cloog_loop_generate_scalar.
 * - November  15th 2005: (debug) Condition for stop option no more take care of
 *                        further scalar dimensions.
 */
CloogLoop *cloog_loop_generate(CloogLoop *loop, CloogDomain *context,
                               int level, int scalar, int *scaldims, int nb_scattdims,
                               CloogOptions *options) {
    /* 1. Replace each polyhedron by its intersection with the context.
     */
    loop = cloog_loop_restrict_all(loop, context);
    if (!loop)
        return NULL;

    return cloog_loop_generate_restricted_or_stop(loop, context,
            level, scalar, scaldims, nb_scattdims, options);
}


/*
 * Internal function for simplifying a single loop in a list of loops.
 * See cloog_loop_simplify.
 */
static CloogLoop *loop_simplify(CloogLoop *loop, CloogDomain *context,
                                int level, int nb_scattdims, CloogOptions *options) {
    int domain_dim;
    CloogBlock * new_block ;
    CloogLoop *simplified, *inner;
    CloogDomain * domain, * simp, * inter, * extended_context ;

    domain = loop->domain ;

    domain_dim = cloog_domain_dimension(domain);
    extended_context = cloog_domain_extend(context, domain_dim);
    inter = cloog_domain_intersection(domain,extended_context) ;
    simp = cloog_domain_simplify(domain, extended_context);
    cloog_domain_free(extended_context) ;

    /* If the constraint system is never true, go to the next one. */
    if (cloog_domain_never_integral(simp)) {
        cloog_loop_free(loop->inner);
        cloog_domain_free(inter);
        cloog_domain_free(simp);
        return NULL;
    }

    inner = cloog_loop_simplify(loop->inner, inter, level+1, nb_scattdims,
                                options);

    if ((inner == NULL) && (loop->block == NULL)) {
        cloog_domain_free(inter);
        cloog_domain_free(simp);
        return NULL;
    }

    new_block = cloog_block_copy(loop->block) ;

    simplified = cloog_loop_alloc(loop->state, simp, loop->otl, loop->stride,
                                  new_block, inner, NULL);

    if (options->save_domains) {
        inter = cloog_domain_add_stride_constraint(inter, loop->stride);
        if (domain_dim > nb_scattdims) {
            CloogDomain *t;
            inter = cloog_domain_project(t = inter, nb_scattdims);
            cloog_domain_free(t);
        }
        simplified->unsimplified = inter;
    } else
        cloog_domain_free(inter);

    return(simplified) ;
}


/**
 * cloog_loop_simplify function:
 * This function implements the part 6. of the Quillere algorithm, it
 * recursively simplifies each loop in the context of the preceding loop domain.
 * It returns a pointer to the simplified loop list.
 * The cloog_domain_simplify (DomainSimplify) behaviour is really bad with
 * polyhedra union and some really awful sidesteppings were written, I plan
 * to solve that...
 * - October   31th 2001: first version.
 * - July 3rd->11th 2003: memory leaks hunt and correction.
 * - April     16th 2005: a memory leak fixed (extended_context was not freed).
 * - June      15th 2005: a memory leak fixed (loop was not conveniently freed
 *                        when the constraint system is never true).
 * - October   27th 2005: - this function called before cloog_loop_fast_simplify
 *                          is now the official cloog_loop_simplify function in
 *                          replacement of a slower and more complex one (after
 *                          deep changes in the pretty printer).
 *                        - we use cloog_loop_disjoint to fix the problem when
 *                          simplifying gives a union of polyhedra (before, it
 *                          was under the responsibility of the pretty printer).
 */
CloogLoop *cloog_loop_simplify(CloogLoop *loop, CloogDomain *context, int level,
                               int nb_scattdims, CloogOptions *options) {
    CloogLoop *now;
    CloogLoop *res = NULL;
    CloogLoop **next = &res;
    int need_split = 0;

    for (now = loop; now; now = now->next)
        if (!cloog_domain_isconvex(now->domain)) {
            now->domain = cloog_domain_simplify_union(now->domain);
            if (!cloog_domain_isconvex(now->domain))
                need_split = 1;
        }

    /* If the input of CLooG contains any union domains, then they
     * may not have been split yet at this point.  Do so now as the
     * clast construction assumes there are no union domains.
     */
    if (need_split)
        loop = cloog_loop_disjoint(loop);

    for (now = loop; now; now = now->next) {
        *next = loop_simplify(now, context, level, nb_scattdims, options);

        now->inner = NULL; /* For loop integrity. */
        cloog_domain_free(now->domain);
        now->domain = NULL;

        if (*next)
            next = &(*next)->next;
    }
    cloog_loop_free(loop);

    return res;
}


/**
 * cloog_loop_scatter function:
 * This function add the scattering (scheduling) informations in a loop.
 */
void cloog_loop_scatter(CloogLoop * loop, CloogScattering *scatt) {
    loop->domain = cloog_domain_scatter(loop->domain, scatt);
}

