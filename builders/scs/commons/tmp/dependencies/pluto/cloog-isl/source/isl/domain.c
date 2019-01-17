#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <cloog/isl/cloog.h>
#include <isl/list.h>
#include <isl/constraint.h>
#include <isl/ilp.h>
#include <isl/lp.h>
#include <isl/aff.h>
#include <isl/map.h>
#include <isl/val.h>

#ifdef OSL_SUPPORT
#include <osl/macros.h>
#include <osl/relation.h>
#endif

CloogDomain *cloog_domain_from_isl_set(__isl_take isl_set *set) {
    if (isl_set_is_params(set))
        set = isl_set_from_params(set);
    set = isl_set_detect_equalities(set);
    set = isl_set_compute_divs(set);
    return (CloogDomain *)set;
}

__isl_give isl_set *isl_set_from_cloog_domain(CloogDomain *domain) {
    return (isl_set *)domain;
}

CloogScattering *cloog_scattering_from_isl_map(__isl_take isl_map *map) {
    return (CloogScattering *)map;
}

__isl_give isl_map *isl_map_from_cloog_scattering(CloogScattering *scattering) {
    return (isl_map *)scattering;
}


/**
 * Returns true if each scattering dimension is defined in terms
 * of the original iterators.
 */
int cloog_scattering_fully_specified(CloogScattering *scattering,
                                     CloogDomain *domain) {
    isl_map *map = isl_map_from_cloog_scattering(scattering);
    return isl_map_is_single_valued(map);
}


CloogConstraintSet *cloog_domain_constraints(CloogDomain *domain) {
    isl_basic_set *bset;
    isl_set *set = isl_set_from_cloog_domain(domain);
    assert(isl_set_n_basic_set(set) == 1);
    bset = isl_set_copy_basic_set(set);
    return cloog_constraint_set_from_isl_basic_set(bset);
}


void cloog_domain_print_constraints(FILE *foo, CloogDomain *domain,
                                    int print_number) {
    isl_printer *p;
    isl_basic_set *bset;
    isl_set *set = isl_set_from_cloog_domain(domain);

    p = isl_printer_to_file(isl_set_get_ctx(set), foo);
    if (print_number) {
        p = isl_printer_set_output_format(p, ISL_FORMAT_EXT_POLYLIB);
        p = isl_printer_print_set(p, set);
    } else {
        assert(isl_set_n_basic_set(set) == 1);
        bset = isl_set_copy_basic_set(set);
        p = isl_printer_set_output_format(p, ISL_FORMAT_POLYLIB);
        p = isl_printer_print_basic_set(p, bset);
        isl_basic_set_free(bset);
    }
    isl_printer_free(p);
}


void cloog_scattering_print_constraints(FILE *foo, CloogScattering *scattering) {
    isl_map *map = isl_map_from_cloog_scattering(scattering);
    isl_printer *p;

    p = isl_printer_to_file(isl_map_get_ctx(map), foo);
    p = isl_printer_set_output_format(p, ISL_FORMAT_EXT_POLYLIB);
    p = isl_printer_print_map(p, map);
    isl_printer_free(p);
}


void cloog_domain_free(CloogDomain * domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_set_free(set);
}


void cloog_scattering_free(CloogScattering *scatt) {
    isl_map *map = isl_map_from_cloog_scattering(scatt);
    isl_map_free(map);
}


CloogDomain * cloog_domain_copy(CloogDomain * domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return cloog_domain_from_isl_set(isl_set_copy(set));
}


/**
 * cloog_domain_convex function:
 * Computes the convex hull of domain.
 */
CloogDomain *cloog_domain_convex(CloogDomain *domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    set = isl_set_coalesce(set);
    set = isl_set_from_basic_set(isl_set_convex_hull(isl_set_copy(set)));
    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_simple_convex:
 * Given a list (union) of polyhedra, this function returns a "simple"
 * convex hull of this union. According to the ISL manual the "simple" convex
 * hull correspond of a single polyhedron which containing the previous union of
 * polyhedra.
 */
CloogDomain *cloog_domain_simple_convex(CloogDomain *domain) {
    struct isl_basic_set *hull;
    isl_set *set = isl_set_from_cloog_domain(domain);

    if (cloog_domain_isconvex(domain))
        return cloog_domain_copy(domain);

    hull = isl_set_simple_hull(isl_set_copy(set));
    return cloog_domain_from_isl_set(isl_set_from_basic_set(hull));
}


/**
 * cloog_domain_simplify function:
 * Given two polyhedral domains (dom1) and (dom2),
 * this function finds the largest domain set (or the smallest list
 * of non-redundant constraints), that when intersected with polyhedral
 * domain (dom2) equals (dom1)intersect(dom2). The output is a new CloogDomain
 * structure with a polyhedral domain with the "redundant" constraints removed.
 * NB: the second domain is required not to be a union.
 */
CloogDomain *cloog_domain_simplify(CloogDomain *dom1, CloogDomain *dom2) {
    isl_set *set1 = isl_set_from_cloog_domain(dom1);
    isl_set *set2 = isl_set_from_cloog_domain(dom2);
    set1 = isl_set_gist(isl_set_copy(set1), isl_set_copy(set2));
    return cloog_domain_from_isl_set(set1);
}


/**
 * cloog_domain_union function:
 * This function returns a new polyhedral domain which is the union of
 * two polyhedral domains (dom1) U (dom2).
 * Frees dom1 and dom2;
 */
CloogDomain *cloog_domain_union(CloogDomain *dom1, CloogDomain *dom2) {
    isl_set *set1 = isl_set_from_cloog_domain(dom1);
    isl_set *set2 = isl_set_from_cloog_domain(dom2);
    set1 = isl_set_union(set1, set2);
    return cloog_domain_from_isl_set(set1);
}



/**
 * cloog_domain_intersection function:
 * This function returns a new polyhedral domain which is the intersection of
 * two polyhedral domains (dom1) \cap (dom2).
 */
CloogDomain *cloog_domain_intersection(CloogDomain *dom1, CloogDomain *dom2) {
    isl_set *set1 = isl_set_from_cloog_domain(dom1);
    isl_set *set2 = isl_set_from_cloog_domain(dom2);
    set1 = isl_set_intersect(isl_set_copy(set1), isl_set_copy(set2));
    return cloog_domain_from_isl_set(set1);
}


/**
 * cloog_domain_difference function:
 * Returns the set difference domain \ minus.
 */
CloogDomain *cloog_domain_difference(CloogDomain *domain, CloogDomain *minus) {
    isl_set *set1 = isl_set_from_cloog_domain(domain);
    isl_set *set2 = isl_set_from_cloog_domain(minus);
    set1 = isl_set_subtract(isl_set_copy(set1), isl_set_copy(set2));
    return cloog_domain_from_isl_set(set1);
}


/**
 * cloog_domain_sort function:
 * This function topologically sorts (nb_doms) domains. Here (doms) is an
 * array of pointers to CloogDomains, (nb_doms) is the number of domains,
 * (level) is the level to consider for partial ordering (nb_par) is the
 * parameter space dimension, (permut) if not NULL, is an array of (nb_doms)
 * integers that contains a permutation specification after call in order to
 * apply the topological sorting.
 */
void cloog_domain_sort(CloogDomain **doms, unsigned nb_doms, unsigned level,
                       int *permut) {
    int i, j, k, cmp;
    struct isl_ctx *ctx;
    unsigned char **follows;
    isl_set *set_i, *set_j;
    isl_basic_set *bset_i, *bset_j;

    if (!nb_doms)
        return;
    set_i = isl_set_from_cloog_domain(doms[0]);
    ctx = isl_set_get_ctx(set_i);
    for (i = 0; i < nb_doms; i++) {
        set_i = isl_set_from_cloog_domain(doms[i]);
        assert(isl_set_n_basic_set(set_i) == 1);
    }

    follows = isl_alloc_array(ctx, unsigned char *, nb_doms);
    assert(follows);
    for (i = 0; i < nb_doms; ++i) {
        follows[i] = isl_alloc_array(ctx, unsigned char, nb_doms);
        assert(follows[i]);
        for (j = 0; j < nb_doms; ++j)
            follows[i][j] = 0;
    }

    for (i = 1; i < nb_doms; ++i) {
        for (j = 0; j < i; ++j) {
            if (follows[i][j] || follows[j][i])
                continue;
            set_i = isl_set_from_cloog_domain(doms[i]);
            set_j = isl_set_from_cloog_domain(doms[j]);
            bset_i = isl_set_copy_basic_set(set_i);
            bset_j = isl_set_copy_basic_set(set_j);
            cmp = isl_basic_set_compare_at(bset_i, bset_j, level-1);
            isl_basic_set_free(bset_i);
            isl_basic_set_free(bset_j);
            if (!cmp)
                continue;
            if (cmp > 0) {
                follows[i][j] = 1;
                for (k = 0; k < i; ++k)
                    follows[i][k] |= follows[j][k];
            } else {
                follows[j][i] = 1;
                for (k = 0; k < i; ++k)
                    follows[k][i] |= follows[k][j];
            }
        }
    }

    for (i = 0, j = 0; i < nb_doms; j = (j + 1) % nb_doms) {
        for (k = 0; k < nb_doms; ++k)
            if (follows[j][k])
                break;
        if (k < nb_doms)
            continue;
        for (k = 0; k < nb_doms; ++k)
            follows[k][j] = 0;
        follows[j][j] = 1;
        permut[i] = 1 + j;
        ++i;
    }

    for (i = 0; i < nb_doms; ++i)
        free(follows[i]);
    free(follows);
}


/**
 * Check whether there is or may be any value of dom1 at the given level
 * that is greater than or equal to a value of dom2 at the same level.
 *
 * Return
 *	 1 is there is or may be a greater-than pair.
 *	 0 if there is no greater-than pair, but there may be an equal-to pair
 *	-1 if there is definitely no such pair
 */
int cloog_domain_follows(CloogDomain *dom1, CloogDomain *dom2, unsigned level) {
    isl_set *set1 = isl_set_from_cloog_domain(dom1);
    isl_set *set2 = isl_set_from_cloog_domain(dom2);
    int follows;

    follows = isl_set_follows_at(set1, set2, level - 1);
    assert(follows >= -1);

    return follows;
}


/**
 * cloog_domain_empty function:
 * Returns an empty domain of the same dimensions as template.
 */
CloogDomain *cloog_domain_empty(CloogDomain *template) {
    isl_set *set = isl_set_from_cloog_domain(template);
    isl_space *space = isl_set_get_space(set);
    return cloog_domain_from_isl_set(isl_set_empty(space));
}


/**
 * Return 1 if the specified dimension has both an upper and a lower bound.
 */
int cloog_domain_is_bounded(CloogDomain *dom, unsigned level) {
    isl_set *set = isl_set_from_cloog_domain(dom);
    return isl_set_dim_is_bounded(set, isl_dim_set, level - 1);
}


/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * cloog_domain_print_structure :
 * this function is a more human-friendly way to display the CloogDomain data
 * structure, it only shows the constraint system and includes an indentation
 * level (level) in order to work with others print_structure functions.
 */
void cloog_domain_print_structure(FILE *file, CloogDomain *domain, int level,
                                  const char *name) {
    int i ;
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_printer *p;

    /* Go to the right level. */
    for (i = 0; i < level; i++)
        fprintf(file, "|\t");

    if (!set) {
        fprintf(file, "+-- Null CloogDomain\n");
        return;
    }
    fprintf(file, "+-- %s\n", name);
    for (i = 0; i < level+1; ++i)
        fprintf(file, "|\t");

    p = isl_printer_to_file(isl_set_get_ctx(set), file);
    p = isl_printer_print_set(p, set);
    isl_printer_free(p);

    fprintf(file, "\n");
}


/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/


void cloog_domain_list_free(CloogDomainList *list) {
    CloogDomainList *next;

    for ( ; list; list = next) {
        next = list->next;
        cloog_domain_free(list->domain);
        free(list);
    }
}


/**
 * cloog_scattering_list_free function:
 * This function frees the allocated memory for a CloogScatteringList structure.
 */
void cloog_scattering_list_free(CloogScatteringList *list) {
    while (list != NULL) {
        CloogScatteringList *temp = list->next;
        isl_map *map = isl_map_from_cloog_scattering(list->scatt);
        isl_map_free(map);
        free(list);
        list = temp;
    }
}


/******************************************************************************
 *                               Reading function                             *
 ******************************************************************************/


/**
 * cloog_domain_read_context function:
 * Read parameter domain.
 */
CloogDomain *cloog_domain_read_context(CloogState *state, FILE *input) {
    struct isl_ctx *ctx = state->backend->ctx;
    isl_set *set;

    set = isl_set_read_from_file(ctx, input);
    set = isl_set_move_dims(set, isl_dim_param, 0,
                            isl_dim_set, 0, isl_set_dim(set, isl_dim_set));

    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_from_context
 * Reinterpret context by turning parameters into variables.
 */
CloogDomain *cloog_domain_from_context(CloogDomain *context) {
    isl_set *set = isl_set_from_cloog_domain(context);

    set = isl_set_move_dims(set, isl_dim_set, 0,
                            isl_dim_param, 0, isl_set_dim(set, isl_dim_param));

    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_union_read function:
 * This function reads a union of polyhedra into a file (input) and
 * returns a pointer to a CloogDomain containing the read information.
 */
CloogDomain *cloog_domain_union_read(CloogState *state,
                                     FILE *input, int nb_parameters) {
    struct isl_ctx *ctx = state->backend->ctx;
    struct isl_set *set;

    set = isl_set_read_from_file(ctx, input);
    if (isl_set_dim(set, isl_dim_param) != nb_parameters) {
        int dim = isl_set_dim(set, isl_dim_set);
        set = isl_set_move_dims(set, isl_dim_param, 0,
                                isl_dim_set, dim - nb_parameters, nb_parameters);
    }
    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_read_scattering function:
 * This function reads in a scattering function from the file input.
 *
 * We try to read the scattering relation as a map, but if it is
 * specified in the original PolyLib format, then isl_map_read_from_file
 * will treat the input as a set return a map with zero input dimensions.
 * In this case, we need to decompose the set into a map from
 * scattering dimensions to domain dimensions and then invert the
 * resulting map.
 */
CloogScattering *cloog_domain_read_scattering(CloogDomain *domain, FILE *input) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_ctx *ctx = isl_set_get_ctx(set);
    struct isl_map *scat;
    unsigned nparam;
    unsigned dim;
    unsigned n_scat;

    dim = isl_set_dim(set, isl_dim_set);
    nparam = isl_set_dim(set, isl_dim_param);
    scat = isl_map_read_from_file(ctx, input);
    if (isl_map_dim(scat, isl_dim_param) != nparam) {
        int n_out = isl_map_dim(scat, isl_dim_out);
        scat = isl_map_move_dims(scat, isl_dim_param, 0,
                                 isl_dim_out, n_out - nparam, nparam);
    }
    if (isl_map_dim(scat, isl_dim_in) != dim) {
        n_scat = isl_map_dim(scat, isl_dim_out) - dim;
        scat = isl_map_move_dims(scat, isl_dim_in, 0,
                                 isl_dim_out, n_scat, dim);
    }
    return cloog_scattering_from_isl_map(scat);
}

/******************************************************************************
 *                      CloogMatrix Reading function                          *
 ******************************************************************************/

/**
 * isl_constraint_read_from_matrix:
 * Convert a single line of a matrix to a isl_constraint.
 * Returns a pointer to the constraint if successful; NULL otherwise.
 */
static struct isl_constraint *isl_constraint_read_from_matrix(
    struct isl_space *dim, cloog_int_t *row) {
    struct isl_constraint *constraint;
    int j;
    int nvariables = isl_space_dim(dim, isl_dim_set);
    int nparam = isl_space_dim(dim, isl_dim_param);
    isl_local_space *ls = isl_local_space_from_space(dim);

    if (cloog_int_is_zero(row[0]))
        constraint = isl_equality_alloc(ls);
    else
        constraint = isl_inequality_alloc(ls);

    for (j = 0; j < nvariables; ++j) {
        isl_val *val = cloog_int_to_isl_val(isl_constraint_get_ctx(constraint), row[1 + j]);
        isl_constraint_set_coefficient_val(constraint, isl_dim_out, j, val);
    }

    for (j = 0; j < nparam; ++j) {
        isl_val *val = cloog_int_to_isl_val(isl_constraint_get_ctx(constraint), row[1 + nvariables + j]);
        isl_constraint_set_coefficient_val(constraint, isl_dim_param, j, val);
    }

    isl_val *val = cloog_int_to_isl_val(isl_constraint_get_ctx(constraint), row[1 + nvariables + nparam]);
    isl_constraint_set_constant_val(constraint, val);

    return constraint;
}

/**
 * isl_basic_set_read_from_matrix:
 * Convert matrix to basic_set. The matrix contains nparam parameter columns.
 * Returns a pointer to the basic_set if successful; NULL otherwise.
 */
static struct isl_basic_set *isl_basic_set_read_from_matrix(struct isl_ctx *ctx,
        CloogMatrix* matrix, int nparam) {
    struct isl_space *dim;
    struct isl_basic_set *bset;
    int i;
    unsigned nrows, ncolumns;

    nrows = matrix->NbRows;
    ncolumns = matrix->NbColumns;
    int nvariables = ncolumns - 2 - nparam;

    dim = isl_space_set_alloc(ctx, nparam, nvariables);

    bset = isl_basic_set_universe(isl_space_copy(dim));

    for (i = 0; i < nrows; ++i) {
        cloog_int_t *row = matrix->p[i];
        struct isl_constraint *constraint =
            isl_constraint_read_from_matrix(isl_space_copy(dim), row);
        bset = isl_basic_set_add_constraint(bset, constraint);
    }

    isl_space_free(dim);

    return bset;
}

/**
 * cloog_domain_from_cloog_matrix:
 * Create a CloogDomain containing the constraints described in matrix.
 * nparam is the number of parameters contained in the domain.
 * Returns a pointer to the CloogDomain if successful; NULL otherwise.
 */
CloogDomain *cloog_domain_from_cloog_matrix(CloogState *state,
        CloogMatrix *matrix, int nparam) {
    struct isl_ctx *ctx = state->backend->ctx;
    struct isl_basic_set *bset;

    bset = isl_basic_set_read_from_matrix(ctx, matrix, nparam);

    return cloog_domain_from_isl_set(isl_set_from_basic_set(bset));
}

/**
 * cloog_scattering_from_cloog_matrix:
 * Create a CloogScattering containing the constraints described in matrix.
 * nparam is the number of parameters contained in the domain.
 * Returns a pointer to the CloogScattering if successful; NULL otherwise.
 */
CloogScattering *cloog_scattering_from_cloog_matrix(CloogState *state,
        CloogMatrix *matrix, int nb_scat, int nb_par) {
    struct isl_ctx *ctx = state->backend->ctx;
    struct isl_basic_set *bset;
    struct isl_basic_map *scat;
    struct isl_space *dims;
    unsigned dim;

    bset = isl_basic_set_read_from_matrix(ctx, matrix, nb_par);
    dim = isl_basic_set_n_dim(bset) - nb_scat;
    dims = isl_space_alloc(ctx, nb_par, nb_scat, dim);

    scat = isl_basic_map_from_basic_set(bset, dims);
    scat = isl_basic_map_reverse(scat);
    return cloog_scattering_from_isl_map(isl_map_from_basic_map(scat));
}


/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


#ifdef OSL_SUPPORT
/**
 * Converts an openscop relation to a CLooG domain.
 * \param[in,out] state    CLooG state.
 * \param[in]     relation OpenScop relation to convert.
 * \return A new CloogDomain corresponding to the input OpenScop relation.
 */
CloogDomain *cloog_domain_from_osl_relation(CloogState *state,
        osl_relation_p relation) {
    char *str;
    struct isl_ctx *ctx = state->backend->ctx;
    isl_set *set;
    CloogDomain *domain = NULL;

    if (relation != NULL) {
        str = osl_relation_spprint_polylib(relation, NULL);
        set = isl_set_read_from_str(ctx, str);
        free(str);

        domain = cloog_domain_from_isl_set(set);
    }

    return domain;
}

/**
 * Converts an openscop scattering relation to a CLooG scattering.
 * \param[in,out] state    CLooG state.
 * \param[in]     relation OpenScop relation to convert.
 * \return A new CloogScattering corresponding to the input OpenScop relation.
 */
CloogScattering *cloog_scattering_from_osl_relation(CloogState *state,
        osl_relation_p relation) {
    char *str;
    struct isl_ctx *ctx = state->backend->ctx;
    isl_map *map;
    CloogScattering *scattering = NULL;

    if (relation != NULL) {
        if (relation->type != OSL_TYPE_SCATTERING)
            cloog_die("Cannot convert a non-scattering relation to a scattering.\n");

        str = osl_relation_spprint_polylib(relation, NULL);
        map = isl_map_read_from_str(ctx, str);
        free(str);

        scattering = cloog_scattering_from_isl_map(map);
    }

    return scattering;
}
#endif

/**
 * cloog_domain_isempty function:
 */
int cloog_domain_isempty(CloogDomain *domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return isl_set_is_empty(set);
}


/**
 * cloog_domain_universe function:
 * This function returns the complete dim-dimensional space.
 */
CloogDomain *cloog_domain_universe(CloogState *state, unsigned dim) {
    struct isl_space *dims;
    struct isl_basic_set *bset;

    dims = isl_space_set_alloc(state->backend->ctx, 0, dim);
    bset = isl_basic_set_universe(dims);
    return cloog_domain_from_isl_set(isl_set_from_basic_set(bset));
}


/**
 * cloog_domain_project function:
 * This function returns the projection of
 * (domain) on the (level) first dimensions (i.e. outer loops).
 */
CloogDomain *cloog_domain_project(CloogDomain *domain, int level) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    set = isl_set_remove_dims(isl_set_copy(set), isl_dim_set,
                              level, isl_set_n_dim(set) - level);
    set = isl_set_compute_divs(set);
    if (level > 0)
        set = isl_set_remove_divs_involving_dims(set,
                isl_dim_set, level - 1, 1);
    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_extend function:
 * This function returns the (domain) given as input with (dim)
 * dimensions and (nb_par) parameters.
 * This function does not free (domain), and returns a new CloogDomain.
 */
CloogDomain *cloog_domain_extend(CloogDomain *domain, int dim) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    int n = isl_set_dim(set, isl_dim_set);
    set = isl_set_add_dims(isl_set_copy(set), isl_dim_set, dim - n);
    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_never_integral function:
 * For us, an equality like 3*i -4 = 0 is always false since 4%3 != 0.
 * There is no need to check for such constraints explicitly for the isl
 * backend.
 */
int cloog_domain_never_integral(CloogDomain * domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return isl_set_is_empty(set);
}


/**
 * Check whether the loop at "level" is executed at most once.
 * We construct a map that maps all remaining variables to this iterator
 * and check whether this map is single valued.
 *
 * Alternatively, we could have mapped the domain through a mapping
 * [p] -> { [..., i] -> [..., i'] : i' > i }
 * and then taken the intersection of the original domain and the transformed
 * domain.  If this intersection is empty, then the corresponding
 * loop is executed at most once.
 */
int cloog_domain_is_otl(CloogDomain *domain, int level) {
    int otl;
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_map *map;

    map = isl_map_from_domain(isl_set_copy(set));
    map = isl_map_move_dims(map, isl_dim_out, 0, isl_dim_in, level - 1, 1);
    otl = isl_map_is_single_valued(map);
    isl_map_free(map);

    return otl;
}


/**
 * cloog_domain_stride function:
 * This function finds the stride imposed to unknown with the column number
 * 'strided_level' in order to be integral. For instance, if we have a
 * constraint like -i - 2j + 2k = 0, and we consider k, then k can be integral
 * only if (i + 2j)%2 = 0. Then only if i%2 = 0. Then k imposes a stride 2 to
 * the unknown i. The function returns the imposed stride in a parameter field.
 * - domain is the set of constraint we have to consider,
 * - strided_level is the column number of the unknown for which a stride have
 *   to be found,
 * - looking_level is the column number of the unknown that impose a stride to
 *   the first unknown.
 * - stride is the stride that is returned back as a function parameter.
 * - offset is the value of the constant c if the condition is of the shape
 *   (i + c)%s = 0, s being the stride.
 */
void cloog_domain_stride(CloogDomain *domain, int strided_level,
                         cloog_int_t *stride, cloog_int_t *offset) {
    int ret = -1;
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_val *stride_val = NULL;
    isl_val *offset_val = NULL;
    ret = isl_set_dim_residue_class_val(set, strided_level - 1, &stride_val, &offset_val);
    if (ret != 0)
        cloog_die("failure to compute stride.\n");
    isl_val_to_cloog_int(stride_val, stride);
    isl_val_to_cloog_int(offset_val, offset);

    if (!cloog_int_is_zero(*offset))
        cloog_int_sub(*offset, *stride, *offset);

    isl_val_free(stride_val);
    isl_val_free(offset_val);

    return;
}


struct cloog_can_stride {
    int level;
    int can_stride;
};

static int constraint_can_stride(__isl_take isl_constraint *c, void *user) {
    struct cloog_can_stride *ccs = (struct cloog_can_stride *)user;
    int i;
    isl_val *v;
    unsigned n_div;

    if (isl_constraint_is_equality(c)) {
        isl_constraint_free(c);
        return 0;
    }

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, ccs->level - 1);
    if (isl_val_is_pos(v)) {
        n_div = isl_constraint_dim(c, isl_dim_div);

        for (i = 0; i < n_div; ++i) {
            isl_val_free(v);
            v = isl_constraint_get_coefficient_val(c, isl_dim_div, i);
            if (!isl_val_is_zero(v))
                break;
        }
        if (i < n_div)
            ccs->can_stride = 0;
    }
    isl_val_free(v);

    isl_constraint_free(c);
    return 0;
}

static int basic_set_can_stride(__isl_take isl_basic_set *bset, void *user) {
    struct cloog_can_stride *ccs = (struct cloog_can_stride *)user;
    int r;

    r = isl_basic_set_foreach_constraint(bset, constraint_can_stride, ccs);
    isl_basic_set_free(bset);
    return r;
}


/**
 * Return 1 if CLooG is allowed to perform stride detection on level "level"
 * and 0 otherwise.
 * Currently, stride detection is only allowed when none of the lower
 * bound constraints involve any existentially quantified variables.
 * The reason is that the current isl interface does not make it
 * easy to construct an integer division that depends on other integer
 * divisions.
 * By not allowing existentially quantified variables in the constraints,
 * we can ignore them in cloog_domain_stride_lower_bound.
 */
int cloog_domain_can_stride(CloogDomain *domain, int level) {
    struct cloog_can_stride ccs = { level, 1 };
    isl_set *set = isl_set_from_cloog_domain(domain);
    int r;
    r = isl_set_foreach_basic_set(set, basic_set_can_stride, &ccs);
    assert(r == 0);
    return ccs.can_stride;
}


struct cloog_stride_lower {
    int level;
    CloogStride *stride;
    isl_set *set;
    isl_basic_set *bounds;
};

/* If the given constraint is a lower bound on csl->level, then add
 * a lower bound to csl->bounds that makes sure that the remainder
 * of the smallest value on division by csl->stride is equal to csl->offset.
 *
 * In particular, the given lower bound is of the form
 *
 *	a i + f >= 0
 *
 * where f may depend on the parameters and other iterators.
 * The stride is s and the offset is d.
 * The lower bound -f/a may not satisfy the above condition.  In fact,
 * it may not even be integral.  We want to round this value of i up
 * to the nearest value that satisfies the condition and add the corresponding
 * lower bound constraint.  This nearest value is obtained by rounding
 * i - d up to the nearest multiple of s.
 * That is, we first subtract d
 *
 *	i' = -f/a - d
 *
 * then we round up to the nearest multiple of s
 *
 *	i'' = s * ceil(i'/s)
 *
 * and finally, we add d again
 *
 *	i''' = i'' + d
 *
 * and impose the constraint i >= i'''.
 *
 * We find
 *
 *	i'' = s * ceil((-f - a * d)/(a * s)) = - s * floor((f + a * d)/(a * s))
 *
 *	i >= - s * floor((f + a * d)/(a * s)) + d
 *
 * or
 *	i + s * floor((f + a * d)/(a * s)) - d >= 0
 */
static int constraint_stride_lower(__isl_take isl_constraint *c, void *user) {
    struct cloog_stride_lower *csl = (struct cloog_stride_lower *)user;
    isl_val *v;
    isl_constraint *bound;
    isl_aff *b;

    if (isl_constraint_is_equality(c)) {
        isl_constraint_free(c);
        return 0;
    }

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, csl->level - 1);
    if (!isl_val_is_pos(v)) {
        isl_val_free(v);
        isl_constraint_free(c);

        return 0;
    }
    isl_val_free(v);

    b = isl_constraint_get_bound(c, isl_dim_set, csl->level - 1);

    b = isl_aff_neg(b);
    b = isl_aff_add_constant_val(b, cloog_int_to_isl_val(isl_constraint_get_ctx(c), csl->stride->offset));
    b = isl_aff_scale_down_val(b, cloog_int_to_isl_val(isl_constraint_get_ctx(c), csl->stride->stride));
    b = isl_aff_floor(b);
    b = isl_aff_scale_val(b, cloog_int_to_isl_val(isl_constraint_get_ctx(c), csl->stride->stride));
    v = cloog_int_to_isl_val(isl_constraint_get_ctx(c), csl->stride->offset);
    v = isl_val_neg(v);
    b = isl_aff_add_constant_val(b, v);
    b = isl_aff_add_coefficient_si(b, isl_dim_in, csl->level - 1, 1);

    bound = isl_inequality_from_aff(b);

    csl->bounds = isl_basic_set_add_constraint(csl->bounds, bound);

    isl_constraint_free(c);

    return 0;
}

/* This functions performs essentially the same operation as
 * constraint_stride_lower, the only difference being that the offset d
 * is not a constant, but an affine expression in terms of the parameters
 * and earlier variables.  In particular the affine expression is equal
 * to the coefficients of stride->constraint multiplied by stride->factor.
 * As in constraint_stride_lower, we add an extra bound
 *
 *	i + s * floor((f + a * d)/(a * s)) - d >= 0
 *
 * for each lower bound
 *
 *	a i + f >= 0
 *
 * where d is not the aforementioned affine expression.
 */
static int constraint_stride_lower_c(__isl_take isl_constraint *c, void *user) {
    struct cloog_stride_lower *csl = (struct cloog_stride_lower *)user;
    isl_val *v;
    isl_constraint *bound;
    isl_constraint *csl_c;
    isl_aff *d, *b;

    if (isl_constraint_is_equality(c)) {
        isl_constraint_free(c);
        return 0;
    }

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, csl->level - 1);
    if (!isl_val_is_pos(v)) {
        isl_val_free(v);
        isl_constraint_free(c);

        return 0;
    }

    csl_c = cloog_constraint_to_isl(csl->stride->constraint);

    d = isl_constraint_get_aff(csl_c);
    d = isl_aff_drop_dims(d, isl_dim_div, 0, isl_aff_dim(d, isl_dim_div));
    d = isl_aff_set_coefficient_si(d, isl_dim_in, csl->level - 1, 0);
    d = isl_aff_scale_val(d, cloog_int_to_isl_val(isl_constraint_get_ctx(csl_c), csl->stride->factor));

    b = isl_constraint_get_bound(c, isl_dim_set, csl->level - 1);

    b = isl_aff_neg(b);
    b = isl_aff_add(b, isl_aff_copy(d));
    b = isl_aff_scale_down_val(b, cloog_int_to_isl_val(isl_constraint_get_ctx(csl_c), csl->stride->stride));
    b = isl_aff_floor(b);
    b = isl_aff_scale_val(b, cloog_int_to_isl_val(isl_constraint_get_ctx(csl_c), csl->stride->stride));
    b = isl_aff_sub(b, d);
    b = isl_aff_add_coefficient_si(b, isl_dim_in, csl->level - 1, 1);

    bound = isl_inequality_from_aff(b);

    csl->bounds = isl_basic_set_add_constraint(csl->bounds, bound);

    isl_val_free(v);
    isl_constraint_free(c);

    return 0;
}

static int basic_set_stride_lower(__isl_take isl_basic_set *bset, void *user) {
    struct cloog_stride_lower *csl = (struct cloog_stride_lower *)user;
    int r;

    csl->bounds = isl_basic_set_universe(isl_basic_set_get_space(bset));
    if (csl->stride->constraint)
        r = isl_basic_set_foreach_constraint(bset,
                                             &constraint_stride_lower_c, csl);
    else
        r = isl_basic_set_foreach_constraint(bset,
                                             &constraint_stride_lower, csl);
    bset = isl_basic_set_intersect(bset, csl->bounds);
    csl->set = isl_set_union(csl->set, isl_set_from_basic_set(bset));

    return r;
}

/**
 * Update the lower bounds at level "level" to the given stride information.
 * That is, make sure that the remainder on division by "stride"
 * is equal to "offset".
 */
CloogDomain *cloog_domain_stride_lower_bound(CloogDomain *domain, int level,
        CloogStride *stride) {
    struct cloog_stride_lower csl;
    isl_set *set = isl_set_from_cloog_domain(domain);
    int r;

    csl.stride = stride;
    csl.level = level;
    csl.set = isl_set_empty(isl_set_get_space(set));

    r = isl_set_foreach_basic_set(set, basic_set_stride_lower, &csl);
    assert(r == 0);

    cloog_domain_free(domain);
    return cloog_domain_from_isl_set(csl.set);
}


/* Add stride constraint, if any, to domain.
 */
CloogDomain *cloog_domain_add_stride_constraint(CloogDomain *domain,
        CloogStride *stride) {
    isl_constraint *c;
    isl_set *set;

    if (!stride || !stride->constraint)
        return domain;

    set = isl_set_from_cloog_domain(domain);
    c = isl_constraint_copy(cloog_constraint_to_isl(stride->constraint));

    set = isl_set_add_constraint(set, c);

    return cloog_domain_from_isl_set(set);
}


/**
 * cloog_domain_lazy_equal function:
 * This function returns 1 if the domains given as input are the same, 0 if it
 * is unable to decide.
 */
int cloog_domain_lazy_equal(CloogDomain *d1, CloogDomain *d2) {
    isl_set *set1 = isl_set_from_cloog_domain(d1);
    isl_set *set2 = isl_set_from_cloog_domain(d2);
    return isl_set_plain_is_equal(set1, set2);
}

struct cloog_bound_split {
    isl_set *set;
    int level;
    int lower;
    int upper;
};

static int constraint_bound_split(__isl_take isl_constraint *c, void *user) {
    struct cloog_bound_split *cbs = (struct cloog_bound_split *)user;
    isl_val *v;
    int i;
    int handle = 0;

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, cbs->level - 1);
    if (!cbs->lower && isl_val_is_pos(v))
        cbs->lower = handle = 1;
    else if (!cbs->upper && isl_val_is_neg(v))
        cbs->upper = handle = 1;

    if (handle) {
        for (i = 0; i < isl_set_dim(cbs->set, isl_dim_param); ++i) {
            isl_val_free(v);
            v = isl_constraint_get_coefficient_val(c, isl_dim_param, i);
            if (isl_val_is_zero(v))
                continue;

            cbs->set = isl_set_split_dims(cbs->set,
                                          isl_dim_param, i, 1);
        }
    }
    isl_val_free(v);

    isl_constraint_free(c);
    return (cbs->lower && cbs->upper) ? -1 : 0;
}

static int basic_set_bound_split(__isl_take isl_basic_set *bset, void *user) {
    struct cloog_bound_split *cbs = (struct cloog_bound_split *)user;
    int r;

    cbs->lower = 0;
    cbs->upper = 0;
    r = isl_basic_set_foreach_constraint(bset, constraint_bound_split, cbs);
    isl_basic_set_free(bset);
    return ((!cbs->lower || !cbs->upper) && r < 0) ? -1 : 0;
}

/**
 * Return a union of sets S_i such that the convex hull of "dom",
 * when intersected with one the sets S_i, will have an upper and
 * lower bound for the dimension at "level" (provided "dom" itself
 * has such bounds for the dimensions).
 *
 * We currently take a very simple approach.  For each of the basic
 * sets in "dom" we pick a lower and an upper bound and split the
 * range of any parameter involved in these two bounds in a
 * nonnegative and a negative part.  This ensures that the symbolic
 * constant in these two constraints are themselves bounded and
 * so there will be at least one upper and one lower bound
 * in the convex hull.
 */
CloogDomain *cloog_domain_bound_splitter(CloogDomain *dom, int level) {
    struct cloog_bound_split cbs;
    isl_set *set = isl_set_from_cloog_domain(dom);
    int r;
    cbs.level = level;
    cbs.set = isl_set_universe(isl_set_get_space(set));
    r = isl_set_foreach_basic_set(set, basic_set_bound_split, &cbs);
    assert(r == 0);
    return cloog_domain_from_isl_set(cbs.set);
}


/* Check whether the union of scattering functions over all domains
 * is obviously injective.
 */
static int injective_scattering(CloogScatteringList *list) {
    isl_map *map;
    isl_union_map *umap;
    int injective;
    int i = 0;
    char name[30];

    if (!list)
        return 1;

    map = isl_map_copy(isl_map_from_cloog_scattering(list->scatt));
    snprintf(name, sizeof(name), "S%d", i);
    map = isl_map_set_tuple_name(map, isl_dim_in, name);
    umap = isl_union_map_from_map(map);

    for (list = list->next, ++i; list; list = list->next, ++i) {
        map = isl_map_copy(isl_map_from_cloog_scattering(list->scatt));
        snprintf(name, sizeof(name), "S%d", i);
        map = isl_map_set_tuple_name(map, isl_dim_in, name);
        umap = isl_union_map_add_map(umap, map);
    }

    injective = isl_union_map_plain_is_injective(umap);

    isl_union_map_free(umap);

    return injective;
}


/**
 * cloog_scattering_lazy_block function:
 * This function returns 1 if the two scattering functions s1 and s2 given
 * as input are the same (except possibly for the final dimension, where we
 * allow a difference of 1), assuming that the domains on which this
 * scatterings are applied are the same.
 * In fact this function answers the question "can I
 * safely consider the two domains as only one with two statements (a block) ?".
 * A difference of 1 in the final dimension is only allowed if the
 * entire scattering function is injective.
 * - s1 and s2 are the two domains to check for blocking,
 * - scattering is the linked list of all domains,
 * - scattdims is the total number of scattering dimentions.
 */
int cloog_scattering_lazy_block(CloogScattering *s1, CloogScattering *s2,
                                CloogScatteringList *scattering, int scattdims) {
    int i;
    struct isl_space *dim;
    struct isl_map *rel;
    struct isl_set *delta;
    isl_map *map1 = isl_map_from_cloog_scattering(s1);
    isl_map *map2 = isl_map_from_cloog_scattering(s2);
    int block;
    isl_val *cst;
    unsigned n_scat;

    n_scat = isl_map_dim(map1, isl_dim_out);
    if (n_scat != isl_map_dim(map2, isl_dim_out))
        return 0;

    dim = isl_map_get_space(map1);
    dim = isl_space_map_from_set(isl_space_domain(dim));
    rel = isl_map_identity(dim);
    rel = isl_map_apply_domain(rel, isl_map_copy(map1));
    rel = isl_map_apply_range(rel, isl_map_copy(map2));
    delta = isl_map_deltas(rel);
    cst = NULL;
    for (i = 0; i < n_scat; ++i) {
        cst = isl_set_plain_get_val_if_fixed(delta, isl_dim_set, i);
        if (!cst) {
            isl_val_free(cst);
            break;
        }
        if (isl_val_is_zero(cst)) {
            isl_val_free(cst);
            continue;
        }
        if (i + 1 < n_scat) {
            isl_val_free(cst);
            break;
        }
        if (!isl_val_is_one(cst)) {
            isl_val_free(cst);
            break;
        }
        if (!injective_scattering(scattering)) {
            isl_val_free(cst);
            break;
        }

        isl_val_free(cst);
    }
    block = i >= n_scat;
    isl_set_free(delta);
    return block;
}


/**
 * cloog_domain_lazy_disjoint function:
 * This function returns 1 if the domains given as input are disjoint, 0 if it
 * is unable to decide.
 */
int cloog_domain_lazy_disjoint(CloogDomain *d1, CloogDomain *d2) {
    isl_set *set1 = isl_set_from_cloog_domain(d1);
    isl_set *set2 = isl_set_from_cloog_domain(d2);
    return isl_set_plain_is_disjoint(set1, set2);
}


/**
 * cloog_scattering_list_lazy_same function:
 * This function returns 1 if two domains in the list are the same, 0 if it
 * is unable to decide.
 */
int cloog_scattering_list_lazy_same(CloogScatteringList *list) {
    CloogScatteringList *one, *other;
    isl_map *one_map, *other_map;

    for (one = list; one; one = one->next) {
        one_map = isl_map_from_cloog_scattering(one->scatt);
        for (other = one->next; other; other = other->next) {
            other_map = isl_map_from_cloog_scattering(other->scatt);
            if (isl_map_plain_is_equal(one_map, other_map))
                return 1;
        }
    }
    return 0;
}

int cloog_domain_dimension(CloogDomain * domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return isl_set_dim(set, isl_dim_set);
}

int cloog_domain_parameter_dimension(CloogDomain *domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return isl_set_dim(set, isl_dim_param);
}

int cloog_scattering_dimension(CloogScattering *scatt, CloogDomain *domain) {
    isl_map *map = isl_map_from_cloog_scattering(scatt);
    return isl_map_dim(map, isl_dim_out);
}

int cloog_domain_isconvex(CloogDomain * domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return isl_set_n_basic_set(set) <= 1;
}


/**
 * cloog_domain_cut_first function:
 * This function splits off and returns the first convex set in the
 * union "domain".  The remainder of the union is returned in rest.
 * The original "domain" itself is destroyed and may not be used
 * after a call to this function.
 */
CloogDomain *cloog_domain_cut_first(CloogDomain *domain, CloogDomain **rest) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    struct isl_basic_set *first;

    first = isl_set_copy_basic_set(set);
    set = isl_set_drop_basic_set(set, first);
    *rest = cloog_domain_from_isl_set(set);

    return cloog_domain_from_isl_set(isl_set_from_basic_set(first));
}


/**
 * Given a union domain, try to find a simpler representation
 * using fewer sets in the union.
 * The original "domain" itself is destroyed and may not be used
 * after a call to this function.
 */
CloogDomain *cloog_domain_simplify_union(CloogDomain *domain) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    return cloog_domain_from_isl_set(isl_set_coalesce(set));
}


/**
 * cloog_scattering_lazy_isscalar function:
 * this function returns 1 if the scattering dimension 'dimension' in the
 * scattering 'scatt' is constant.
 * If value is not NULL, then it is set to the constant value of dimension.
 */
int cloog_scattering_lazy_isscalar(CloogScattering *scatt, int dimension,
                                   cloog_int_t *value) {
    isl_map *map = isl_map_from_cloog_scattering(scatt);
    isl_val *v = isl_map_plain_get_val_if_fixed(map, isl_dim_out, dimension);
    if (v != NULL) {
        if (!isl_val_is_nan(v)) {
            if (value != NULL)
                isl_val_to_cloog_int(v, value);

            isl_val_free(v);
            return 1;
        } else {
            isl_val_free(v);
            return 0;
        }
    }

    return 0;
}


/**
 * cloog_domain_lazy_isconstant function:
 * this function returns 1 if the dimension 'dimension' in the
 * domain 'domain' is constant.
 * If value is not NULL, then it is set to the constant value of dimension.
 */
int cloog_domain_lazy_isconstant(CloogDomain *domain, int dimension,
                                 cloog_int_t *value) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_val *cst = isl_set_plain_get_val_if_fixed(set, isl_dim_set, dimension);
    if (cst != NULL) {
        if (!isl_val_is_nan(cst)) {
            if (value != NULL)
                isl_val_to_cloog_int(cst, value);

            isl_val_free(cst);
            return 1;
        } else {
            isl_val_free(cst);
            return 0;
        }
    }

    return 0;
}


/**
 * cloog_scattering_erase_dimension function:
 * this function returns a CloogDomain structure builds from 'domain' where
 * we removed the dimension 'dimension' and every constraint involving this
 * dimension.
 */
CloogScattering *cloog_scattering_erase_dimension(CloogScattering *scattering,
        int dimension) {
    isl_map *map = isl_map_from_cloog_scattering(scattering);
    map = isl_map_remove_dims(isl_map_copy(map), isl_dim_out, dimension, 1);
    return cloog_scattering_from_isl_map(map);
}

/**
 * cloog_domain_cube:
 * Construct and return a dim-dimensional cube, with values ranging
 * between min and max in each dimension.
 */
CloogDomain *cloog_domain_cube(CloogState *state,
                               int dim, cloog_int_t min, cloog_int_t max) {
    int i;
    isl_space *space;
    isl_set *cube;
    isl_val *min_v;
    isl_val *max_v;

    if (dim == 0)
        return cloog_domain_universe(state, dim);

    space = isl_space_set_alloc(state->backend->ctx, 0, dim);
    cube = isl_set_universe(space);
    for (i = 0; i < dim; ++i) {
        min_v = cloog_int_to_isl_val(isl_set_get_ctx(cube), min);
        max_v = cloog_int_to_isl_val(isl_set_get_ctx(cube), max);
        cube = isl_set_lower_bound_val(cube, isl_dim_set, i, min_v);
        cube = isl_set_upper_bound_val(cube, isl_dim_set, i, max_v);
    }

    return cloog_domain_from_isl_set(cube);
}

/**
 * cloog_domain_from_bounds
 * Create an N dimensional domain where each dimension's bounds are taken from
 * each pair of vector bounds [lower_bounds[i], upper_bounds[i]].
 */
CloogDomain *cloog_domain_from_bounds(
    CloogState *state, struct cloog_vec *lower_bounds,
    struct cloog_vec *upper_bounds) {
    unsigned i, dim;
    isl_space *space;
    isl_set *domain;
    isl_val *min_v;
    isl_val *max_v;

    assert(lower_bounds->size == upper_bounds->size);

    dim = upper_bounds->size;

    if (dim == 0)
        return cloog_domain_universe(state, 0);

    space = isl_space_set_alloc(state->backend->ctx, 0, dim);
    domain = isl_set_universe(space);
    for (i = 0; i < dim; ++i) {
        min_v = cloog_int_to_isl_val(isl_set_get_ctx(domain), lower_bounds->p[i]);
        max_v = cloog_int_to_isl_val(isl_set_get_ctx(domain), upper_bounds->p[i]);
        domain = isl_set_lower_bound_val(domain, isl_dim_set, i, min_v);
        domain = isl_set_upper_bound_val(domain, isl_dim_set, i, max_v);
    }

    return cloog_domain_from_isl_set(domain);
}


/**
 * cloog_domain_scatter function:
 * This function add the scattering (scheduling) informations to a domain.
 */
CloogDomain *cloog_domain_scatter(CloogDomain *domain, CloogScattering *scatt) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_map *map = isl_map_from_cloog_scattering(scatt);

    map = isl_map_reverse(isl_map_copy(map));
    map = isl_map_intersect_range(map, set);
    set = isl_set_flatten(isl_map_wrap(map));
    return cloog_domain_from_isl_set(set);
}

static int add_domain_from_map(__isl_take isl_map *map, void *user) {
    isl_space *dim;
    const char *name;
    CloogDomain *domain;
    CloogScattering *scat;
    CloogUnionDomain **ud = (CloogUnionDomain **)user;

    dim = isl_map_get_space(map);
    name = isl_space_get_tuple_name(dim, isl_dim_in);
    domain = cloog_domain_from_isl_set(isl_map_domain(isl_map_copy(map)));
    scat = cloog_scattering_from_isl_map(map);
    *ud = cloog_union_domain_add_domain(*ud, name, domain, scat, NULL);
    isl_space_free(dim);

    return 0;
}

/**
 * Construct a CloogUnionDomain from an isl_union_map representing
 * a global scattering function.  The input is a mapping from different
 * spaces (different tuple names and possibly different dimensions)
 * to a common space.  The iteration domains are set to the domains
 * in each space.  The statement names are set to the names of the
 * spaces.  The parameter names of the result are set to those of
 * the input, but the iterator and scattering dimension names are
 * left unspecified.
 */
CloogUnionDomain *cloog_union_domain_from_isl_union_map(
    __isl_take isl_union_map *umap) {
    int i;
    int nparam;
    isl_space *dim;
    CloogUnionDomain *ud;

    dim = isl_union_map_get_space(umap);
    nparam = isl_space_dim(dim, isl_dim_param);

    ud = cloog_union_domain_alloc(nparam);

    for (i = 0; i < nparam; ++i) {
        const char *s = isl_space_get_dim_name(dim, isl_dim_param, i);
        ud = cloog_union_domain_set_name(ud, CLOOG_PARAM, i, s);
    }
    isl_space_free(dim);

    if (isl_union_map_foreach_map(umap, &add_domain_from_map, &ud) < 0) {
        isl_union_map_free(umap);
        cloog_union_domain_free(ud);
        assert(0);
    }

    isl_union_map_free(umap);

    return ud;
}

static int count_same_name(__isl_keep isl_space *dim,
                           enum isl_dim_type type, unsigned pos, const char *name) {
    enum isl_dim_type t;
    unsigned p, s;
    int count = 0;
    int len = strlen(name);

    for (t = isl_dim_param; t <= type && t <= isl_dim_out; ++t) {
        s = t == type ? pos : isl_space_dim(dim, t);
        for (p = 0; p < s; ++p) {
            const char *n = isl_space_get_dim_name(dim, t, p);
            if (n && !strncmp(n, name, len))
                count++;
        }
    }
    return count;
}

static CloogUnionDomain *add_domain(__isl_take isl_set *set, CloogUnionDomain *ud) {
    int i, nvar;
    isl_ctx *ctx;
    isl_space *dim;
    char buffer[20];
    const char *name;
    CloogDomain *domain;

    ctx = isl_set_get_ctx(set);
    dim = isl_set_get_space(set);
    name = isl_space_get_tuple_name(dim, isl_dim_set);
    set = isl_set_flatten(set);
    set = isl_set_set_tuple_name(set, NULL);
    domain = cloog_domain_from_isl_set(set);
    ud = cloog_union_domain_add_domain(ud, name, domain, NULL, NULL);

    nvar = isl_space_dim(dim, isl_dim_set);
    for (i = 0; i < nvar; ++i) {
        char *long_name = NULL;
        int n;

        name = isl_space_get_dim_name(dim, isl_dim_set, i);
        if (!name) {
            snprintf(buffer, sizeof(buffer), "i%d", i);
            name = buffer;
        }
        n = count_same_name(dim, isl_dim_set, i, name);
        if (n) {
            int size = strlen(name) + 10;
            long_name = isl_alloc_array(ctx, char, size);
            if (!long_name)
                cloog_die("memory overflow.\n");
            snprintf(long_name, size, "%s_%d", name, n);
            name = long_name;
        }
        ud = cloog_union_domain_set_name(ud, CLOOG_ITER, i, name);
        free(long_name);
    }
    isl_space_free(dim);

    return ud;
}

/**
 * Construct a CloogUnionDomain from an isl_set.
 * The statement names are set to the names of the
 * spaces.  The parameter and iterator names of the result are set to those of
 * the input, but the scattering dimension names are left unspecified.
 */
CloogUnionDomain *cloog_union_domain_from_isl_set(
    __isl_take isl_set *set) {
    int i;
    int nparam;
    isl_space *dim;
    CloogUnionDomain *ud;

    dim = isl_set_get_space(set);
    nparam = isl_space_dim(dim, isl_dim_param);

    ud = cloog_union_domain_alloc(nparam);

    for (i = 0; i < nparam; ++i) {
        const char *s = isl_space_get_dim_name(dim, isl_dim_param, i);
        ud = cloog_union_domain_set_name(ud, CLOOG_PARAM, i, s);
    }
    isl_space_free(dim);

    ud = add_domain(set, ud);

    return ud;
}

/* Computes x, y and g such that g = gcd(a,b) and a*x+b*y = g */
static void Euclid(cloog_int_t a, cloog_int_t b,
                   cloog_int_t *x, cloog_int_t *y, cloog_int_t *g) {
    cloog_int_t c, d, e, f, tmp;

    cloog_int_init(c);
    cloog_int_init(d);
    cloog_int_init(e);
    cloog_int_init(f);
    cloog_int_init(tmp);
    cloog_int_abs(c, a);
    cloog_int_abs(d, b);
    cloog_int_set_si(e, 1);
    cloog_int_set_si(f, 0);
    while (cloog_int_is_pos(d)) {
        cloog_int_tdiv_q(tmp, c, d);
        cloog_int_mul(tmp, tmp, f);
        cloog_int_sub(e, e, tmp);
        cloog_int_tdiv_q(tmp, c, d);
        cloog_int_mul(tmp, tmp, d);
        cloog_int_sub(c, c, tmp);
        cloog_int_swap(c, d);
        cloog_int_swap(e, f);
    }
    cloog_int_set(*g, c);
    if (cloog_int_is_zero(a))
        cloog_int_set_si(*x, 0);
    else if (cloog_int_is_pos(a))
        cloog_int_set(*x, e);
    else cloog_int_neg(*x, e);
    if (cloog_int_is_zero(b))
        cloog_int_set_si(*y, 0);
    else {
        cloog_int_mul(tmp, a, *x);
        cloog_int_sub(tmp, c, tmp);
        cloog_int_divexact(*y, tmp, b);
    }
    cloog_int_clear(c);
    cloog_int_clear(d);
    cloog_int_clear(e);
    cloog_int_clear(f);
    cloog_int_clear(tmp);
}

/* Construct a CloogStride from the given constraint for the given level,
 * if possible.
 * We first compute the gcd of the coefficients of the existentially
 * quantified variables and then remove any common factors it has
 * with the coefficient at the given level.
 * The result is the value of the stride and if it is not one,
 * then it is possible to construct a CloogStride.
 * The constraint leading to the stride is stored in the CloogStride
 * as well a value (factor) such that the product of this value
 * and the coefficient at the given level is equal to -1 modulo the stride.
 */
static CloogStride *construct_stride(isl_constraint *c, int level) {
    int i, n, sign;
    isl_val *v, *m, *gcd, *stride;
    isl_val *v_copy, *m_copy, *gcd_copy;
    cloog_int_t c_v, c_m, c_gcd, c_stride, c_factor;
    CloogStride *s;
    isl_ctx *ctx = isl_constraint_get_ctx(c);;

    if (!c)
        return NULL;

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, level - 1);

    sign = isl_val_sgn(v);
    m = isl_val_abs(v); /* *takes* v. */

    gcd = isl_val_int_from_si(ctx, 0);
    n = isl_constraint_dim(c, isl_dim_div);
    for (i = 0; i < n; ++i) {
        v = isl_constraint_get_coefficient_val(c, isl_dim_div, i);
        gcd = isl_val_gcd(gcd, v);
    }

    m_copy = isl_val_copy(m);
    gcd_copy = isl_val_copy(gcd);

    v = isl_val_gcd(m, gcd);

    v_copy = isl_val_copy(v);
    gcd = isl_val_copy(gcd_copy);
    stride = isl_val_div(gcd, v);

    if (isl_val_is_zero(stride) || isl_val_is_one(stride))
        s = NULL;
    else {
        cloog_int_init(c_m);
        cloog_int_init(c_stride);
        cloog_int_init(c_v);
        cloog_int_init(c_gcd);
        cloog_int_init(c_factor);

        isl_val_to_cloog_int(m_copy, &c_m);
        isl_val_to_cloog_int(stride, &c_stride);
        isl_val_to_cloog_int(v_copy, &c_v);
        isl_val_to_cloog_int(gcd_copy, &c_gcd);

        Euclid(c_m, c_stride, &c_factor, &c_v, &c_gcd);
        if (sign > 0)
            cloog_int_neg(c_factor, c_factor);

        c = isl_constraint_copy(c);
        s = cloog_stride_alloc_from_constraint(c_stride,
                                               cloog_constraint_from_isl_constraint(c), c_factor);


        cloog_int_clear(c_m);
        cloog_int_clear(c_stride);
        cloog_int_clear(c_v);
        cloog_int_clear(c_gcd);
        cloog_int_clear(c_factor);
    }

    isl_val_free(stride);
    isl_val_free(gcd_copy);
    isl_val_free(m_copy);
    isl_val_free(v_copy);

    return s;
}

struct cloog_isl_find_stride_data {
    int level;
    CloogStride *stride;
};

/* Check if the given constraint can be used to derive
 * a stride on the iterator identified by data->level.
 * We first check that there are some existentially quantified variables
 * and that the coefficient at data->level is non-zero.
 * Then we call construct_stride for further checks and the actual
 * construction of the CloogStride.
 */
static int find_stride(__isl_take isl_constraint *c, void *user) {
    struct cloog_isl_find_stride_data *data;
    int n;
    isl_val *v;

    if (!isl_constraint_is_equality(c)) {
        isl_constraint_free(c);
        return 0;
    }

    data = (struct cloog_isl_find_stride_data *)user;

    if (data->stride) {
        isl_constraint_free(c);
        return 0;
    }

    n = isl_constraint_dim(c, isl_dim_div);
    if (n == 0) {
        isl_constraint_free(c);
        return 0;
    }

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, data->level - 1);
    if (!isl_val_is_zero(v))
        data->stride = construct_stride(c, data->level);

    isl_val_free(v);

    isl_constraint_free(c);

    return 0;
}

/* Check if the given list of domains has a common stride on the given level.
 * If so, return a pointer to a CloogStride object.  If not, return NULL.
 *
 * We project out all later variables, take the union and compute
 * the affine hull of the union.  Then we check the (equality)
 * constraints in this affine hull for imposing a stride.
 */
CloogStride *cloog_domain_list_stride(CloogDomainList *list, int level) {
    struct cloog_isl_find_stride_data data = { level, NULL };
    isl_set *set;
    isl_basic_set *aff;
    int first = level;
    int n;
    int r;

    set = isl_set_from_cloog_domain(list->domain);
    n = isl_set_dim(set, isl_dim_set) - first;
    set = isl_set_project_out(isl_set_copy(set), isl_dim_set, first, n);

    for (list = list->next; list; list = list->next) {
        isl_set *set_i = isl_set_from_cloog_domain(list->domain);
        n = isl_set_dim(set_i, isl_dim_set) - first;
        set_i = isl_set_project_out(isl_set_copy(set_i),
                                    isl_dim_set, first, n);
        set = isl_set_union(set, set_i);
    }
    aff = isl_set_affine_hull(set);

    r = isl_basic_set_foreach_constraint(aff, &find_stride, &data);
    assert(r == 0);

    isl_basic_set_free(aff);

    return data.stride;
}

struct cloog_can_unroll {
    int can_unroll;
    int level;
    isl_constraint *c;
    isl_set *set;
    isl_val *n;
};


/*
 * Check if the given lower bound can be used for unrolling
 * and, if so, return the unrolling factor/trip count in *v.
 * If the lower bound involves any existentially quantified
 * variables, we currently punt.
 * Otherwise we compute the maximal value of (i - ceil(l) + 1),
 * with l the given lower bound and i the iterator identified by level.
 */
static int is_valid_unrolling_lower_bound(struct cloog_can_unroll *ccu,
        __isl_keep isl_constraint *c, isl_val **v) {
    unsigned n_div;
    isl_aff *aff;
    enum isl_lp_result;

    n_div = isl_constraint_dim(c, isl_dim_div);
    if (isl_constraint_involves_dims(c, isl_dim_div, 0, n_div))
        return 0;

    aff = isl_constraint_get_bound(c, isl_dim_set, ccu->level - 1);
    aff = isl_aff_ceil(aff);
    aff = isl_aff_neg(aff);
    aff = isl_aff_add_coefficient_si(aff, isl_dim_in, ccu->level - 1, 1);
    *v = isl_set_max_val(ccu->set, aff);
    isl_aff_free(aff);

    if (!*v || isl_val_is_nan(*v))
        cloog_die("Fail to decide about unrolling (cannot find max)");

    if (isl_val_is_infty(*v) || isl_val_is_neginfty(*v)) {
        isl_val_free(*v);
        *v = NULL;
        return 0;
    }

    *v = isl_val_add_ui(*v, 1);

    return 1;
}


/* Check if we can unroll based on the given constraint.
 * Only lower bounds can be used.
 * Record it if it turns out to be usable and if we haven't recorded
 * any other constraint already.
 */
static int constraint_can_unroll(__isl_take isl_constraint *c, void *user) {
    struct cloog_can_unroll *ccu = (struct cloog_can_unroll *)user;
    isl_val *v;
    isl_val *count = NULL;

    v = isl_constraint_get_coefficient_val(c, isl_dim_set, ccu->level - 1);
    if (isl_val_is_pos(v) &&
            is_valid_unrolling_lower_bound(ccu, c, &count) &&
            (!ccu->c || (isl_val_lt(count, ccu->n))) ) {
        isl_constraint_free(ccu->c);
        ccu->c = isl_constraint_copy(c);
        if (ccu->n)
            isl_val_free(ccu->n);
        ccu->n = isl_val_copy(count);
    }
    isl_val_free(count);
    isl_val_free(v);
    isl_constraint_free(c);

    return 0;
}


/* Check if we can unroll the domain at the current level.
 * If the domain is a union, we cannot.  Otherwise, we check the
 * constraints.
 */
static int basic_set_can_unroll(__isl_take isl_basic_set *bset, void *user) {
    struct cloog_can_unroll *ccu = (struct cloog_can_unroll *)user;
    int r = 0;

    if (ccu->c || !ccu->can_unroll)
        ccu->can_unroll = 0;
    else {
        bset = isl_basic_set_remove_redundancies(bset);
        r = isl_basic_set_foreach_constraint(bset,
                                             &constraint_can_unroll, ccu);
    }
    isl_basic_set_free(bset);
    return r;
}


/* Check if we can unroll the given domain at the given level, and
 * if so, return the single lower bound in *lb and an upper bound
 * on the number of iterations in *n.
 * If we cannot unroll, return 0 and set *lb to NULL.
 *
 * We can unroll, if we can identify a lower bound on level
 * such that the number of iterations is bounded by a constant.
 */
int cloog_domain_can_unroll(CloogDomain *domain, int level, cloog_int_t *n,
                            CloogConstraint **lb) {
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_val *v = cloog_int_to_isl_val(isl_set_get_ctx(set), *n);
    struct cloog_can_unroll ccu = { 1, level, NULL, set, v };
    int r;

    *lb = NULL;
    r = isl_set_foreach_basic_set(set, &basic_set_can_unroll, &ccu);
    assert(r == 0);
    if (!ccu.c)
        ccu.can_unroll = 0;
    if (!ccu.can_unroll) {
        isl_constraint_free(ccu.c);
        return 0;
    }

    *lb = cloog_constraint_from_isl_constraint(ccu.c);

    isl_val_to_cloog_int(ccu.n, n);
    /* Note: we have to free ccu.n and not v because v has been
     * freed and replaced in ccu during isl_set_foreach_basic_set
     */
    isl_val_free(ccu.n);
    return ccu.can_unroll;
}


/* Fix the iterator i at the given level to l + o,
 * where l is prescribed by the constraint lb and o is equal to offset.
 * In particular, if lb is the constraint
 *
 *	a i >= f(j)
 *
 * then l = ceil(f(j)/a).
 */
CloogDomain *cloog_domain_fixed_offset(CloogDomain *domain,
                                       int level, CloogConstraint *lb, cloog_int_t offset) {
    isl_aff *aff;
    isl_set *set = isl_set_from_cloog_domain(domain);
    isl_ctx *ctx = isl_set_get_ctx(set);
    isl_constraint *c;
    isl_constraint *eq;

    c = cloog_constraint_to_isl(lb);
    aff = isl_constraint_get_bound(c, isl_dim_set, level - 1);
    aff = isl_aff_ceil(aff);
    aff = isl_aff_add_coefficient_si(aff, isl_dim_in, level - 1, -1);
    aff = isl_aff_add_constant_val(aff, cloog_int_to_isl_val(ctx, offset));
    eq = isl_equality_from_aff(aff);
    set = isl_set_add_constraint(set, eq);

    return cloog_domain_from_isl_set(set);
}
