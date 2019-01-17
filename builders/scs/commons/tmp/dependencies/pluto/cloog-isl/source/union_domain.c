#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include "../include/cloog/cloog.h"

#ifdef OSL_SUPPORT
#include <osl/strings.h>
#include <osl/extensions/scatnames.h>
#include <osl/statement.h>
#include <osl/scop.h>
#endif

#define ALLOC(type) (type*)malloc(sizeof(type))
#define ALLOCN(type,n) (type*)malloc((n)*sizeof(type))

void cloog_named_domain_list_free(CloogNamedDomainList *list) {
    while (list != NULL) {
        CloogNamedDomainList *temp = list->next;
        cloog_domain_free(list->domain);
        cloog_scattering_free(list->scattering);
        free(list->name);
        free(list);
        list = temp;
    }
}

CloogUnionDomain *cloog_union_domain_alloc(int nb_par) {
    CloogUnionDomain *ud;

    ud = ALLOC(CloogUnionDomain);
    if (!ud)
        cloog_die("memory overflow.\n");

    ud->domain = NULL;
    ud->next_domain = &ud->domain;

    ud->n_name[CLOOG_PARAM] = nb_par;
    ud->n_name[CLOOG_ITER] = 0;
    ud->n_name[CLOOG_SCAT] = 0;

    ud->name[CLOOG_PARAM] = NULL;
    ud->name[CLOOG_ITER] = NULL;
    ud->name[CLOOG_SCAT] = NULL;

    return ud;
}

void cloog_union_domain_free(CloogUnionDomain *ud) {
    int i;
    int j;

    if (!ud)
        return;

    for (i = 0; i < 3; ++i) {
        if (!ud->name[i])
            continue;
        for (j = 0; j < ud->n_name[i]; ++j)
            free(ud->name[i][j]);
        free(ud->name[i]);
    }

    cloog_named_domain_list_free(ud->domain);

    free(ud);
}

/**
 * Add a domain with scattering function to the union of domains.
 * name may be NULL and is duplicated if it is not.
 * domain and scattering are taken over by the CloogUnionDomain.
 * scattering may be NULL.
 */
CloogUnionDomain *cloog_union_domain_add_domain(CloogUnionDomain *ud,
        const char *name, CloogDomain *domain, CloogScattering *scattering,
        void *usr) {
    CloogNamedDomainList *named;
    int n;

    if (!ud)
        return NULL;

    named = ALLOC(CloogNamedDomainList);
    if (!named)
        cloog_die("memory overflow.\n");

    if (ud->name[CLOOG_ITER])
        cloog_die("iterator names must be set after adding domains.\n");
    if (ud->name[CLOOG_SCAT])
        cloog_die("scattering names must be set after adding domains.\n");

    n = cloog_domain_dimension(domain);
    if (n > ud->n_name[CLOOG_ITER])
        ud->n_name[CLOOG_ITER] = n;

    if (scattering) {
        n = cloog_scattering_dimension(scattering, domain);
        if (n > ud->n_name[CLOOG_SCAT])
            ud->n_name[CLOOG_SCAT] = n;
    }

    named->domain = domain;
    named->scattering = scattering;
    named->name = name ? strdup(name) : NULL;
    named->usr = usr;
    named->next = NULL;

    *ud->next_domain = named;
    ud->next_domain = &named->next;

    return ud;
}

/**
 * Set the name of parameter, iterator or scattering dimension
 * at the specified position.  The name is duplicated.
 */
CloogUnionDomain *cloog_union_domain_set_name(CloogUnionDomain *ud,
        enum cloog_dim_type type, int index, const char *name) {
    int i;

    if (!ud)
        return ud;

    if (type != CLOOG_PARAM &&
            type != CLOOG_ITER &&
            type != CLOOG_SCAT)
        cloog_die("invalid dim type\n");

    if (index < 0 || index >= ud->n_name[type])
        cloog_die("index out of range\n");

    if (!ud->name[type]) {
        ud->name[type] = ALLOCN(char *, ud->n_name[type]);
        if (!ud->name[type])
            cloog_die("memory overflow.\n");
        for (i = 0; i < ud->n_name[type]; ++i)
            ud->name[type][i] = NULL;
    }

    free(ud->name[type][index]);
    ud->name[type][index] = strdup(name);
    if (!ud->name[type][index])
        cloog_die("memory overflow.\n");

    return ud;
}

static char *next_line(FILE *input, char *line, unsigned len) {
    char *p;

    do {
        if (!(p = fgets(line, len, input)))
            return NULL;
        while (isspace(*p) && *p != '\n')
            ++p;
    } while (*p == '#' || *p == '\n');

    return p;
}

/**
 * cloog_scattering_list_read
 * Read in a list of scattering functions for the nb_statements
 * domains in loop.
 */
static CloogScatteringList *cloog_scattering_list_read(FILE * foo,
        CloogDomain **domain, int nb_statements, int nb_parameters) {
    int nb_scat = 0;
    char s[MAX_STRING];
    CloogScatteringList *list = NULL, **next = &list;

    /* We read first the number of scattering functions in the list. */
    do {
        if (!fgets(s, MAX_STRING, foo))
            break;
    } while ((*s=='#' || *s=='\n') || (sscanf(s, " %d", &nb_scat) < 1));

    if (nb_scat == 0)
        return NULL;

    if (nb_scat != nb_statements)
        cloog_die("wrong number of scattering functions.\n");

    while (nb_scat--) {
        *next = (CloogScatteringList *)malloc(sizeof(CloogScatteringList));
        (*next)->scatt = cloog_domain_read_scattering(*domain, foo);
        (*next)->next = NULL;

        next = &(*next)->next;
        domain++;
    }
    return list;
}

static CloogUnionDomain *set_names_from_list(CloogUnionDomain *ud,
        enum cloog_dim_type type, int n, char **names) {
    int i;

    if (!names)
        return ud;

    for (i = 0; i < n; ++i) {
        ud = cloog_union_domain_set_name(ud, type, i, names[i]);
        free(names[i]);
    }
    free(names);

    return ud;
}

/**
 * Fill up a CloogUnionDomain from information in a CLooG input file.
 * The language and the context are assumed to have been read from
 * the input file already.
 */
CloogUnionDomain *cloog_union_domain_read(FILE *file, int nb_par,
        CloogOptions *options) {
    int op1, op2, op3;
    char line[MAX_STRING];
    CloogDomain **domain;
    CloogUnionDomain *ud;
    CloogScatteringList *scatteringl;
    int i;
    int n_iter = -1;
    int n_dom;
    char **names;

    ud = cloog_union_domain_alloc(nb_par);

    names = cloog_names_read_strings(file, nb_par);
    ud = set_names_from_list(ud, CLOOG_PARAM, nb_par, names);

    /* We read the number of statements. */
    if (!next_line(file, line, sizeof(line)))
        cloog_die("Input error.\n");
    if (sscanf(line, "%d", &n_dom) != 1)
        cloog_die("Input error.\n");

    domain = ALLOCN(CloogDomain *, n_dom);
    if (!domain)
        cloog_die("memory overflow.\n");

    for (i = 0; i < n_dom; ++i) {
        int dim;

        domain[i] = cloog_domain_union_read(options->state, file,
                                            nb_par);
        dim = cloog_domain_dimension(domain[i]);
        if (dim > n_iter)
            n_iter = dim;

        /* To read that stupid "0 0 0" line. */
        if (!next_line(file, line, sizeof(line)))
            cloog_die("Input error.\n");
        if (sscanf(line, " %d %d %d", &op1, &op2, &op3) != 3)
            cloog_die("Input error.\n");
    }

    /* Reading of the iterator names. */
    names = cloog_names_read_strings(file, n_iter);

    /* Reading and putting the scattering data in program structure. */
    scatteringl = cloog_scattering_list_read(file, domain, n_dom, nb_par);

    if (scatteringl) {
        CloogScatteringList *is, *next;

        if (cloog_scattering_list_lazy_same(scatteringl))
            cloog_msg(options, CLOOG_WARNING,
                      "some scattering functions are similar.\n");

        for (i = 0, is = scatteringl; i < n_dom; ++i, is = next) {
            next = is->next;
            ud = cloog_union_domain_add_domain(ud, NULL, domain[i],
                                               is->scatt, NULL);
            free(is);
        }
    } else {
        for (i = 0; i < n_dom; ++i)
            ud = cloog_union_domain_add_domain(ud, NULL, domain[i],
                                               NULL, NULL);
    }

    ud = set_names_from_list(ud, CLOOG_ITER, n_iter, names);

    if (scatteringl) {
        int n_scat = ud->n_name[CLOOG_SCAT];
        names = cloog_names_read_strings(file, n_scat);
        ud = set_names_from_list(ud, CLOOG_SCAT, n_scat, names);
    }

    free(domain);

    return ud;
}


#ifdef OSL_SUPPORT
/**
 * Extracts a CloogUnionDomain from an openscop scop (the CloogUnionDomain
 * corresponds more or less to the openscop statement).
 * \param[in,out] state CLooG state.
 * \param[in]     scop  OpenScop scop to convert.
 * \return A new CloogUnionDomain corresponding the input OpenScop scop.
 */
CloogUnionDomain *cloog_union_domain_from_osl_scop(CloogState *state,
        osl_scop_p scop) {
    int i, nb_parameters;
    CloogDomain *domain = NULL;
    CloogScattering *scattering = NULL;
    CloogUnionDomain *ud = NULL;
    osl_scop_p normalized;
    osl_statement_p statement;
    osl_scatnames_p scatnames;

    /* Set the union of domains. */
    nb_parameters = (scop->context == NULL) ? 0 : scop->context->nb_parameters;
    ud = cloog_union_domain_alloc(nb_parameters);

    /* - Set the parameter names. */
    if (osl_generic_has_URI(scop->parameters, OSL_URI_STRINGS)) {
        for (i = 0; i < osl_strings_size(scop->parameters->data); i++) {
            ud = cloog_union_domain_set_name(ud, CLOOG_PARAM, i,
                                             ((osl_strings_p)(scop->parameters->data))->string[i]);
        }
    }

    /* - Set each statement (domain/scattering).
     *   Since CLooG requires all number of scattering dimensions to be
     *   equal, we normalize them first.
     */
    normalized = osl_scop_clone(scop);
    osl_scop_normalize_scattering(normalized);
    statement = normalized->statement;
    while(statement != NULL) {
        domain = cloog_domain_from_osl_relation(state, statement->domain);
        scattering = cloog_scattering_from_osl_relation(state,
                     statement->scattering);
        ud = cloog_union_domain_add_domain(ud, NULL, domain, scattering, NULL);
        statement = statement->next;
    }
    osl_scop_free(normalized);

    /* - Set the scattering dimension names. */
    scatnames = osl_generic_lookup(scop->extension, OSL_URI_SCATNAMES);
    if ((scatnames != NULL) && (scatnames->names != NULL)) {
        for (i = 0; (i < osl_strings_size(scatnames->names)) &&
                (i < ud->n_name[CLOOG_SCAT]); i++) {
            ud = cloog_union_domain_set_name(ud, CLOOG_SCAT, i,
                                             scatnames->names->string[i]);
        }
    }

    return ud;
}
#endif
