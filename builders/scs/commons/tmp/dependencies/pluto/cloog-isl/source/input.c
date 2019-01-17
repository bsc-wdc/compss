#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include "../include/cloog/cloog.h"

#ifdef OSL_SUPPORT
#include <osl/scop.h>
#endif

#define ALLOC(type) (type*)malloc(sizeof(type))

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

#ifdef OSL_SUPPORT
/**
 * This function translates an OpenScop scop to a CLooG input.
 * \param[in,out] state CLooG state.
 * \param[in]     scop  Scop to translate.
 * \return A CloogInput corresponding to the scop input.
 */
CloogInput *cloog_input_from_osl_scop(CloogState *state, osl_scop_p scop) {
    CloogInput *input    = NULL;
    CloogDomain *context = NULL;
    CloogUnionDomain *ud = NULL;

    if (scop) {
        /* Extract the context. */
        context = cloog_domain_from_osl_relation(state, scop->context);

        /* Extract the union of domains. */
        ud = cloog_union_domain_from_osl_scop(state, scop);

        /* Build and return the input. */
        input = cloog_input_alloc(context, ud);
    }

    return input;
}
#endif

/**
 * Read input from a .cloog file, putting most of the information
 * in the returned CloogInput.  The chosen language is put in
 * options->language.
 */
CloogInput *cloog_input_read(FILE *file, CloogOptions *options) {
    char line[MAX_STRING];
    char language;
    CloogDomain *context;
    CloogUnionDomain *ud;
    int nb_par;

#ifdef OSL_SUPPORT
    if (options->openscop) {
        osl_scop_p scop = osl_scop_read(file);
        CloogInput * input = cloog_input_from_osl_scop(options->state,
                             scop);
        cloog_options_copy_from_osl_scop(scop, options);
        return input;
    }
#else
    if (options->openscop) {
        cloog_die("CLooG has not been compiled with OpenScop support.\n");
    }
#endif

    /* First of all, we read the language to use. */
    if (!next_line(file, line, sizeof(line)))
        cloog_die("Input error.\n");
    if (sscanf(line, "%c", &language) != 1)
        cloog_die("Input error.\n");

    if (language == 'f')
        options->language = CLOOG_LANGUAGE_FORTRAN;
    else if (language == 'p')
        options->language = CLOOG_LANGUAGE_PYTHON;
    else
        options->language = CLOOG_LANGUAGE_C;

    /* We then read the context data. */
    context = cloog_domain_read_context(options->state, file);
    nb_par = cloog_domain_parameter_dimension(context);

    ud = cloog_union_domain_read(file, nb_par, options);

    return cloog_input_alloc(context, ud);
}

/**
 * Create a CloogInput from a CloogDomain context and a CloogUnionDomain.
 */
CloogInput *cloog_input_alloc(CloogDomain *context, CloogUnionDomain *ud) {
    CloogInput *input;

    input = ALLOC(CloogInput);
    if (!input)
        cloog_die("memory overflow.\n");

    input->context = context;
    input->ud = ud;

    return input;
}

void cloog_input_free(CloogInput *input) {
    cloog_domain_free(input->context);
    cloog_union_domain_free(input->ud);
    free(input);
}

static void print_names(FILE *file, CloogUnionDomain *ud,
                        enum cloog_dim_type type, const char *name) {
    int i;

    fprintf(file, "\n%d # %s name(s)\n", ud->name[type] ? 1 : 0, name);
    if (!ud->name[type])
        return;

    for (i = 0; i < ud->n_name[type]; i++)
        fprintf(file, "%s ", ud->name[type][i]);

    fprintf(file, "\n");
}

/**
 * Dump the .cloog description of a CloogInput and a CloogOptions data structure
 * into a file. The generated .cloog file will contain the same information as
 * the data structures. The file can be used to run the cloog program on the
 * example.
 */
void cloog_input_dump_cloog(FILE *file, CloogInput *input, CloogOptions *opt) {
    int i, num_statements;
    CloogUnionDomain *ud = input->ud;
    CloogNamedDomainList *ndl = ud->domain;

    fprintf(file,
            "# CLooG -> CLooG\n"
            "# This is an automatic dump of a CLooG input file from a "
            "CloogInput data\n"
            "# structure.\n\n");

    /* Language. */
    if (opt->language == CLOOG_LANGUAGE_FORTRAN) {
        fprintf(file, "# Language: FORTRAN\n");
        fprintf(file, "f\n\n");
    } else if (opt->language == CLOOG_LANGUAGE_PYTHON) {
        fprintf(file, "# Language: PYTHON\n");
        fprintf(file, "p\n\n");
    } else {
        fprintf(file, "# Language: C\n");
        fprintf(file, "c\n\n");
    }

    /* Context. */
    fprintf(file, "# Context:\n");
    cloog_domain_print_constraints(file, input->context, 1);

    print_names(file, ud, CLOOG_PARAM, "Parameter");

    /* Statement number. */
    i = 0;
    while (ndl != NULL) {
        i++;
        ndl = ndl->next;
    }
    num_statements = i;
    fprintf(file, "\n# Statement number:\n%d\n\n", num_statements);

    /* Iteration domains. */
    i = 1;
    ndl = ud->domain;
    while (ndl != NULL) {
        fprintf(file, "# Iteration domain of statement %d (%s).\n", i,
                ndl->name);

        cloog_domain_print_constraints(file, ndl->domain, 1);
        fprintf(file,"\n0 0 0 # For future options.\n\n");

        i++;
        ndl = ndl->next;
    }

    print_names(file, ud, CLOOG_ITER, "Iterator");

    /* Exit, if no scattering is supplied. */
    if (!ud->domain || !ud->domain->scattering) {
        fprintf(file, "# No scattering functions.\n0\n\n");
        return;
    }

    /* Scattering relations. */
    fprintf(file,
            "# --------------------- SCATTERING --------------------\n");

    fprintf(file, "%d # Scattering functions\n", num_statements);

    i = 1;
    ndl = ud->domain;
    while (ndl != NULL) {
        fprintf(file, "\n# Scattering of statement %d (%s).\n", i,
                ndl->name);

        cloog_scattering_print_constraints(file, ndl->scattering);

        i++;
        ndl = ndl->next;
    }

    print_names(file, ud, CLOOG_SCAT, "Scattering dimension");
}
