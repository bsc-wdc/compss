/* poc.c A complete C to polyhedra to C compiler */

#include <stdlib.h>
#include <osl/osl.h>
#include <clan/clan.h>
#include <cloog/cloog.h>

/* Use the Clan library to convert a SCoP from C to OpenScop */
osl_scop_p read_scop_from_c(FILE* input, char* input_name) {
    clan_options_p clanoptions;
    osl_scop_p scop;

    clanoptions = clan_options_malloc();
    clanoptions->precision = OSL_PRECISION_MP;
    CLAN_strdup(clanoptions->name, input_name);
    scop = clan_scop_extract(input, clanoptions);
    clan_options_free(clanoptions);
    return scop;
}

/* Use the CLooG library to output a SCoP from OpenScop to C */
void print_scop_to_c(FILE* output, osl_scop_p scop) {
    CloogState* state;
    CloogOptions* options;
    CloogInput* input;
    struct clast_stmt* clast;

    state = cloog_state_malloc();
    options = cloog_options_malloc(state);
    options->openscop = 1;
    cloog_options_copy_from_osl_scop(scop, options);
    input = cloog_input_from_osl_scop(options->state, scop);
    clast = cloog_clast_create_from_input(input, options);
    clast_pprint(output, clast, 0, options);

    cloog_clast_free(clast);
    options->scop = NULL; // don't free the scop
    cloog_options_free(options);
    cloog_state_free(state); // the input is freed inside
}

int main(int argc, char* argv[]) {
    osl_scop_p scop;
    FILE* input;

    if ((argc < 2) || (argc > 2)) {
        fprintf(stderr, "usage: %s file.c\n", argv[0]);
        exit(0);
    }

    if (argc == 1)
        input = stdin;
    else
        input = fopen(argv[1], "r");

    if (input == NULL) {
        fprintf(stderr, "cannot open input file\n");
        exit(0);
    }

    scop = read_scop_from_c(input, argv[1]);
    osl_scop_print(stdout, scop);

    // UPDATE THE SCOP IN A SMART WAY HERE

    print_scop_to_c(stdout, scop);
    osl_scop_free(scop);

    fclose(input);
    return 0;
}
