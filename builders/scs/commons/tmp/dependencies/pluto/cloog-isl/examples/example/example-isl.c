/* example-isl.c */
#include <cloog/cloog.h>
#include <cloog/isl/cloog.h>

/* Input problem */
int nb_parameters = 1;
char *parameter_name[]  = {"N"};
char *iterator_name[]   = {"i", "j"};
char *scattering_name[] = {"t0", "t1", "t2"};
char *str_context       = "[N] -> { : N > 0}";
char *str_domain1       = "[N] -> {[i, j] : 0 <= i < N and 0 <= j < N}";
char *str_domain2       = "[N] -> {[i, j] : 0 <= i < N and 0 <= j < N}";
char *str_scattering1   = "[N] -> {[i, j] -> [0, i + j, j]}";
char *str_scattering2   = "[N] -> {[i, j] -> [1, i,    -j]}";

int main() {
    isl_ctx *ctx;
    isl_set *set_context, *set1, *set2;
    isl_map *map1, *map2;
    CloogDomain *context, *domain1, *domain2;
    CloogScattering *scattering1, *scattering2;
    CloogUnionDomain *domains;
    CloogInput *input;
    CloogState *state;
    CloogOptions *options;
    struct clast_stmt *root;

    /* Build isl structures for context, sets and mapping */
    ctx = isl_ctx_alloc();
    set_context = isl_set_read_from_str(ctx, str_context);
    set1 = isl_set_read_from_str(ctx, str_domain1);
    set2 = isl_set_read_from_str(ctx, str_domain2);
    map1 = isl_map_read_from_str(ctx, str_scattering1);
    map2 = isl_map_read_from_str(ctx, str_scattering2);

    /* Translate them to CLooG context, domains and scattering */
    context = cloog_domain_from_isl_set(set_context);
    domain1 = cloog_domain_from_isl_set(set1);
    domain2 = cloog_domain_from_isl_set(set2);
    scattering1 = cloog_scattering_from_isl_map(map1);
    scattering2 = cloog_scattering_from_isl_map(map2);

    /* Prepare the list of domains to scan */
    domains = cloog_union_domain_alloc(nb_parameters);
    cloog_union_domain_add_domain(domains, "S1", domain1, scattering1, NULL);
    cloog_union_domain_add_domain(domains, "S2", domain2, scattering2, NULL);
    cloog_union_domain_set_name(domains, CLOOG_PARAM, 0, parameter_name[0]);
    cloog_union_domain_set_name(domains, CLOOG_ITER,  0, iterator_name[0]);
    cloog_union_domain_set_name(domains, CLOOG_ITER,  1, iterator_name[1]);
    cloog_union_domain_set_name(domains, CLOOG_SCAT,  0, scattering_name[0]);
    cloog_union_domain_set_name(domains, CLOOG_SCAT,  1, scattering_name[1]);
    cloog_union_domain_set_name(domains, CLOOG_SCAT,  2, scattering_name[2]);

    /* Build the input, generate the AST of the scanning code and print it */
    input = cloog_input_alloc(context, domains);
    state = cloog_isl_state_malloc(ctx);
    options = cloog_options_malloc(state);
    root = cloog_clast_create_from_input(input, options);
    clast_pprint(stdout, root, 0, options);

    /* Recycle allocated memory */
    cloog_clast_free(root);
    cloog_options_free(options);
    cloog_state_free(state);
    isl_ctx_free(ctx);
}
