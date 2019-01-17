/* This is a very simple example of how to use the PipLib inside your programs.
 * You should compile it by typing 'make' (after edition of the makefile), then
 * test it for instance by typing 'more FILE.pol | ./example'. Finally you can
 * compare results given by PIP by typing 'pip32 FILE.dat'
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>
#include <strings.h>

#include <piplib/piplib.h>

static PIPLIB_NAME(PipOptions) *options_read(FILE *f) {
    char s[1024];
    PIPLIB_NAME(PipOptions) *options = PIPLIB_NAME(pip_options_init)();
    while (fgets(s, 1024, f)) {
        if (strncasecmp(s, "Maximize", 8) == 0)
            options->Maximize = 1;
        if (strncasecmp(s, "Urs_parms", 9) == 0)
            options->Urs_parms = 1;
        if (strncasecmp(s, "Urs_unknowns", 12) == 0)
            options->Urs_unknowns = 1;
        if (strncasecmp(s, "Rational", 8) == 0)
            options->Nq = 0;
        if (strncasecmp(s, "Dual", 4) == 0)
            options->Compute_dual = 1;
    }
    return options;
}

int main(int argc, const char **argv) {
    int bignum ;
    PIPLIB_NAME(PipMatrix)  * domain, * context  ;
    PIPLIB_NAME(PipQuast)   * solution ;
    PIPLIB_NAME(PipOptions) * options ;
    int PIPLIB_NAME(verbose) = 0;

    while (argc > 1) {
        if (strncmp(argv[1], "-v", 2) == 0) {
            const char *v = argv[1]+2;
            ++PIPLIB_NAME(verbose);
            while (*v++ == 'v')
                ++PIPLIB_NAME(verbose);
        } else
            break;
        ++argv;
        --argc;
    }

    printf("[PIP2-like future input] Please enter:\n- the context matrix,\n") ;
    context = PIPLIB_NAME(pip_matrix_read)(stdin) ;
    PIPLIB_NAME(pip_matrix_print)(stdout,context) ;

    printf("- the bignum column (start at 0, -1 if no bignum),\n") ;
    if (fscanf(stdin, " %d", &bignum) != 1) {
        fprintf(stderr, "Cannot read the bignum option value\n");
        exit(1);
    }
    printf("%d\n",bignum) ;

    printf("- the constraint matrix.\n") ;
    domain = PIPLIB_NAME(pip_matrix_read)(stdin) ;
    PIPLIB_NAME(pip_matrix_print)(stdout,domain) ;
    printf("\n") ;

    if (isatty(0))
        printf("- options (EOF to stop).\n") ;
    options = options_read(stdin);
    options->Verbose = PIPLIB_NAME(verbose);
    if (isatty(0))
        PIPLIB_NAME(pip_options_print)(stdout, options);

    /* The bignum in PIP1 is fixed on the constraint matrix, here is
     * the translation.
     */
    if (bignum > 0)
        bignum += domain->NbColumns - context->NbColumns ;

    solution = PIPLIB_NAME(pip_solve)(domain,context,bignum,options) ;

    PIPLIB_NAME(pip_options_free)(options) ;
    PIPLIB_NAME(pip_matrix_free)(domain) ;
    PIPLIB_NAME(pip_matrix_free)(context) ;

    PIPLIB_NAME(pip_quast_print)(stdout,solution,0) ;

    PIPLIB_NAME(pip_quast_free)(solution) ;
    return 0 ;
}
