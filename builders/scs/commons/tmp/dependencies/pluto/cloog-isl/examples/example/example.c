/* This is a very simple example of how to use the CLooGLib inside your
 * programs. You should compile it by typing 'make' (after edition of the
 * makefile), then test it for instance by typing
 * 'more FILE.cloog | ./example' (or example.exe under Cygwin).
 */

# include <stdio.h>
# include <cloog/cloog.h>

int main() {
    CloogState *state;
    CloogInput *input;
    CloogOptions * options ;
    struct clast_stmt *root;

    state = cloog_state_malloc();
    options = cloog_options_malloc(state);
    input = cloog_input_read(stdin, options);

    root = cloog_clast_create_from_input(input, options);
    clast_pprint(stdout, root, 0, options);

    cloog_clast_free(root);
    cloog_options_free(options) ;
    cloog_state_free(state);

    return 0 ;
}
