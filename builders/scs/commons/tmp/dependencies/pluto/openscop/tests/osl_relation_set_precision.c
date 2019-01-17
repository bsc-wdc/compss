// Copyright © 2014 Inria, Written by Lénaïc Bagnères, lenaic.bagneres@inria.fr

// (3-clause BSD license)
// Redistribution and use in source  and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <stdio.h>

#include <osl/relation.h>
#include <osl/macros.h>


int main(int argc, char** argv) {
    if (argc > 1) {
        printf("argv are ignored\n");
    }

    unsigned int nb_fail = 0;

#ifdef OSL_GMP_IS_HERE

    char * string =
        "SCATTERING\n"
        "11 14 9 2 0 1\n"
        "# e/i| c1   c2   c3   c4   c5   c6   c7   c8   c9 | i1    j |  n |  1\n"
        "   0   -1    0    0    0    0    0    0    0    0    0    0    0    1    ## c1 == 1\n"
        "   0    0    0    0    0    0   -1    0    0    0    1    0    0    0    ## c6 == i1\n"
        "   0    0    0   -1    0    0    0    0    0    0    0    0    0    0    ## c3 == 0\n"
        "   1    0  -32    0    0    0    1    0    0    0    0    0    0    0    ## -32*c2+c6 >= 0\n"
        "   0    0    0    0    0   -1    0    0    0    0    0    0    0    0    ## c5 == 0\n"
        "   1    0   32    0    0    0   -1    0    0    0    0    0    0   31    ## 32*c2-c6+31 >= 0\n"
        "   0    0    0    0    0    0    0   -1    0    0    0    0    0    0    ## c7 == 0\n"
        "   1    0    0    0  -32    0    0    0    1    0    0    0    0    0    ## -32*c4+c8 >= 0\n"
        "   0    0    0    0    0    0    0    0    0   -1    0    0    0    0    ## c9 == 0\n"
        "   1    0    0    0   32    0    0    0   -1    0    0    0    0   31    ## 32*c4-c8+31 >= 0\n"
        "   0    0    0    0    0    0    0    0   -1    0    0    1    0    0    ## c8 == j\n";
    char * * p_string = &string;

    osl_relation_p r0 = osl_relation_sread(p_string);
    osl_relation_p r1 = osl_relation_clone(r0);

    printf("r0 %d =\n", r0->precision);
    osl_relation_print(stdout, r0);
    printf("\n");

    osl_relation_set_precision(OSL_PRECISION_SP, r1);
    printf("r1 %d =\n", r1->precision);
    osl_relation_print(stdout, r1);
    osl_relation_set_same_precision(r0, r1);
    nb_fail += osl_relation_equal(r0, r1) ? 0 : 1;
    printf("nb fail = %d\n\n", nb_fail);

    osl_relation_set_precision(OSL_PRECISION_DP, r1);
    printf("r1 %d =\n", r1->precision);
    osl_relation_print(stdout, r1);
    osl_relation_set_same_precision(r0, r1);
    nb_fail += osl_relation_equal(r0, r1) ? 0 : 1;
    printf("nb fail = %d\n\n", nb_fail);

    osl_relation_set_precision(OSL_PRECISION_MP, r1);
    printf("r1 %d =\n", r1->precision);
    osl_relation_print(stdout, r1);
    osl_relation_set_same_precision(r0, r1);
    nb_fail += osl_relation_equal(r0, r1) ? 0 : 1;
    printf("nb fail = %d\n\n", nb_fail);

    osl_relation_free(r0);
    osl_relation_free(r1);

    printf("%s ", argv[0]);
    printf("fails = %d\n", nb_fail);

#else

    printf("%s ", argv[0]);
    printf("works only with GMP\n");

#endif

    return nb_fail;
}
