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
#include <stdlib.h>

#include <osl/extensions/pluto_unroll.h>


int main(int argc, char** argv) {
    if (argc > 1) {
        printf("argv are ignored\n");
    }

    int nb_fail = 0;

    osl_pluto_unroll_p p0 = NULL;
    osl_pluto_unroll_p p1 = NULL;
    osl_pluto_unroll_p pc = NULL;

    // Free without malloc
    printf("// Free without malloc\n");
    osl_pluto_unroll_p p = NULL;
    osl_pluto_unroll_free(p);
    if (p != NULL) {
        nb_fail++;
    }
    printf("\n");

    // Malloc and free
    printf("// Malloc and free\n");
    p = osl_pluto_unroll_malloc();
    osl_pluto_unroll_dump(stdout, p);
    {
        char * s = osl_pluto_unroll_sprint(p);
        printf("%s\n", s);
        free(s);
        s = NULL;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    printf("\n");

    // Malloc, malloc and free
    printf("// Malloc, malloc and free\n");
    p = osl_pluto_unroll_malloc();
    p->next = osl_pluto_unroll_malloc();
    osl_pluto_unroll_dump(stdout, p);
    {
        char * s = osl_pluto_unroll_sprint(p);
        printf("%s\n", s);
        free(s);
        s = NULL;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    printf("\n");

    // osl_pluto_unroll_fill
    printf("// osl_pluto_unroll_fill\n");
    p = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p, "iterator_name_0", true, 4);
    p->next = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p->next, "iterator_name_1", false, 4);
    osl_pluto_unroll_dump(stdout, p);
    {
        char * s = osl_pluto_unroll_sprint(p);
        printf("%s\n", s);
        free(s);
        s = NULL;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    printf("\n");

    // osl_pluto_unroll_fill with NULL
    printf("// osl_pluto_unroll_fill with NULL\n");
    p = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p, NULL, true, 4);
    p->next = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p->next, "iterator_name_1", false, 4);
    osl_pluto_unroll_dump(stdout, p);
    {
        char * s = osl_pluto_unroll_sprint(p);
        printf("%s\n", s);
        free(s);
        s = NULL;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    printf("\n");

    // !=
    printf("// !=\n");
    p0 = osl_pluto_unroll_malloc();
    p0->next = osl_pluto_unroll_malloc();
    p0->next->next = osl_pluto_unroll_malloc();
    p1 = osl_pluto_unroll_malloc();
    p1->next = osl_pluto_unroll_malloc();
    if (osl_pluto_unroll_equal(p0, p1) == 1) {
        nb_fail++;
    }
    if (osl_pluto_unroll_equal(p0, p0) == 0) {
        nb_fail++;
    }
    if (osl_pluto_unroll_equal(p1, p1) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_free(p0);
    p0 = NULL;
    osl_pluto_unroll_free(p1);
    p1 = NULL;
    printf("\n");

    // ==
    printf("// ==\n");
    p0 = osl_pluto_unroll_malloc();
    p1 = osl_pluto_unroll_malloc();
    if (osl_pluto_unroll_equal(p0, p1) == 0) {
        nb_fail++;
    }
    if (osl_pluto_unroll_equal(p1, p0) == 0) {
        nb_fail++;
    }
    p0->next = osl_pluto_unroll_malloc();
    p1->next = osl_pluto_unroll_malloc();
    if (osl_pluto_unroll_equal(p0, p1) == 0) {
        nb_fail++;
    }
    if (osl_pluto_unroll_equal(p1, p0) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p0->next, "iterator_name_1", false, 4);
    if (osl_pluto_unroll_equal(p0, p1) == 1) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p1->next, "iterator_name_1", false, 4);
    if (osl_pluto_unroll_equal(p0, p1) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p0->next, "iterator_name_2", false, 4);
    if (osl_pluto_unroll_equal(p0, p1) == 1) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p1->next, "iterator_name_2", false, 4);
    if (osl_pluto_unroll_equal(p0, p1) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p1->next, "iterator_name_2", false, 8);
    if (osl_pluto_unroll_equal(p0, p1) == 1) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p0->next, "iterator_name_2", false, 8);
    if (osl_pluto_unroll_equal(p0, p1) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_fill(p0->next, "iterator_name_2", true, 8);
    if (osl_pluto_unroll_equal(p0, p1) == 1) {
        nb_fail++;
    }
    osl_pluto_unroll_free(p0);
    p0 = NULL;
    osl_pluto_unroll_free(p1);
    p1 = NULL;
    printf("\n");

    // Clone NULL
    printf("// Clone NULL\n");
    pc = osl_pluto_unroll_clone(p);
    printf("\n");

    // Clone
    printf("// Clone\n");
    p = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p, "iterator_name_0", true, 4);
    p->next = osl_pluto_unroll_malloc();
    p->next->next = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p->next->next, "iterator_name_2", false, 2);
    pc = osl_pluto_unroll_clone(p);
    osl_pluto_unroll_dump(stdout, p);
    osl_pluto_unroll_dump(stdout, pc);
    if (osl_pluto_unroll_equal(p, pc) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    osl_pluto_unroll_free(pc);
    pc = NULL;
    printf("\n");

    // osl_pluto_unroll_sread
    printf("// osl_pluto_unroll_sread\n");
    p = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p, "iterator_name_0", true, 4);
    p->next = osl_pluto_unroll_malloc();
    osl_pluto_unroll_fill(p->next, "iterator_name_1", false, 4);
    osl_pluto_unroll_dump(stdout, p);
    {
        char * s = osl_pluto_unroll_sprint(p);
        char * f = s;
        pc = osl_pluto_unroll_sread(&s);
        free(f);
        f = NULL;
        s = NULL;
    }
    osl_pluto_unroll_dump(stdout, pc);
    if (osl_pluto_unroll_equal(p, pc) == 0) {
        nb_fail++;
    }
    osl_pluto_unroll_free(p);
    p = NULL;
    osl_pluto_unroll_free(pc);
    pc = NULL;
    printf("\n");

    printf("%s ", argv[0]);
    printf("fails = %d\n", nb_fail);
    printf("\n");

    return nb_fail;
}
