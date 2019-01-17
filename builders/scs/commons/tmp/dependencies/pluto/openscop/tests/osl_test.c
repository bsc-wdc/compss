
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                            test.c                               **
 **-----------------------------------------------------------------**
 **                   First version: 01/10/2010                     **
 **-----------------------------------------------------------------**


*****************************************************************************
* OpenScop: Structures and formats for polyhedral tools to talk together    *
*****************************************************************************
*    ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,                *
*    /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,             *
*   /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\             *
*  |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \            *
*  | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\           *
*  | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\          *
*  | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \         *
*  | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \        *
*  | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \       *
*  | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \      *
*  | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \     *
*  | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \    *
*  | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \   *
*  | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ /   *
*  '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'    *
*                                                                           *
* Copyright (C) 2008 University Paris-Sud 11 and INRIA                      *
*                                                                           *
* (3-clause BSD license)                                                    *
* Redistribution and use in source  and binary forms, with or without       *
* modification, are permitted provided that the following conditions        *
* are met:                                                                  *
*                                                                           *
* 1. Redistributions of source code must retain the above copyright notice, *
*    this list of conditions and the following disclaimer.                  *
* 2. Redistributions in binary form must reproduce the above copyright      *
*    notice, this list of conditions and the following disclaimer in the    *
*    documentation and/or other materials provided with the distribution.   *
* 3. The name of the author may not be used to endorse or promote products  *
*    derived from this software without specific prior written permission.  *
*                                                                           *
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      *
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES *
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   *
* IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          *
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  *
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, *
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     *
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       *
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  *
* THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         *
*                                                                           *
* OpenScop Library, a library to manipulate OpenScop formats and data       *
* structures. Written by:                                                   *
* Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                         *
* Louis-Noel Pouchet <Louis-Noel.Pouchet@inria.fr>                          *
*                                                                           *
*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <osl/osl.h>

//#define FORK                 // Comment that if you want only one process
// (best for debugging with valgrind but bad
// for make check since any error will
// stop the job).

#define TEST_DIR    "."      // Directory to scan for OpenScop files
#define TEST_SUFFIX ".scop"  // Suffix of OpenScop files

/// Check if a scop without unions corresponds to the one with unions.
static int test_unions(osl_scop_p scop) {
    osl_statement_p stmt, nounion_stmt;
    osl_scop_p nounion_scop = osl_scop_remove_unions(scop);
    osl_scop_p nounion_scop_first = nounion_scop;
    osl_relation_p domain, scattering;
    int result = 1;
    osl_scop_print(stderr, nounion_scop);
    for ( ; scop != NULL && nounion_scop != NULL; scop = scop->next, nounion_scop = nounion_scop->next) {
        nounion_stmt = nounion_scop->statement;
        for (stmt = scop->statement; stmt != NULL; stmt = stmt->next) {
            domain = stmt->domain;
            scattering = stmt->scattering;
            do {
                do {
                    if (nounion_stmt == NULL)
                        return 0;
                    result = result && osl_relation_part_equal(domain, nounion_stmt->domain);
                    result = result && osl_relation_part_equal(scattering, nounion_stmt->scattering);
                    result = result && (nounion_stmt->domain == NULL || nounion_stmt->domain->next == NULL);
                    result = result && (nounion_stmt->scattering == NULL || nounion_stmt->scattering->next == NULL);
                    result = result && osl_relation_list_equal(nounion_stmt->access, stmt->access);
                    result = result && osl_generic_equal(nounion_stmt->extension, stmt->extension);
                    if (result == 0)
                        return 0;
                    nounion_stmt = nounion_stmt->next;

                    if (scattering == NULL || scattering->next == NULL)
                        break;
                    scattering = scattering->next;
                } while (1);
                if (domain == NULL || domain->next == NULL)
                    break;
                domain = domain->next;
            } while (1);
        }
        if (nounion_stmt != NULL)
            return 0;
    }
    if (scop != NULL || nounion_scop != NULL)
        return 0;
    osl_scop_free(nounion_scop_first);
    return 1;
}

/**
 * test_file function
 * This function tests an onpenscop file. A test has six steps:
 * 1. read the file to raise the data up to OpenScop data structures,
 * 2. clone the data structures,
 * 3. compare the clone and the original one,
 * 4. dump the data structures to a new OpenScop file,
 * 5. read the generated file,
 * 6. compare the data structures.
 * If everything went well, the data structure of the two scops are the same.
 * \param input_name The name of the input file.
 * \param verbose    Verbose option (1 to set, 0 not to set).
 * \return 1 if the test is successful, 0 otherwise.
 */
static int test_file(char* input_name, int verbose) {
    int cloning = 0;
    int dumping = 0;
    int equal   = 0;
    int unions  = 0;
    FILE* input_file;
    FILE* output_file;
    osl_scop_p input_scop;
    osl_scop_p output_scop;
    osl_scop_p cloned_scop;

    printf("\nTesting file %s... \n", input_name);

    // PART I. Raise from file.
    input_file = fopen(input_name, "r");
    if (input_file == NULL) {
        fflush(stdout);
        fprintf(stderr, "\nError: unable to open file %s\n", input_name);
        exit(2);
    }
    input_scop = osl_scop_read(input_file);
    fclose(input_file);

    if (verbose)
        printf("- reading file succeeded\n");

    // PART II. Clone and test.
    cloned_scop = osl_scop_clone(input_scop);
    // Compare the two scops.
    if ((cloning = osl_scop_equal(input_scop, cloned_scop)))
        printf("- cloning succeeded\n");
    else
        printf("- cloning failed\n");

    // PART III. Dump to file and test.
    output_file = tmpfile();
    if (output_file == NULL)
        OSL_error("cannot open temporary output file for writing");
    //osl_scop_dump(stdout, input_scop);
    //osl_scop_print(stdout, input_scop);
    osl_scop_print(output_file, input_scop);

    // Raise the generated file to data structures.
    rewind(output_file);
    output_scop = osl_scop_read(output_file);
    //osl_scop_dump(stdout, output_scop);
    fclose(output_file);

    if (verbose) {
        printf("\n\n*************************************************\n\n");
        osl_scop_dump(stdout, output_scop);
        osl_scop_print(stdout, output_scop);
        printf("\n*************************************************\n\n");
    }

    // Compare the two scops.
    if ((dumping = osl_scop_equal(input_scop, output_scop)))
        printf("- dumping succeeded\n");
    else
        printf("- dumping failed\n");

    // PART V. Remove unions.
    unions = test_unions(input_scop);

    // PART IV. Report.
    if ((equal = (cloning + dumping + unions > 2) ? 1 : 0))
        printf("Success :-)\n");
    else
        printf("Failure :-(\n");

    // Save the planet.
    osl_scop_free(input_scop);
    osl_scop_free(cloned_scop);
    osl_scop_free(output_scop);

    return equal;
}


/**
 * OpenScop test program.
 * Usage: osl_test [-v] [osl_file]
 * This program scans a directory for openscop files and test each of them.
 * Optionnally the user can provide a file name to check this file only. A
 * verbose option is also provided to output more information during tests.
 */
int main(int argc, char* argv[]) {
    int total   = 0; // Total number of tests.
    int success = 0; // Number of successes.
    int verbose = 0; // 1 if the verbose option is set, 0 otherwise.
    int dirtest = 1; // 1 if we check a whole directory, 0 for a single file.
    int fileidx = 0; // Index of the file to check in argv (0 if none).
    size_t d_namlen;
    size_t suffix_length;
    DIR * dir;
    struct dirent * dp;

    // Process the command line information
    if (((argc > 1) && (!strcmp(argv[1], "-v"))) ||
            ((argc > 2) && (!strcmp(argv[2], "-v"))))
        verbose = 1;

    if ((argc > 3) || ((argc == 3) && (!verbose))) {
        fprintf(stderr, "usage: osl_test [-v] [osl_file]\n");
        exit(1);
    }

    if ((argc - verbose) > 1) {
        dirtest = 0;
        fileidx = (!strcmp(argv[1], "-v")) ? 2 : 1;
    }

    // Proceed with the test(s), either directory or single file
    if (dirtest) {
        suffix_length = strlen(TEST_SUFFIX);

        // For each file in the directory to check...
        dir = opendir(TEST_DIR);
        while ((dp = readdir(dir)) != NULL) {
            d_namlen = strlen(dp->d_name);
            // If the file has the convenient suffix...
            if ((d_namlen > suffix_length) &&
                    (!strcmp(dp->d_name+(d_namlen-suffix_length), TEST_SUFFIX))) {
                // Test it !
#ifdef FORK
                int report;
                if (!fork())
                    exit(test_file(dp->d_name, verbose) ? 0 : 1);
                wait(&report);
                if (!WEXITSTATUS(report))
                    success++;
#else
                success += test_file(dp->d_name, verbose);
#endif
                total++;
            }
        }
        closedir(dir);
    } else {
        success = test_file(argv[fileidx], verbose);
        total++;
    }

    printf("\n  +-----------------------+\n");
    printf("  | OpenScop Test Summary |\n");
    printf("  |-----------------------|\n");
    printf("  | total          %4d   |\n", total);
    printf("  | success(es)    %4d   |\n", success);
    printf("  | failure(s)     %4d   |\n", total - success);
    printf("  +-----------------------+\n\n");

    // Return 0 if all tests were successful, 1 otherwise.
    if (total - success)
        return 1;
    else
        return 0;
}
