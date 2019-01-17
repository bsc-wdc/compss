/* Converts a SCoP from C to OpenScop using CLAN */

#include <stdlib.h>
#include <osl/osl.h>
#include <clan/clan.h>

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

int main(int argc, char* argv[]) {
  osl_scop_p scop;
  FILE* input;
  FILE* output;

  // Process arguments
  if (argc != 3) {
    fprintf(stderr, "usage: %s src.c output.scop\n", argv[0]);
    exit(0);
  }

  // Check open files
  input = fopen(argv[1], "r");
  if (input == NULL) {
    fprintf(stderr, "ERROR: Cannot open input file\n");
    exit(1);
  }
  
  output = fopen(argv[2], "w");
  if (output == NULL) {
    fprintf(stderr, "ERROR: Cannot open output file\n");
    exit(1);
  }

  // Process and transform
  scop = read_scop_from_c(input, argv[1]);
  osl_scop_print(output, scop);

  // Clean
  osl_scop_free(scop);  
  fclose(input);
  fclose(output);

  return 0;
}
