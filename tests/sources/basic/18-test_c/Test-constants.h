#ifndef _TEST_CONSTANTS_H
#define _TEST_CONSTANTS_H

#define TEST_VALUE_FILE "output.txt"
#define TEST_VALUE_CHAR 'A'
#define TEST_VALUE_CHAR2 'B'
#define TEST_VALUE_STRING "This sentence is for testing the string type."
#define TEST_VALUE_STRING2 "This string has been changed in the worker function!"
#define TEST_VALUE_INT 1000
#define TEST_VALUE_INT2 2000
#define TEST_VALUE_SHORT 10
#define TEST_VALUE_SHORT2 20
#define TEST_VALUE_LONG 1000000
#define TEST_VALUE_LONG2 2000000
#define TEST_VALUE_FLOAT 1000.001f
#define TEST_VALUE_FLOAT2 2000.002f
#define TEST_VALUE_DOUBLE 1000.001f
#define TEST_VALUE_DOUBLE2 2000.002f
#define TEST_VALUE_BOOLEAN 1
#define TEST_VALUE_BOOLEAN2 0
#define TEST_VALUE_NLINES 9

int assert_master (int condition) {
  if ( !condition ){
    printf("\tTEST FAILED.\n");
  }
  assert( condition );
  return 0;
}

int assert_worker (FILE *fp, int condition) {
  if ( !condition ){
    fprintf(fp, "\tTEST FAILED.\n");
    fclose(fp);
  }
  assert( condition );
  return 0;
}

#endif /* _TEST_CONSTANTS_H */
