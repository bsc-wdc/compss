#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>

#include "Test.h"
#include "Test-constants.h"

void char_size(char c, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} char parameter.......................");
	assert_worker(fp, c == TEST_VALUE_CHAR);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}


void string_size(char *s, file filename) {
	FILE *fp;
	printf("Running function with %s, %s\n",s, filename);
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} string parameter.......................");
	assert_worker(fp, strcmp(s, TEST_VALUE_STRING ) == 0);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void int_size(int i, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} int parameter........................");
	assert_worker(fp, i == TEST_VALUE_INT);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void short_size(short si, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp,"[TEST_FILE] Testing {in} short parameter......................");
	assert_worker(fp, si == TEST_VALUE_SHORT);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void long_size(long li, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} long parameter.......................");
	assert_worker(fp, li == TEST_VALUE_LONG);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void float_size(float f, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} float parameter......................");
	assert_worker(fp, f == TEST_VALUE_FLOAT);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void double_size(double d, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} double parameter.....................");
	assert_worker(fp, d == TEST_VALUE_DOUBLE);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}

void boolean_size(int b, file filename) {
	FILE *fp;
	fp = fopen(filename, "a");

	fprintf(fp, "[TEST_FILE] Testing {in} boolean parameter....................");
	assert_worker(fp, b == TEST_VALUE_BOOLEAN);
	fprintf(fp, "\tTEST PASSED.\n");

	fclose(fp);
}


void objects(student *st) {

	printf("Name: %s\n", st->name.c_str());
	printf("Surname: %s\n", st->surname.c_str());
	printf("Age: %d\n", st->age);

	printf("Domicile name: %s\n", st->domicile.name.c_str());

	st->name = "Ana";
	st->surname = "Suárez";
	st->age = 31;

}
