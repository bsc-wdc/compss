#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

// #define DEBUG_BINDING

#include "Test.h"
#include "Test-constants.h"


int main(int argc, char **argv)
{
	printf("*--------------------------------------------------------------------*\n");
	printf("*                                                                    *\n");
	printf("*     Test for COMP Superscalar C/C++ Binding...                     *\n");
	printf("*                                                                    *\n");
	printf("*     This test will test primitive types and c++ objects.           *\n");
	printf("*                                                                    *\n");
	printf("*     IMPORTANT: input parameters are tested in the worker           *\n");
	printf("*                so look in the job .err files.                      *\n");
	printf("*                                                                    *\n");
	printf("*--------------------------------------------------------------------*\n");
	printf("\n");

	FILE *fp;
	file filename = strdup(TEST_VALUE_FILE);

	compss_on();

	fp = fopen(filename, "w");
	fprintf(fp, "[TEST_FILE] TestFile content. Initialized from master \n");
	fclose(fp);

	// Log init test
        printf("[   TEST] Test of C types within COMPSs.\n");

	// Char (char_dt, wchar_dt)
	char c = TEST_VALUE_CHAR;
	char_size(c, filename);
	printf("[   TEST] Testing {in} char inside a task.\n");

	// String (string_dt)
	char *s = strdup(TEST_VALUE_STRING);
	string_size(s, filename);
	printf("[   TEST] Testing {in} string inside a task.\n");

	// Integer (int_dt)
	int i = TEST_VALUE_INT;
	int_size(i, filename);
	printf("[   TEST] Testing {in} int inside a task.\n");

	// Short (short_dt)
	short si = TEST_VALUE_SHORT;
	short_size(si, filename);
	printf("[   TEST] Testing {in} short inside a task.\n");

	// Long (long_dt)
	long li = TEST_VALUE_LONG;
	long_size(li, filename);
	printf("[   TEST] Testing {in} long inside a task.\n");

	// Float (float_dt)
	float f = TEST_VALUE_FLOAT;
	float_size(f, filename);
	printf("[   TEST] Testing {in} float inside a task.\n");

	// Double (double_dt)
	double d = TEST_VALUE_DOUBLE;
	double_size(d, filename);
	printf("[   TEST] Testing {in} double inside a task.\n");

	// Boolean (boolean_dt)
	int b = TEST_VALUE_BOOLEAN;
	boolean_size(b, filename);
	printf("[   TEST] Testing {in} boolean inside a task.\n");

	// File (file_dt)
	printf("[   TEST] Testing file synchronization at master.\n");
	string line;
	
	printf("[   TEST] Opening filename: %s.\n", filename);
	ifstream output;
	compss_ifstream(filename, output);
	int nlines = 0;
	if (output.is_open()) {
	    while ( getline (output, line) ) {
	      cout << line << endl;
	      nlines++;
	    }
	    output.close();
 	}
	else {
		cout << "[  TEST] Unable to open output file." << endl;
	}
        cout << "Lines " << nlines << " of " << TEST_VALUE_NLINES << endl;
	assert_master(nlines == TEST_VALUE_NLINES);


	//student (object_dt)
	student st;
	st.name = "Carlos";
	st.surname = "Díaz";
	st.age = 34;
	st.domicile.name = "Carrer de Sant Elm";
	st.domicile.number = 75;
	st.domicile.flat = 5;
	st.domicile.letter = 'B';
	objects(&st);

	printf("[   TEST] Testing {in-out} object parameter.................\n");
	compss_wait_on(st);
	assert_master(st.name == "Ana");
	assert_master(st.surname == "Suárez");
	assert_master(st.age == 31);
	printf("TEST PASSED.\n");

	compss_off();

	return 0;
}
