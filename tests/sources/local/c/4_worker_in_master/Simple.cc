/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <algorithm>
#include<iostream>
#include<fstream>
#include<string>
#include <locale.h>

#include"Simple.h"
#include "StringWrapper.h"

using namespace std;

static int PARALLEL_TEST_COUNT = 20;
static int PARALLEL_TEST_MAX_COUNT = 4;


static char* INITIAL_CONTENT = "This is the initial content of the file";
static char* UPDATED_CONTENT_1 = "This is the updated content 1 of the file";
static char* UPDATED_CONTENT_2 = "This is the updated content 2 of the file";
static char* UPDATED_CONTENT_3 = "This is the updated content 3 of the file";
static char* UPDATED_CONTENT_4 = "This is the updated content 4 of the file";
static char* UPDATED_CONTENT_5 = "This is the updated content 5 of the file";

int validate_line(const char* obtained, const char* expected) {
    if (obtained){
        if (! expected){
            cerr << "Expecting:\n***" << expected <<"***\n and obtained:\n***" << obtained << "***\n";
            return -1;
        } else {
            if (strcmp(obtained, expected)) {
                cerr << "Expecting:\n***" << expected <<"***\n and obtained:\n***" << obtained << "***\n";
                return -1;
            }
        }
    }else{
        if (!expected){
            cerr << "Expecting:\n***" << expected <<"***\n and obtained:\n***" << obtained << "***\n";
            return -1;
        }
    }
    return 0;
}


int test_basic_types() {
    setlocale(LC_NUMERIC, "es_ES.UTF-8");
    // Run basic types test
    cout << "Testing basic types" << "\n";

	file filename = strdup("/tmp/basic_types_file");
    int b = true;
    char c = 'E';
    char* s = "My Test";
    short sh = 77;
    int i = 777;
    long l = 7777;
    float f = 7.7f;
    double d = 7.77777d;

    testBasicTypes(filename, b, c, s, sh, i, l, f, d);

    ifstream output;
	compss_ifstream(filename, output);

	if (output.is_open()) {
        string line;
        char expected_string[100];
	    getline (output, line);
        if (validate_line(line.c_str(), "TEST BASIC TYPES")){
            output.close();
            return -1;
        }

        sprintf(expected_string, "- boolean: %s", b ? "true":"false");
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- char: %c", c);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }
        
        sprintf(expected_string, "- string: %s", s);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- short: %hd", sh);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- int: %d", i);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- long: %ld", l);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- float: %f", f);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }

        sprintf(expected_string, "- double: %lf", d);
	    getline (output, line);
        if (validate_line(line.c_str(),expected_string)) {
            output.close();
            return -1;
        }
	    output.close();
 	} else {
        output.close() ;
        return -1;
    }

    cout << "\t OK" << "\n";
    return 0;
}


int mainToTaskTest(){
    cout << "Creating file on main and using it on task" << "\n";

    file file_name = strdup("main_to_task_file");
	FILE *fp;
	fp = fopen(file_name, "w");
	fprintf(fp, INITIAL_CONTENT);
    fprintf(fp, "\n");          
	fclose(fp);

    checkFileWithContent(INITIAL_CONTENT, file_name);

    compss_barrier();
    // new File(fileName).delete();

    cout << "\t OK" << "\n";
    return 0;
}

int taskToMainTest(){
    cout << "Creating file on task and using it on main" << "\n";
    file file_name = strdup("task_to_main_file");
    createFileWithContent(INITIAL_CONTENT, file_name);
 
    ifstream output;
	compss_ifstream(file_name, output);

	if (output.is_open()) {
        string line;
        char expected_string[100];
	    getline (output, line);
        if (validate_line(line.c_str(), INITIAL_CONTENT)){
            output.close();
            return -1;
        }
        output.close();
    } else {
        output.close();
        return -1;
    }

    compss_barrier();
    //new File(fileName).delete();

    cout << "\t OK" << "\n";
    return 0;
}

int fileDependenciesTest(){
    cout << "Testing file dependencies" << "\n";
    file file_name = "dependencies_file_1";

    createFileWithContent(INITIAL_CONTENT, file_name);
    checkFileWithContent(INITIAL_CONTENT, file_name);
    checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name);

    ifstream output;
    compss_ifstream(file_name, output);

    if (output.is_open()) {
        string line;
        char expected_string[100];
        getline (output, line);
        if (validate_line(line.c_str(), UPDATED_CONTENT_1)){
            output.close();
            return -1;
        }
        output.close();
    } else {
        output.close();
        return -1;
    }

    compss_barrier();

    //new File(fileName).delete();
    cout << "\t OK" << "\n";
    return 0;
}

int fileDependenciesTestComplex() {
    cout << "Testing file dependencies - Complex Version" << "\n";
    file file_name = "dependencies_file_1";

    createFileWithContent(INITIAL_CONTENT, file_name);
    checkFileWithContent(INITIAL_CONTENT, file_name);
    checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name);

    ifstream input;
    compss_ifstream(file_name, input);

    if (input.is_open()) {
        string line;
        getline (input, line);
        if (validate_line(line.c_str(), UPDATED_CONTENT_1)){
            input.close();
            return -1;
        }
        input.close();
    } else {
        return -1;
    }
    
    // Update File Content on Main
    ofstream output;
    compss_ofstream(file_name, output);
    if (output.is_open()) {
        output.write(UPDATED_CONTENT_2,(unsigned)strlen(UPDATED_CONTENT_2));
        output.close();
    }
        

    // Verify File update on Main
    compss_ifstream(file_name, input);
    if (input.is_open()) {
        string line;
        getline (input, line);
        if (validate_line(line.c_str(), UPDATED_CONTENT_2)){
            input.close();
            return -1;
        }
        input.close();
    } else {
        return -1;
    }

    checkFileWithContent(UPDATED_CONTENT_2, file_name);

    checkAndUpdateFileWithContent(UPDATED_CONTENT_2, UPDATED_CONTENT_3, file_name);
    checkFileWithContent(UPDATED_CONTENT_3, file_name);
    checkAndUpdateFileWithContent(UPDATED_CONTENT_3, UPDATED_CONTENT_4, file_name);
    checkAndUpdateFileWithContent(UPDATED_CONTENT_4, UPDATED_CONTENT_5, file_name);

    compss_ifstream(file_name, input);
    if (input.is_open()) {
        string line;
        getline (input, line);
        if (validate_line(line.c_str(), UPDATED_CONTENT_5)){
            input.close();
            return -1;
        }
        input.close();
    } else {
        return -1;
    }

    compss_barrier();

    //new File(fileName).delete();
    cout << "\t OK" << "\n";
    return 0;
}

int test_files(){
    int result;

    result = mainToTaskTest();
    if ( result != 0 ) {
        return -1;
    }

    result = taskToMainTest();
    if ( result != 0 ) {
        return -1;
    }

    result = fileDependenciesTest();
    if ( result != 0 ) {
        return -1;
    }

    result = fileDependenciesTestComplex();
    if ( result != 0 ) {
        return -1;
    }

    return 0;
}

int objectDependenciesTest(){
    cout << "Testing object dependencies" << "\n";

    StringWrapper* sw = createObjectWithContent(INITIAL_CONTENT);
    checkObjectWithContent(INITIAL_CONTENT, sw);
    checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);
    string line;
    compss_wait_on(sw);
    line = sw->getValue();
    if (validate_line(line.c_str(), UPDATED_CONTENT_1)){
        return -1;
    }

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}

int objectDependenciesTestComplex(){
    cout << "Testing object dependencies - Complex Version" << "\n";

    StringWrapper* sw = createObjectWithContent(INITIAL_CONTENT);
    checkObjectWithContent(INITIAL_CONTENT, sw);
    checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);

    compss_wait_on(sw);
    string line = sw->getValue();
    if (validate_line(line.c_str(), UPDATED_CONTENT_1)){
        return -1;
    }

    // Update object Content on Main
    sw->setValue(UPDATED_CONTENT_2);

    // Verify object update on Main
    line = sw->getValue();
    if (validate_line(line.c_str(), UPDATED_CONTENT_2)){
        return -1;
    }

    // Verify Object content on task
    checkObjectWithContent(UPDATED_CONTENT_2, sw);

    // Update value on task
    checkAndUpdateObjectWithContent(UPDATED_CONTENT_2, UPDATED_CONTENT_3, sw);
    // Check proper value on task
    checkObjectWithContent(UPDATED_CONTENT_3, sw);
    // Update twice on tasks
    checkAndUpdateObjectWithContent(UPDATED_CONTENT_3, UPDATED_CONTENT_4, sw);
    checkAndUpdateObjectWithContent(UPDATED_CONTENT_4, UPDATED_CONTENT_5, sw);

    compss_wait_on(sw);
    // Verify object update on Main
    line = sw->getValue();
    if (validate_line(line.c_str(), UPDATED_CONTENT_5)){
        return -1;
    }

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}


int test_objects(){
        int result;

    result = objectDependenciesTest();
    if ( result != 0 ) {
        return -1;
    }

    result = objectDependenciesTestComplex();
    if ( result != 0 ) {
        return -1;
    }
    return 0;
}

struct WorkerModification {
    long int time;
    int modification;

    void set_time(long int time) {
        this->time = time;
    }

    long int get_time(){
        return this->time;
    }

    void set_modification(int mod) {
        this->modification = mod;
    }
    int get_modification(){
        return this->modification;
    }
};

bool modification_sorter(const WorkerModification& m1, const WorkerModification& m2){
    return m1.time < m2.time;
}

int test_concurrency(){
    Report* reports[20];
    for (int i=0;i<PARALLEL_TEST_COUNT;i++){
        reports[i] = sleepTask();
    }
    compss_barrier();

    
    WorkerModification modifications[PARALLEL_TEST_COUNT*2];
    for (int i=0;i<PARALLEL_TEST_COUNT;i++){
        compss_wait_on(reports[i]);
        WorkerModification wm_start;
        wm_start.set_time(reports[i]->get_start_time());
        wm_start.set_modification(1);
        modifications[i*2] = wm_start;

        WorkerModification wm_end;
        wm_end.set_time(reports[i]->get_end_time());
        wm_end.set_modification(-1);

        modifications[i*2+1] = wm_end;
    }

    std::sort(modifications, modifications+40, modification_sorter);
    for (int i=0;i<PARALLEL_TEST_COUNT*2;i++){
        cout << modifications[i].time << "\n";
    }


    return 0;
}


int main(int argc, char *argv[]) {

    // Init compss
    compss_on();

    int result;

    result = test_basic_types();
    if ( result != 0 ) {
        return -1;
    }
    
    result = test_files();
    if ( result != 0 ) {
        return -1;
    }
        
    result = test_objects();
    if ( result != 0 ) {
        return -1;
    }
        
    result = test_concurrency();
    if ( result != 0 ) {
        return -1;
    }

    // Close COMPSs and end
    compss_off();

    return 0;
}
