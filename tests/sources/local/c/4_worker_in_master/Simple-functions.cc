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
#include<iostream>
#include<fstream>
#include<string>
#include <locale.h>
#include <unistd.h>
#include <sys/time.h>

#include"Simple.h"
#include"StringWrapper.h"
#include"Report.h"

using namespace std;

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

void testBasicTypes(
    file file_name,
    int b,
    char c,
    char* s,
    short sh,
    int i,
    long l,
    float f,
    double d) {

    setlocale(LC_NUMERIC, "es_ES.UTF-8");

	FILE *fp;
	fp = fopen(file_name, "w");

	fprintf(fp, "TEST BASIC TYPES\n");
    fprintf(fp, "- boolean: %s\n", b ? "true":"false");
    fprintf(fp, "- char: %c\n", c);
    fprintf(fp, "- string: %s\n", s);
    fprintf(fp, "- short: %hd\n", sh);
    fprintf(fp, "- int: %d\n", i);
    fprintf(fp, "- long: %lld\n", l);
    fprintf(fp, "- float: %f\n", f);
    fprintf(fp, "- double: %lf\n", d);
            
	fclose(fp);
           
}

void checkFileWithContent(
    char *content,
    file file_name
) { 
    ifstream ifs;
    ifs.open(file_name, std::ifstream::in);
    if (ifs.is_open()) {
        string line;
	    getline (ifs, line);
        if (validate_line(line.c_str(), content)){
            cout << "Task Failed" << "\n";
        }
    } else { 
        cout << "Task Failed" << "\n";
    }
}

void createFileWithContent(
    char *content,
    file file_name
) { 
    FILE *fp;
	fp = fopen(file_name, "w");
	fprintf(fp, content);
    fprintf(fp, "\n");          
	fclose(fp);
}


void checkAndUpdateFileWithContent(
     char *content,
     char *new_content,
     file file_name
) { 
    ifstream ifs;
    ifs.open(file_name, std::ifstream::in);
    if (ifs.is_open()) {
        string line;
        getline (ifs, line);
        if (validate_line(line.c_str(), content)){
            cout << "Task Failed" << "\n";
        }
    } else { 
        cout << "Task Failed" << "\n";
    }

    FILE *fp;
    fp = fopen(file_name, "w");
    fprintf(fp, new_content);
    fprintf(fp, "\n");          
    fclose(fp);
}

StringWrapper* createObjectWithContent(char* content) {
    StringWrapper* sw = new StringWrapper();
    sw->setValue(content);
    return sw;
}


void checkObjectWithContent(char* content, StringWrapper* sw) {
    string line = sw->getValue();
    if (validate_line(line.c_str(), content)){
        cout << "Task Failed" << "\n";
    }
}


void checkAndUpdateObjectWithContent(char* content, char* newContent, StringWrapper* sw) {
    string line = sw->getValue();
    if (validate_line(line.c_str(), content)){
        cout << "Task Failed" << "\n";
    }

    sw->setValue(newContent);
}

Report* sleepTask(){
    struct timeval tp;
    gettimeofday(&tp, NULL);
    long int ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    Report* r = new Report();
    r->set_start_time(ms);

    usleep(500);

    gettimeofday(&tp, NULL);
    ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    r->set_end_time(ms);
    return r;
}