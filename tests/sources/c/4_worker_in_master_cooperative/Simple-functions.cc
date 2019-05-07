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

void checkFileWithContentMaster(
    char *content,
    file file_name
) { 
    checkFileWithContent(content, file_name);
}

void checkFileWithContentWorker(
    char *content,
    file file_name
) { 
    checkFileWithContent(content, file_name);
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

void createFileWithContentMaster(
    char *content,
    file file_name
) {
    createFileWithContent(content, file_name);
}

void createFileWithContentWorker(
    char *content,
    file file_name
) {
    createFileWithContent(content, file_name);
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

void checkAndUpdateFileWithContentMaster(
     char *content,
     char *new_content,
     file file_name
) { 
    checkAndUpdateFileWithContent(content, new_content, file_name);
}

void checkAndUpdateFileWithContentWorker(
     char *content,
     char *new_content,
     file file_name
) { 
    checkAndUpdateFileWithContent(content, new_content, file_name);
}

StringWrapper* createObjectWithContent(char* content) {
    StringWrapper* sw = new StringWrapper();
    sw->setValue(content);
    return sw;
}

StringWrapper* createObjectWithContentMaster(char* content) {
    createObjectWithContent(content);
}

StringWrapper* createObjectWithContentWorker(char* content) {
    createObjectWithContent(content);
}
    

void checkObjectWithContent(char* content, StringWrapper* sw) {
    string line = sw->getValue();
    if (validate_line(line.c_str(), content)){
        cout << "Task Failed" << "\n";
    }
}

void checkObjectWithContentMaster(char* content, StringWrapper* sw) {
    checkObjectWithContent(content, sw);
}

void checkObjectWithContentWorker(char* content, StringWrapper* sw) {
    checkObjectWithContent(content, sw);
}

void checkAndUpdateObjectWithContent(char* content, char* newContent, StringWrapper* sw) {
    string line = sw->getValue();
    if (validate_line(line.c_str(), content)){
        cout << "Task Failed" << "\n";
    }

    sw->setValue(newContent);
}

void checkAndUpdateObjectWithContentMaster(char* content, char* newContent, StringWrapper* sw) {
    checkAndUpdateObjectWithContent(content, newContent, sw);
}

void checkAndUpdateObjectWithContentWorker(char* content, char* newContent, StringWrapper* sw) {
    checkAndUpdateObjectWithContent(content, newContent, sw);
}