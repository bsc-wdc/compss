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

#include"Simple.h"
#include "StringWrapper.h"

using namespace std;

static int PARALLEL_TEST_COUNT = 20;
static int PARALLEL_TEST_MAX_COUNT = 4;


static char* INITIAL_CONTENT = "This is the initial content of the file";
static char* UPDATED_CONTENT_1 = "This is the updated content 1 of the file";

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
int masterProducerWorkerConsumerFile(){
    cout << "Master produces file, worker consumes" << "\n";
    
    file file_name = strdup("master_producer_worker_consumer");
    createFileWithContentMaster(INITIAL_CONTENT,file_name);
    checkFileWithContentWorker(INITIAL_CONTENT, file_name);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}

int workerProducerMasterConsumerFile(){
    cout << "Worker produces file, master consumes" << "\n";

    file file_name = strdup("worker_producer_master_consumer");
    createFileWithContentWorker(INITIAL_CONTENT, file_name);
    checkFileWithContentMaster(INITIAL_CONTENT, file_name);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}

int masterProducerWorkerConsumerMasterUpdatesFile(){
    cout << "Master produces file, several workers consume, master updates, worker reads" << "\n";
    
    file file_name = strdup("produce_consume_update");
    createFileWithContentMaster(INITIAL_CONTENT, file_name);
    for (int i = 0; i < PARALLEL_TEST_COUNT; i++) {
        checkFileWithContentWorker(INITIAL_CONTENT, file_name);
    }
    checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name);
    checkFileWithContentWorker(UPDATED_CONTENT_1, file_name);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}


int test_files(){
    int result;

    result = masterProducerWorkerConsumerFile();
    if ( result != 0 ) {
        return -1;
    }

    result = workerProducerMasterConsumerFile();
    if ( result != 0 ) {
        return -1;
    }

    result = masterProducerWorkerConsumerMasterUpdatesFile();
    if ( result != 0 ) {
        return -1;
    }
    return 0;
}


int masterProducerWorkerConsumerObject(){
    cout << "Master produces object, worker consumes" << "\n";

    StringWrapper* sw = createObjectWithContentMaster(INITIAL_CONTENT);
    checkObjectWithContentWorker(INITIAL_CONTENT, sw);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}

int workerProducerMasterConsumerObject(){
    cout << "Worker produces object, master consumes" << "\n";

    StringWrapper* sw = createObjectWithContentWorker(INITIAL_CONTENT);
    checkObjectWithContentMaster(INITIAL_CONTENT, sw);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}

int masterProducerWorkerConsumerMasterUpdatesObject(){
    cout << "Master produces object, several workers consume, master updates, worker reads" << "\n";

    StringWrapper* sw = createObjectWithContentMaster(INITIAL_CONTENT);
    for (int i = 0; i < PARALLEL_TEST_COUNT; i++) {
        checkObjectWithContentWorker(INITIAL_CONTENT, sw);
    }
    checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);
    checkObjectWithContentWorker(UPDATED_CONTENT_1, sw);

    compss_barrier();

    cout << "\t OK" << "\n";
    return 0;
}


int test_objects(){
    int result;

    result = masterProducerWorkerConsumerObject();
    if ( result != 0 ) {
        return -1;
    }

    result = workerProducerMasterConsumerObject();
    if ( result != 0 ) {
        return -1;
    }

    result = masterProducerWorkerConsumerMasterUpdatesObject();
    if ( result != 0 ) {
        return -1;
    }
    return 0;
}

int main(int argc, char *argv[]) {

    // Init compss
    compss_on();

    int result;
    
    result = test_files();
    if ( result != 0 ) {
        return -1;
    }
        
    // result = test_objects();
    if ( result != 0 ) {
        return -1;
    }
        

    // Close COMPSs and end
    compss_off();

    return 0;
}
