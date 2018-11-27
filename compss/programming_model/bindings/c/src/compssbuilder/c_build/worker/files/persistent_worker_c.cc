/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <fstream>
#include <map>
#include <vector>
#include <sstream>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <iostream>
#include <customStream.h>
#include <fcntl.h>
#include <compss_worker_lock.h>
#include <CBindingCache.h>
#include "generated_executor.h"
#define gettid() syscall(SYS_gettid)

using namespace std;

CBindingCache *cache;

string EXECUTE_TASK_TAG = "EXECUTE_TASK";
string END_TASK_TAG = "END_TASK";
string ERROR_TASK_TAG = "ERROR_TASK";
string QUIT_TAG = "QUIT";
string REMOVE_TAG = "REMOVE";
string SERIALIZE_TAG = "SERIALIZE";


int endedThreads;

pthread_mutex_t mtx;

struct arg_t {
    char * inPipe;
    char * outPipe;
    char * inDataPipe;
    char * outDataPipe;
    customStream *csOut;
    customStream *csErr;
    int nThreads;
};

int get_compss_worker_lock() {
    return pthread_mutex_lock(&mtx);
}

int release_compss_worker_lock() {
    return pthread_mutex_unlock(&mtx);
}


//Reads a command when the other end of the pipe is written
string readline(const char* inPipe) {
    ifstream inFile;
    inFile.open( inPipe, ios::in);
    string command;
    getline(inFile,command);
    inFile.close();
    return command;
}


streambuf * redirect_error(const char * filenm, ofstream& filestr) {
    streambuf *newsb, *oldsb;
    filestr.open(filenm);
    oldsb = cerr.rdbuf();     // back up cout's streambuf
    newsb = filestr.rdbuf();       // get file's streambuf
    cerr.rdbuf(newsb);        // assign streambuf to cout
    return oldsb;
}


void restore_error(streambuf * oldsb, ofstream& filestr) {
    cerr.rdbuf(oldsb);        // restore cout's original streambuf
    filestr.close();
}



streambuf * redirect_output(const char * filenm, ofstream& filestr) {
    streambuf *newsb, *oldsb;
    filestr.open(filenm);
    oldsb = cout.rdbuf();     // back up cout's streambuf
    newsb = filestr.rdbuf();       // get file's streambuf
    cout.rdbuf(newsb);        // assign streambuf to cout
    return oldsb;
}


void restore_output(streambuf * oldsb, ofstream& filestr) {
    cout.rdbuf(oldsb);        // restore cout's original streambuf
    filestr.close();
}


void *runThread(void * arg) {

    cout << "[Persistent C] Start compute thread with id " << gettid() << endl << flush;

#ifdef OMPSS_ENABLED
    nanos_admit_current_thread();
    cout << "[Persistent C (Th " << gettid() <<")] Admitting thread to OmpSs runtime" << endl << flush;
#endif

    ofstream outFile;
    string command;
    string output;
    char *inPipe, *outPipe;
    char *inDataPipe, *outDataPipe;
    struct arg_t *args = (struct arg_t *)arg;

    inPipe = args->inPipe;
    outPipe = args->outPipe;
    inDataPipe = args->inDataPipe;
    outDataPipe = args->outDataPipe;
    customStream *csOut = args->csOut;
    customStream *csErr = args->csErr;
    int nThreads = args->nThreads;

    while(true) {
        cout << "[Persistent C (Th " << gettid() <<")] Waiting to read command..." << endl << flush;
        command = readline(inPipe);
        if (command != "") {
            if (command == QUIT_TAG) {
                cout << "[Persistent C (Th " << gettid() <<")] Quit received" << endl;
                break;
            }

            cout << "[Persistent C (Th " << gettid() <<")] Command received. Reading arguments..." << endl << flush;
            string aux;
            stringstream ss(command);
            vector<string> commandArgs;
            vector<string> executeArgs;
            char** executeArgsC;
            while (ss >> aux) {
                commandArgs.push_back(aux);
            }
            for (int i = 0; i < commandArgs.size(); i++) {
                int pos = commandArgs[i].find("worker_c");
                if (pos != -1) {
                    executeArgs = vector<string>(commandArgs.begin() + i, commandArgs.end());
                }
            }

            cout << "[Persistent C (Th " << gettid() <<")] Registering task output streams redirection..." << endl << flush;
            ofstream * jobOut = new ofstream(commandArgs[2].c_str());
            ofstream * jobErr = new ofstream(commandArgs[3].c_str());

            csOut->registerThread(jobOut->rdbuf());
            csErr->registerThread(jobErr->rdbuf());

            cout << "[Persistent C (Th " << gettid() <<")] Setting affinity to the assigned to the cores assigned by the runtime..." << endl << flush;
            for (int i = 0; i < commandArgs.size(); i++) {
                if (commandArgs[i] == "taskset") {
                    cpu_set_t to_assign;
                    CPU_ZERO(&to_assign);
                    string assignedCpuString = commandArgs[i+2];
                    vector<int>assignedCpus;
                    stringstream ss_cpus(assignedCpuString);
                    int cpu;
                    //Read integers from the list of cpus assigned, ignore commas
                    while (ss_cpus >> cpu) {
                        CPU_SET(cpu, &to_assign);
                        if (ss_cpus.peek() == ',') ss_cpus.ignore();
                    }

                    if(sched_setaffinity(gettid(), sizeof(cpu_set_t), &to_assign) < 0) {
                        cout << "[Persistent C(Th " << gettid() <<")] WARN: Error during sched_setaffinity call! Ignoring affinity." << endl << flush;
                    }
                }
            }

            cout << "[Persistent C (Th " << gettid() <<")] Starting task execution..." << endl << flush;
            executeArgsC = new char*[executeArgs.size()];
            for (int i = 0; i < executeArgs.size(); i++) {
                executeArgsC[i] = new char[executeArgs[i].size() + 1];
                strcpy(executeArgsC[i], executeArgs[i].c_str());
            }
            for (int i = 0; i < 5; i++) {
                cout << executeArgs[i+14] << endl;
            }
            //last integer indicates if output data is going to be serialized at the end of the task execution 0=no 1=yes
            int ret = execute(executeArgs.size(), executeArgsC, cache, 0);

            cout << "[Persistent C (Th " << gettid() <<")] Task execution finished. Unregistering task output streams redirection..." << endl << flush;
            csOut->unregisterThread();
            csErr->unregisterThread();

            cout << "[Persistent C (Th " << gettid() <<")] Closing task output files..." << endl << flush;
            jobOut->close();
            jobErr->close();

            cout << "[Persistent C (Th " << gettid() <<")] Writting result to the result pipe..." << endl << flush;
            ostringstream out_ss;
            out_ss << END_TASK_TAG << " " << commandArgs[1] << " " << ret << endl;
            output = out_ss.str();
            outFile.open(outPipe);
            outFile << output << flush;
            //fflush(NULL);
            outFile.close();
            cout << "[Persistent C (Th " << gettid() <<")] Task processing finished." << endl << flush;
        } else {
            cout << "[Persistent C (Th " << gettid() <<")] Command is empty!." << endl << flush;
        }
    }
    cout << "[Persistent C (Th " << gettid() << ")] Finalizing compute thread " << endl << flush;

#ifdef OMPSS_ENABLED
    cout << "[Persistent C (Th " << gettid() << ")] Leaving OmpSs runtime..."  << endl << flush;
    get_compss_worker_lock();
    nanos_leave_team();
    nanos_expel_current_thread();
    release_compss_worker_lock();
#endif
    cout << "[Persistent C] Thread " << gettid() << " Finished." << endl << flush;
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 6) {
        printf("ERROR: Incorrect number of COMPSs internal parameters\n");
        printf("Aborting...\n");
        return -1;
    }
    //Data pipes
    char* inDataPipe = argv[1];
    char* outDataPipe = argv[2];
    //Reading in pipes
    int numInPipes=atoi(argv[3]);
    printf("[Persistent C] Detected %d in pipes.\n", numInPipes);
    char* inPipes[numInPipes];
    for (int i=0; i<numInPipes; i++) {
        inPipes[i] = argv[i+4];
    }
    //Reading out pipes
    int numOutPipes=atoi(argv[numInPipes+4]);
    printf("[Persistent C] Detected %d out pipes.\n", numOutPipes);
    char* outPipes[numOutPipes];
    for (int i=0; i<numOutPipes; i++) {
        outPipes[i] = argv[i+numInPipes+5];
    }

    fflush(NULL);

    //maybe not need
    pthread_mutex_init(&mtx,NULL);
    endedThreads = 0;
    cache = new CBindingCache();
    //Creating custom streams
    streambuf* outbuf = cout.rdbuf();
    streambuf* errbuf = cerr.rdbuf();
    customStream *csOut = new customStream(cout.rdbuf());
    customStream *csErr = new customStream(cerr.rdbuf());
    cout.rdbuf(csOut);
    cerr.rdbuf(csErr);

    cout << "[Persistent C] Creating " << numInPipes << " compute threads..."<< endl <<flush;
    pthread_t threadpool[numInPipes];
    arg_t arguments[numInPipes];
    for (int i = 0; i < numInPipes; i++) {
        arguments[i].inPipe = inPipes[i];
        arguments[i].outPipe = outPipes[i];
        arguments[i].inDataPipe = inDataPipe;
        arguments[i].outDataPipe = outDataPipe;
        arguments[i].csOut = csOut;
        arguments[i].csErr = csErr;
        arguments[i].nThreads = numInPipes;
        if(pthread_create(&threadpool[i], NULL, runThread, &arguments[i])) {
            fprintf(stderr, "Error creating thread\n");
            return 1;
        }
    }

    cout << "[Persistent C] Starting data command mangement..."<< endl << flush;
    string cmd;
    while (true) {
        cout << "[Persistent C] Waiting for data commands..."<< endl << flush;
        cmd = readline(inDataPipe);
        if (cmd != "") {
            if (cmd == QUIT_TAG) {
                cout << "[Persistent C] Quit received as data command" << endl << flush;
                break;
            }
            string aux;
            stringstream ss(cmd);
            vector<string> dataArgs;
            while (ss >> aux) {
                dataArgs.push_back(aux);
            }
            if (dataArgs[0] == REMOVE_TAG) {
                cout << "[Persistent C] Manging remove data command..." << endl;
                AbstractCache::removeData(dataArgs[1].c_str(), cache);
            }
            if (dataArgs[0] == SERIALIZE_TAG) {
                cout << "[Persistent C] Manging serialize data command..." << endl;
                int result = AbstractCache::serializeData(dataArgs[1].c_str(), dataArgs[2].c_str(), cache);

                ofstream outFile;
                ostringstream out_ss;
                if (result == 0) {
                    out_ss << true << endl;
                } else {
                    out_ss << false << endl;
                }
                string output = out_ss.str();
                cout << "[Persistent C] Returning "<< output <<" to data result pipe..." << endl << flush;
                outFile.open(outDataPipe);
                outFile << output << flush;
                //fflush(NULL);
                outFile.close();
            }
        } else {
            cout << "[Persistent C] Empty data command!" << endl;
        }
    }
    cout << "[Persistent C] Waiting for compute threads to end..." << endl << flush;
    for (int i = 0; i < numInPipes; i++) {
        if(pthread_join(threadpool[i], NULL)) {
            fprintf(stderr, "Error joining thread\n");
            return 1;
        }
    }
    cout << "[Persistent C] Restoring default streams..." << endl << flush;
    cout.rdbuf(outbuf);
    cerr.rdbuf(errbuf);

    //nanos_expel_current_thread();

    cout << "[Persistent C] Worker shutting down" << endl;
    return 0;
}
