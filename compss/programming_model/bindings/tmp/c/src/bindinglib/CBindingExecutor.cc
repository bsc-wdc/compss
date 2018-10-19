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
#include "customStream.h"
#include <fcntl.h>
#include <common.h>
#include "CBindingExecutor.h"

#include "generated_executor.h"

#define gettid() syscall(SYS_gettid)

using namespace std;

string CBindingExecutor::END_TASK_TAG = "endTask";

void setAffinity(vector<string> commandArgs) {
    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Setting affinity to the assigned to the cores assigned by the runtime..." << endl << flush;
    }
    for (unsigned int i = 0; i < commandArgs.size(); i++) {
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
}

void CBindingExecutor::initThread() {
    if (is_debug()) {
        cout << "[Persistent C] Starting compute thread with id " << gettid() << endl << flush;
    }

    get_compss_worker_lock();
    GE_initThread();
    release_compss_worker_lock();

/*
#ifdef OMPSS_ENABLED
    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Admitting thread to OmpSs runtime..." << endl << flush;
    }
    get_compss_worker_lock();
    nanos_admit_current_thread();
    release_compss_worker_lock();
#endif
*/
}

int CBindingExecutor::executeTask(const char * command, char *&result) {
    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Command received. Reading arguments..." << endl << flush;
    }
    string aux;
    stringstream ss(command);
    vector<string> commandArgs;
    vector<string> executeArgs;
    while (ss >> aux) {
        commandArgs.push_back(aux);
    }
    for (unsigned int i = 0; i < commandArgs.size(); i++) {
        int pos = commandArgs[i].find("worker_c");
        if (pos != -1) {
            executeArgs = vector<string>(commandArgs.begin() + i, commandArgs.end());
        }
    }
    //Reading task number
    string task_num_str = commandArgs[1];
    cout << "[Persistent C] Executing task "<< task_num_str << "in thread " << gettid() << endl << flush;

    //Registering streams
    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Registering task output streams redirection..." << endl << flush;
    }
    ofstream * jobOut = new ofstream(commandArgs[2].c_str());
    ofstream * jobErr = new ofstream(commandArgs[3].c_str());
    csOut->registerThread(jobOut->rdbuf());
    csErr->registerThread(jobErr->rdbuf());
    setAffinity(commandArgs);

    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Starting task execution..." << endl << flush;
    }
    char**executeArgsC = new char*[executeArgs.size()];
    for (unsigned int i = 0; i < executeArgs.size(); i++) {
        executeArgsC[i] = new char[executeArgs[i].size() + 1];
        strcpy(executeArgsC[i], executeArgs[i].c_str());
    }

    /*DEBUG
    for (int i = 0; i < 5; i++){
    	cout << executeArgs[i+14] << endl;
    }*/
    fflush(NULL);
    //last integer indicates if output data is going to be serialized at the end of the task execution 0=no 1=yes
    int ret = execute(executeArgs.size(), executeArgsC, (CBindingCache*)this->cache, 0);

    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Task execution finished. Unregistering task output streams redirection..." << endl << flush;
    }
    csOut->unregisterThread();
    csErr->unregisterThread();

    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Closing task output files..." << endl << flush;
    }
    jobOut->close();
    jobErr->close();

    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() <<")] Writting result ..." << endl << flush;
    }
    ostringstream out_ss;
    out_ss << END_TASK_TAG << " " << task_num_str << " " << ret << flush;
    result = strdup(out_ss.str().c_str());

    cout << "[Persistent C (Th " << gettid() <<")] Task " << task_num_str << " finished (result: " << result <<")" << endl << flush;
    return ret;
}

void CBindingExecutor::finishThread() {
/*#ifdef OMPSS_ENABLED
    if (is_debug()) {
        cout << "[Persistent C (Th " << gettid() << ")] Leaving OmpSs runtime..."  << endl << flush;
    }
    get_compss_worker_lock();
    nanos_leave_team();
    nanos_expel_current_thread();
    release_compss_worker_lock();
#endif*/
    if (is_debug()) {
        cout << "[Persistent C] Thread " << gettid() << " Finished." << endl << flush;
    }

    get_compss_worker_lock();
    GE_finishThread();
    release_compss_worker_lock();

}



