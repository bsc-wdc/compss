/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
#include <sys/time.h>
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
#ifndef OSX
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
#endif
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
	string sandBox ;
    ofstream *jobOut, *jobErr;
	int th_id = gettid();
	struct timeval t_comp_start, t_comp_end, t_ex_end, t_read_end, t_aff_end, t_reg_end, t_cp_end;
	gettimeofday(&t_comp_start, NULL);
	if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Command received. Reading arguments..." << endl << flush;
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
    gettimeofday(&t_read_end, NULL);
    double read_msecs = (((t_read_end.tv_sec - t_comp_start.tv_sec) * 1000000) + (t_read_end.tv_usec - t_comp_start.tv_usec))/1000;

        //Reading task number
    string task_num_str = commandArgs[1];
    cout << "[Persistent C] Executing task "<< task_num_str << "in thread " << gettid() << endl << flush;
    //Registering streams
    if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Registering task output streams redirection..." << endl << flush;
    //}
        sandBox = commandArgs[2].c_str();
        jobOut = new ofstream(commandArgs[3].c_str());
        jobErr = new ofstream(commandArgs[4].c_str());
        csOut->registerThread(jobOut->rdbuf());
        csErr->registerThread(jobErr->rdbuf());
    }
    gettimeofday(&t_reg_end, NULL);
    double reg_msecs = (((t_reg_end.tv_sec - t_read_end.tv_sec) * 1000000) + (t_reg_end.tv_usec - t_read_end.tv_usec))/1000;

    setAffinity(commandArgs);

    gettimeofday(&t_aff_end, NULL);
    double aff_msecs = (((t_aff_end.tv_sec - t_reg_end.tv_sec) * 1000000) + (t_aff_end.tv_usec - t_reg_end.tv_usec))/1000;

    if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Starting task execution..." << endl << flush;
    }

    char**executeArgsC = new char*[executeArgs.size()];
    for (unsigned int i = 0; i < executeArgs.size(); i++) {
        executeArgsC[i] = new char[executeArgs[i].size() + 1];
        strcpy(executeArgsC[i], executeArgs[i].c_str());
    }

    gettimeofday(&t_cp_end, NULL);
    double cp_msecs = (((t_cp_end.tv_sec - t_aff_end.tv_sec) * 1000000) + (t_cp_end.tv_usec - t_aff_end.tv_usec))/1000;

    /*DEBUG
    for (int i = 0; i < 5; i++){
    	cout << executeArgs[i+14] << endl;
    }
    fflush(NULL);
    */

    //last integer indicates if output data is going to be serialized at the end of the task execution 0=no 1=yes
    int ret = execute(executeArgs.size(), executeArgsC, (CBindingCache*)this->cache, 0);
    if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Task execution finished. Unregistering task output streams redirection..." << endl << flush;
    }
    gettimeofday(&t_ex_end, NULL);
    double ex_msecs = (((t_ex_end.tv_sec - t_cp_end.tv_sec) * 1000000) + (t_ex_end.tv_usec - t_cp_end.tv_usec))/1000;
    if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Closing task output files..." << endl << flush;

        csOut->unregisterThread();
        csErr->unregisterThread();

        jobOut->close();
        jobErr->close();
    }

    if (is_debug()) {
        cout << "[Persistent C (Th " << th_id <<")] Writting result ..." << endl << flush;
    }
    ostringstream out_ss;
    out_ss << END_TASK_TAG << " " << task_num_str << " " << ret << flush;
    result = strdup(out_ss.str().c_str());
    gettimeofday(&t_comp_end, NULL);
    double unreg_msecs = (((t_comp_end.tv_sec - t_ex_end.tv_sec) * 1000000) + (t_comp_end.tv_usec - t_ex_end.tv_usec))/1000;
    double total_msecs = (((t_comp_end.tv_sec - t_comp_start.tv_sec) * 1000000) + (t_comp_end.tv_usec - t_comp_start.tv_usec))/1000;
    cout << "[Persistent C (Th " << th_id <<")] COMPSs task executor times: Total " << total_msecs << " readArgs " << read_msecs << " regStd " << reg_msecs << " setAff " << aff_msecs << " exTask " << ex_msecs << " unregStd " << unreg_msecs << endl << flush;
    cout << "[Persistent C (Th " << th_id <<")] Task " << task_num_str << " finished (result: " << result <<")" << endl << flush;
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



