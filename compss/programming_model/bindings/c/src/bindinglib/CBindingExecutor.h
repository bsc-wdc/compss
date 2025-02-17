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
#include <AbstractCache.h>
#include <AbstractExecutor.h>
#include <pthread.h>
#include <customStream.h>
#include <iostream>

#ifndef C_EXECUTOR_H
#define C_EXECUTOR_H


using namespace std;


class CBindingExecutor: public AbstractExecutor {
  private:
    static string END_TASK_TAG;
    pthread_mutex_t mtx;
    customStream *csOut;
    customStream *csErr;
    int get_compss_worker_lock() {
        return pthread_mutex_lock(&mtx);
    };
    int release_compss_worker_lock() {
        return pthread_mutex_unlock(&mtx);
    };
  public:
    CBindingExecutor(AbstractCache *cache) : AbstractExecutor(cache) {
        pthread_mutex_init(&mtx,NULL);
        csOut = new customStream(cout.rdbuf());
        csErr = new customStream(cerr.rdbuf());
        cout.rdbuf(csOut);
        cerr.rdbuf(csErr);
    };
    void initThread();
    int executeTask(const char* args, char*& result);
    void finishThread();
    ~CBindingExecutor() {
        pthread_mutex_destroy(&mtx);
        delete(csOut);
        delete(csErr);
    };
};
#endif






