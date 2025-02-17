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
#include "AbstractCache.h"

#ifndef ABSTRACT_EXECUTOR_H
#define ABSTRACT_EXECUTOR_H


using namespace std;


class AbstractExecutor { 

  protected:
    AbstractCache *cache;

  public:
    AbstractExecutor(AbstractCache *cache) {
        this->cache = cache;
    };
    virtual void initThread() = 0;
    virtual int executeTask(const char* args, char*& result) = 0;
    virtual void finishThread() = 0;
    virtual ~AbstractExecutor() {
        delete(cache);
    };
};
#endif






