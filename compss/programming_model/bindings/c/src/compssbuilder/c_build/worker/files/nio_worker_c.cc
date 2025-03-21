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
#include <CBindingExecutor.h>
#include <CBindingCache.h>
#include <compss_worker.h>

#ifdef OMPSS2_ENABLED
#include <pthread.h>
#include <nanos6/bootstrap.h>
#include <nanos6/library-mode.h>
#endif

int main(int argc, char **argv) {
    
#ifdef OMPSS2_ENABLED
    char const *error = nanos6_library_mode_init();
    if (error != NULL)
    {
        fprintf(stderr, "Error initializing Nanos6: %s\n", error);
        return 1;
    }
    std::cout << "[C-BINDING] Nanos6 initialized" << std::endl; 
#endif

    //Creating custom streams
    char* arg_nio[argc-1];
    int i;
    for (i=1; i < argc; i++) {
        arg_nio[i-1]= argv[i];
    }
    CBindingCache *cache = new CBindingCache();
    CBindingExecutor *executor = new CBindingExecutor(cache);
    worker_start(cache, executor, argc-1, arg_nio);

#ifdef OMPSS2_ENABLED
    nanos6_shutdown();
    std::cout << "[C-BINDING] Nanos6 shutdown" << std::endl;
#endif

}
