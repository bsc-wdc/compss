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
#include <generated_executor.h>
#include <CBindingCache.h>
#include <common.h>
#include <sys/time.h>

#include <iostream>

#ifdef OMPSS2_ENABLED
#include <pthread.h>
#include <nanos6/bootstrap.h>
#include <nanos6/library-mode.h>
#endif

int get_compss_worker_lock() {
    return 0;
}

int release_compss_worker_lock() {
    return 0;
}

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

    init_env_vars();
    CBindingCache *cache = new CBindingCache();
    struct timeval t_comp_start, t_comp_end;
    gettimeofday(&t_comp_start, NULL);
    int out = execute(argc, argv, cache, 1);
    gettimeofday(&t_comp_end, NULL);
    double total_msecs = (((t_comp_end.tv_sec - t_comp_start.tv_sec) * 1000000) + (t_comp_end.tv_usec - t_comp_start.tv_usec))/1000;
    printf("COMPSs task executor time: %f ms\n",total_msecs);
    
    if (out == 0) {
        printf("Task executed successfully");
    } else {
        printf("Error task execution at worker returned %d", out);
    }

#ifdef OMPSS2_ENABLED
    nanos6_shutdown();
    std::cout << "[C-BINDING] Nanos6 shutdown" << std::endl;
#endif

    return out;
}
