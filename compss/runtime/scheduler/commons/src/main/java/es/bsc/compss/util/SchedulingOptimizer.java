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
package es.bsc.compss.util;

import es.bsc.compss.components.impl.TaskScheduler;


public class SchedulingOptimizer<T extends TaskScheduler> extends Thread {

    private static final String SCHEDULING_OPTIMIZER_THREAD_NAME = "Task Optimizer";
    protected T scheduler;


    public SchedulingOptimizer(T ts) {
        this.setName(SCHEDULING_OPTIMIZER_THREAD_NAME);
        this.scheduler = ts;
    }

    @Override
    public void run() {

    }

    public void shutdown() {
    }

}
