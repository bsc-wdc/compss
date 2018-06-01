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
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.nio.worker.util.TaskResultReader;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CPersistentExecutor extends PersistentExternalExecutor {

    public CPersistentExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        super(nw, pool, queue);
    }

    @Override
    public ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) {
        
        return CExecutionCommandGenerator.getTaskExecutionCommand(nw, nt, sandBox, assignedCoreUnits, assignedGPUs, assignedFPGAs);

    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        return CExecutionCommandGenerator.getEnvironment(nw);
    }

}
