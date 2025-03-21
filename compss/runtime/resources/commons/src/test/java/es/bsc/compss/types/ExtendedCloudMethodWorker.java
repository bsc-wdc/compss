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
package es.bsc.compss.types;

import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import java.util.HashMap;


public class ExtendedCloudMethodWorker extends CloudMethodWorker {

    private boolean terminated;


    public ExtendedCloudMethodWorker(String name, CloudProvider provider, CloudMethodResourceDescription description,
        COMPSsWorker worker, int limitOfTasks, HashMap<String, String> sharedDisks) {
        super(name, provider, description, worker, limitOfTasks, 0, 0, 0, sharedDisks);
        terminated = false;
    }

    public void terminate() {
        this.terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

}
