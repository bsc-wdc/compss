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

import es.bsc.compss.types.resources.ResourceDescription;


public interface NodeMonitor {

    /**
     * Notifies the detection of idle resources assigned to an already-running task.
     * 
     * @param resources detected idle resources
     */
    public void idleReservedResourcesDetected(ResourceDescription resources);

    /**
     * Notifies the detection of activity on resources assigned to an already-running task previously notified to be
     * idle.
     * 
     * @param resources reactivated resouces
     */
    public void reactivatedReservedResourcesDetected(ResourceDescription resources);

    public void lostNode();

}
