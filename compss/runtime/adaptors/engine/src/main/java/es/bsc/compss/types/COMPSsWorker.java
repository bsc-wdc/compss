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

import es.bsc.compss.exceptions.AnnounceException;


/**
 * Abstract definition of a COMPSs Worker.
 */
public abstract class COMPSsWorker extends COMPSsNode {

    /**
     * New worker.
     * 
     * @param monitor element monitoring changes on the node
     */
    public COMPSsWorker(NodeMonitor monitor) {
        super(monitor);
    }

    /**
     * Returns the worker user.
     * 
     * @return The worker user.
     */
    public abstract String getUser();

    /**
     * Returns the worker classpath.
     * 
     * @return The worker classpath.
     */
    public abstract String getClasspath();

    /**
     * Returns the worker pythonpath.
     * 
     * @return The worker pythonpath.
     */
    public abstract String getPythonpath();

    /**
     * Updates the task count to the given value {@code processorCoreCount}.
     * 
     * @param processorCoreCount Number of processor cores.
     */
    public abstract void updateTaskCount(int processorCoreCount);

    /**
     * Announces the worker destruction.
     * 
     * @throws AnnounceException Exception announcing destruction.
     */
    public abstract void announceDestruction() throws AnnounceException;

    /**
     * Announces the worker creation.
     * 
     * @throws AnnounceException Error announcing the worker creation.
     */
    public abstract void announceCreation() throws AnnounceException;

}
