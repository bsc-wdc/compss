/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.http.master;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HTTPJob extends Job<HTTPInstance> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private static final String SUBMIT_ERROR = "Error calling HTP Service";

    // Class structures
    private static final int POOL_SIZE = 10;
    private static final String POOL_NAME = "HTTP";

    private static RequestQueue<HTTPJob> callerQueue;

    private static HTTPCaller caller;
    // Pool of worker threads and queue of requests
    private static ThreadPool callerPool;

    private Object returnValue;


    /**
     * Initializes the HTTPJob structures.
     */
    public static void init() {
        // Create thread that will handle job submission requests
        if (callerQueue == null) {
            callerQueue = new RequestQueue<>();
        } else {
            callerQueue.clear();
        }
        caller = new HTTPCaller(callerQueue);
        callerPool = new ThreadPool(POOL_SIZE, POOL_NAME, caller);
        callerPool.startThreads();
    }

    /**
     * Stops the WSJob structures.
     */
    public static void end() {
        callerPool.stopThreads();
    }

    /**
     * Creates a new WSJob instance.
     *
     * @param taskId Associated task Id.
     * @param taskParams Associated task parameters.
     * @param impl Task implementation.
     * @param res Resource.
     * @param listener Task listener.
     */
    public HTTPJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, JobListener listener,
        List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, taskParams, impl, res, listener, predecessors, numSuccessors);

        this.returnValue = null;
    }

    public void setReturnValue(Object returnValue) {
        this.returnValue = returnValue;
    }

    @Override
    public TaskType getType() {
        return TaskType.HTTP;
    }

    @Override
    public void submit() {
        callerQueue.enqueue(this);
    }

    @Override
    public void cancelJob() {

    }

    @Override
    public Object getReturnValue() {
        return returnValue;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[[Job id: ").append(getJobId()).append("]");
        buffer.append(", ").append(taskParams.toString());
        String name = "";
        COMPSsNode node = getResourceNode();
        name = node.getName();
        buffer.append(", [Target URL: ").append(name).append("]]");

        return buffer.toString();
    }

    @Override
    public String getHostName() {
        return getResourceNode().getName();
    }

}
