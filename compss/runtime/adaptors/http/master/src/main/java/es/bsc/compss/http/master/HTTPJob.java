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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HTTPJob extends JobImpl<HTTPInstance> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private static final String SUBMIT_ERROR = "Error calling HTTP Service";

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
    public void submitJob() {
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
    protected void registerParameterResult(DependencyParameter dp, String dataName) {

        JsonObject retValue = (JsonObject) this.getReturnValue();

        if (dp.getType().equals(DataType.FILE_T)) {
            Object value = retValue.get(dp.getName()).toString();
            try {
                FileWriter file = new FileWriter(dp.getDataTarget());
                // 0004 is the JSON serializer ID in Python binding
                file.write("0004");
                file.write(value.toString());
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            registerResultLocation(dp, dataName, Comm.getAppHost());
        } else {
            // it's a Java HTTP task, can have only single value of a primitive type
            Gson gson = new Gson();
            JsonPrimitive primValue = retValue.getAsJsonPrimitive("$return_0");
            Object value;
            switch (dp.getType()) {
                case INT_T:
                    value = gson.fromJson(primValue, int.class);
                    break;
                case LONG_T:
                    value = gson.fromJson(primValue, long.class);
                    break;
                case STRING_T:
                    value = gson.fromJson(primValue, String.class);
                    break;
                case STRING_64_T:
                    String temp = gson.fromJson(primValue, String.class);
                    byte[] encoded = Base64.getEncoder().encode(temp.getBytes());
                    value = new String(encoded);
                    break;
                case OBJECT_T:
                    if (dp.getContentType().equals("int")) {
                        value = gson.fromJson(primValue, int.class);
                    } else {
                        if (dp.getContentType().equals("long")) {
                            value = gson.fromJson(primValue, long.class);
                        } else {
                            if (dp.getContentType().equals("java.lang.String")) {
                                value = gson.fromJson(primValue, String.class);
                            } else {
                                value = gson.fromJson(primValue, Object.class);
                            }
                        }
                    }
                    break;
                default:
                    value = null;
                    break;
            }
            LogicalData ld = Comm.registerValue(dataName, value);
            for (DataLocation dl : ld.getLocations()) {
                dp.setDataTarget(dl.getPath());
            }
        }
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

}
