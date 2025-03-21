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
package es.bsc.compss.http.master;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HTTPJob extends JobImpl<HTTPInstance> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    // ERRORS
    private static final String ERR_SERIALIZE_TO_FILE = "Cannot serialize to file HTTP Result";

    // Class structures
    private static final int POOL_SIZE =
        Integer.parseInt(System.getenv().getOrDefault(COMPSsConstants.COMPSS_HTTP_POOL_SIZE, "100"));
    private static final String POOL_NAME = "HTTP";

    private static RequestQueue<HTTPJob> callerQueue;
    private static Gson gson = new Gson();

    private static HTTPCaller caller;
    // Pool of worker threads and queue of requests
    private static ThreadPool callerPool;


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

    /**
     * The job has fully been executed.
     * 
     * @param retValue the body of the return message
     */
    public void completed(JsonObject retValue) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        List<Parameter> params = this.taskParams.getParameters();
        for (Parameter p : params) {
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                String dataName = getOutputRename(p);
                if (dataName != null) {
                    registerParameterResult(dp, dataName, retValue);
                    notifyResultAvailability(dp, dataName);
                }
            }
        }
        super.completed();
    }

    @Override
    public void failed(JobEndStatus status) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        switch (this.taskParams.getOnFailure()) {
            case IGNORE:
            case CANCEL_SUCCESSORS:
                List<Parameter> params = this.taskParams.getParameters();
                for (Parameter p : params) {
                    if (p.isPotentialDependency()) {
                        DependencyParameter dp = (DependencyParameter) p;
                        String dataName = getOutputRename(p);
                        if (dataName != null) {
                            emptyParameterResult(dp, dataName);
                            notifyResultAvailability(dp, dataName);
                        }
                    }
                }
                break;
            default:
                // case RETRY:
                // case FAIL:
                removeTmpData();
        }
        super.failed(status);
    }

    /**
     * Actions to be done when the job execution fails.
     *
     * @param status end status of the job
     * @param retValue return value as JSON object
     */

    public void failed(JobEndStatus status, JsonObject retValue) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        switch (this.taskParams.getOnFailure()) {
            case IGNORE:
            case CANCEL_SUCCESSORS:
                List<Parameter> params = this.taskParams.getParameters();
                for (Parameter p : params) {
                    if (p.isPotentialDependency()) {
                        DependencyParameter dp = (DependencyParameter) p;
                        String dataName = getOutputRename(p);
                        if (dataName != null) {
                            registerParameterResult(dp, dataName, retValue);
                            notifyResultAvailability(dp, dataName);
                        }
                    }
                }
                break;
            default:
                // case RETRY:
                // case FAIL:
                removeTmpData();
        }
        super.failed(status);
    }

    private void emptyParameterResult(DependencyParameter dp, String dataName) {
        if (dp.getType() == DataType.FILE_T) {
            try {
                File f = new File(dp.getDataTarget());
                f.createNewFile();
            } catch (IOException e) {
                ErrorManager.error(ERR_SERIALIZE_TO_FILE, e);
            }

            SimpleURI uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + dp.getDataTarget());
            try {
                DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                Comm.registerLocation(dataName, outLoc);
            } catch (IOException e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
            }
        } else {
            // it's a Java HTTP task, can have only single value of a primitive type
            Object value = null;
            Comm.registerValue(dataName, value);
        }
    }

    private void registerParameterResult(DependencyParameter dp, String dataName, JsonObject retValue) {

        if (dp.getType() == DataType.FILE_T) {
            try {
                storeFileResult(retValue.get(dp.getName()), dp.getDataTarget());
            } catch (IOException e) {
                ErrorManager.error(ERR_SERIALIZE_TO_FILE, e);
            }

            SimpleURI uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + dp.getDataTarget());
            try {
                DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                Comm.registerLocation(dataName, outLoc);
            } catch (IOException e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
            }
        } else {
            // it's a Java HTTP task, can have only single value of a primitive type
            Object value;
            if (dp.getType() == DataType.OBJECT_T) {
                JsonPrimitive objectValue = retValue.getAsJsonPrimitive("$return_0");
                value = getObjectResult(objectValue, dp.getContentType());
            } else {
                JsonPrimitive primValue = retValue.getAsJsonPrimitive("$return_0");
                value = getPrimitiveResult(primValue, dp.getType());
            }
            Comm.registerValue(dataName, value);
        }
    }

    private Object getObjectResult(JsonPrimitive primValue, String contentType) {
        Object value;
        switch (contentType) {
            case "int":
                value = gson.fromJson(primValue, int.class);
                break;
            case "long":
                value = gson.fromJson(primValue, long.class);
                break;
            case "java.lang.String":
                value = gson.fromJson(primValue, String.class);
                break;
            default:
                value = gson.fromJson(primValue, Object.class);
        }
        return value;
    }

    private Object getPrimitiveResult(JsonPrimitive primValue, DataType type) {
        Object value;
        switch (type) {
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
            default:
                value = null;
        }
        return value;
    }

    private void storeFileResult(JsonElement value, String location) throws IOException {
        FileWriter file = new FileWriter(location);
        // 0004 is the JSON serializer ID in Python binding
        file.write("0004");
        file.write(value.toString());
        file.close();
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
