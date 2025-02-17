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
package es.bsc.compss.executor.external.commands;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


/**
 * Command to request an execution to the runtime.
 */
public class ExecuteNestedTaskExternalCommand implements ExternalCommand {

    // Number of fields per parameter
    public static final int NUM_FIELDS_PER_PARAM = 9;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);


    public enum EntryPoint {
        SIGNATURE, CLASS_METHOD
    }


    protected EntryPoint entryPoint;
    protected String onFailure;
    protected int timeout;
    protected boolean prioritary;
    protected String signature;
    protected Object[] parameters;
    protected int parameterCount;
    protected int numReturns;
    protected boolean hasTarget;
    protected int numNodes;
    protected boolean reduce;
    protected int reduceChunkSize;
    protected boolean isReplicated;
    protected boolean isDistributed;
    protected String methodName;
    protected String methodClass;


    @Override
    public CommandType getType() {
        return CommandType.EXECUTE_NESTED_TASK;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_NESTED_TASK.name());
        return sb.toString();
    }

    public EntryPoint getEntryPoint() {
        return this.entryPoint;
    }

    public String getOnFailure() {
        return this.onFailure;
    }

    public int getTimeOut() {
        return this.timeout;
    }

    public boolean getPrioritary() {
        return this.prioritary;
    }

    public boolean hasTarget() {
        return this.hasTarget;
    }

    public int getNumReturns() {
        return this.numReturns;
    }

    public int getParameterCount() {
        return this.parameterCount;
    }

    public Object[] getParameters() {
        return this.parameters;
    }

    public String getSignature() {
        return this.signature;
    }

    public int getNumNodes() {
        return this.numNodes;
    }

    public boolean isReduce() {
        return this.reduce;
    }

    public int getReduceChunkSize() {
        return this.reduceChunkSize;
    }

    public boolean isReplicated() {
        return this.isReplicated;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    public String getMethodClass() {
        return this.methodClass;
    }

    public String getMethodName() {
        return this.methodName;
    }

    /**
     * Parses the task parameters.
     * 
     * @param commandParams string array with the parameters read from the external command
     * @return Object array to pass in into the executeTask call
     */
    protected Object[] processParameters(String commandParams) {
        Object[] methodParams = new Object[commandParams.length()];
        JSONTokener tokener = new JSONTokener(commandParams);
        JSONArray jsonParams = new JSONArray(tokener);
        int offset = 0;

        for (int i = 0; i < jsonParams.length(); i++) {
            JSONObject param = jsonParams.getJSONObject(i);
            methodParams[offset++] = param.getString("Value");
            methodParams[offset++] = DataType.values()[param.getInt("DataType")];
            methodParams[offset++] = Direction.values()[param.getInt("Direction")];
            methodParams[offset++] = StdIOStream.values()[param.getInt("IOStream")];
            methodParams[offset++] = param.getString("Prefix");
            methodParams[offset++] = param.getString("Name");
            methodParams[offset++] = param.getString("ContType");
            methodParams[offset++] = param.getString("Weight");
            methodParams[offset++] = param.getBoolean("KeepRename");
        }
        return methodParams;
    }
}
