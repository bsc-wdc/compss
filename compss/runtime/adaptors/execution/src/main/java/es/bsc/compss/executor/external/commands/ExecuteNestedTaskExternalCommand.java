/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

/**
 * Command to request an execution to the runtime.
 */
public class ExecuteNestedTaskExternalCommand implements ExternalCommand {

    public static enum EntryPoint {
        SIGNATURE, CLASS_METHOD
    }


    protected EntryPoint entryPoint;
    protected String onFailure;
    protected int timeout;
    protected boolean prioritary;
    protected String signature;
    protected String[] parameters;
    protected int parameterCount;
    protected int numReturns;
    protected boolean hasTarget;
    protected int numNodes;
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

    public String[] getParameters() {
        return this.parameters;
    }

    public String getSignature() {
        return this.signature;
    }

    public int getNumNodes() {
        return this.numNodes;
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
}
