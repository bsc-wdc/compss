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
package es.bsc.compss.executor.external.piped.commands;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import java.util.Arrays;


/**
 * Description of a task execution command to be sent through a pipe.
 */
public class ExecuteNestedTaskPipeCommand extends ExecuteTaskExternalCommand implements PipeCommand {

    private final Lang lang;
    private final String onFailure;
    private final int timeout;
    private final boolean prioritary;
    private final String signature;
    private final String[] parameters;
    private final int parameterCount;
    private final int numReturns;
    private final boolean hasTarget;
    private final int numNodes;
    private final boolean isReplicated;
    private final boolean isDistributed;
    private final String methodName;
    private final String methodClass;


    /**
     * Execute task command constructor.
     */
    public ExecuteNestedTaskPipeCommand(String[] command) {
        super();

        this.lang = Lang.valueOf(command[1]);

        String onFailure = "";
        int timeout = 0;
        boolean prioritary = false;
        String signature = null;
        String[] parameters = null;
        int parameterCount = 0;
        int numReturns = 0;
        boolean hasTarget = false;
        int numNodes = 0;
        boolean isReplicated = false;
        boolean isDistributed = false;
        String methodName = null;
        String methodClass = null;

        switch (lang) {
            case PYTHON:
                // EXECUTE_NESTED_TASK LANG SIGNATURE ONFAILURE TIMEOUT IS_PRIORITARY NUM_NODES IS_REPLICATED
                // IS_DISTRIBUTED HAS_TARGET NUM_RETURNS PARAMETER_COUNT PARAMENTERS
                signature = command[2];
                onFailure = command[3];
                timeout = Integer.parseInt(command[4]);
                prioritary = Boolean.parseBoolean(command[5]);
                numNodes = Integer.parseInt(command[6]);
                isReplicated = Boolean.parseBoolean(command[7]);
                isDistributed = Boolean.parseBoolean(command[8]);
                hasTarget = Boolean.parseBoolean(command[9]);
                numReturns = Integer.parseInt(command[10]);
                parameterCount = Integer.parseInt(command[11]);
                parameters = new String[command.length - 12];
                if (command.length > 12) {
                    System.arraycopy(command, 12, parameters, 0, parameters.length);
                }
                break;
            case C:
                // EXECUTE_NESTED_TASK LANG METHOD_CLASS ONFAILURE TIMEOUT METHOD_NAME IS_PRIORITARY
                // HAS_TARGET NUM_RETURNS PARAMETER_COUNT PARAMENTERS
                methodClass = command[2];
                onFailure = command[3];
                timeout = Integer.parseInt(command[4]);
                methodName = command[5];
                prioritary = Boolean.parseBoolean(command[6]);
                hasTarget = Boolean.parseBoolean(command[7]);
                numReturns = Integer.parseInt(command[8]);
                parameterCount = Integer.parseInt(command[9]);
                parameters = new String[command.length - 10];
                if (command.length > 10) {
                    System.arraycopy(command, 10, parameters, 0, parameters.length);
                }
                break;
            default:

        }
        this.onFailure = onFailure;
        this.timeout = timeout;
        this.prioritary = prioritary;
        this.signature = signature;
        this.parameters = parameters;
        this.parameterCount = parameterCount;
        this.numReturns = numReturns;
        this.hasTarget = hasTarget;
        this.numNodes = numNodes;
        this.isReplicated = isReplicated;
        this.isDistributed = isDistributed;
        this.methodClass = methodClass;
        this.methodName = methodName;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_NESTED_TASK.name());
        for (String c : this.arguments) {
            sb.append(TOKEN_SEP);
            sb.append(c);
        }
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            value = t.hashCode() - this.hashCode();
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Lang getLang() {
        return this.lang;
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
