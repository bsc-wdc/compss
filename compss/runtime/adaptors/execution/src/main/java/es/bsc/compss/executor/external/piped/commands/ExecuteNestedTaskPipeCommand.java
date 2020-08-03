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

import es.bsc.compss.executor.external.commands.ExecuteNestedTaskExternalCommand;


/**
 * Description of a task execution command to be sent through a pipe.
 */
public class ExecuteNestedTaskPipeCommand extends ExecuteNestedTaskExternalCommand implements PipeCommand {

    /**
     * Execute task command constructor.
     */
    public ExecuteNestedTaskPipeCommand(String[] command) {
        super();

        entryPoint = EntryPoint.valueOf(command[1]);

        switch (this.entryPoint) {
            case SIGNATURE: {
                // EXECUTE_NESTED_TASK "SIGNATURE" SIGNATURE ONFAILURE TIMEOUT IS_PRIORITARY NUM_NODES IS_REPLICATED
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
                String[] params = new String[command.length - 12];
                if (command.length > 12) {
                    System.arraycopy(command, 12, params, 0, params.length);
                }
                parameters = processParameters(params);
                break;
            }
            case CLASS_METHOD: {
                // EXECUTE_NESTED_TASK "CLASS_METHOD" METHOD_CLASS ONFAILURE TIMEOUT METHOD_NAME IS_PRIORITARY
                // HAS_TARGET NUM_RETURNS PARAMETER_COUNT PARAMENTERS
                methodClass = command[2];
                onFailure = command[3];
                timeout = Integer.parseInt(command[4]);
                methodName = command[5];
                prioritary = Boolean.parseBoolean(command[6]);
                hasTarget = Boolean.parseBoolean(command[7]);
                numReturns = Integer.parseInt(command[8]);
                parameterCount = Integer.parseInt(command[9]);
                String[] params = new String[command.length - 10];
                if (command.length > 10) {
                    System.arraycopy(command, 10, params, 0, params.length);
                }
                parameters = processParameters(params);
                break;
            }
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

}
