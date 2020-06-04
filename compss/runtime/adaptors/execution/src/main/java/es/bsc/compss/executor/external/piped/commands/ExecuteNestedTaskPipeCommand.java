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

        this.entryPoint = EntryPoint.valueOf(command[1]);

        switch (this.entryPoint) {
            case SIGNATURE: {
                // EXECUTE_NESTED_TASK "SIGNATURE" SIGNATURE ONFAILURE TIMEOUT IS_PRIORITARY NUM_NODES IS_REPLICATED
                // IS_DISTRIBUTED HAS_TARGET NUM_RETURNS PARAMETER_COUNT PARAMENTERS
                this.signature = command[2];
                this.onFailure = command[3];
                this.timeout = Integer.parseInt(command[4]);
                this.prioritary = Boolean.parseBoolean(command[5]);
                this.numNodes = Integer.parseInt(command[6]);
                this.reduce = Boolean.parseBoolean(command[7]);
                this.reduceChunkSize = Integer.parseInt(command[8]);
                this.isReplicated = Boolean.parseBoolean(command[9]);
                this.isDistributed = Boolean.parseBoolean(command[10]);
                this.hasTarget = Boolean.parseBoolean(command[11]);
                this.numReturns = Integer.parseInt(command[12]);
                this.parameterCount = Integer.parseInt(command[13]);
                String[] params = new String[command.length - 14];
                if (command.length > 14) {
                    System.arraycopy(command, 14, params, 0, params.length);
                }
                this.parameters = processParameters(params);
                break;
            }
            case CLASS_METHOD: {
                // EXECUTE_NESTED_TASK "CLASS_METHOD" METHOD_CLASS ONFAILURE TIMEOUT METHOD_NAME IS_PRIORITARY
                // HAS_TARGET NUM_RETURNS PARAMETER_COUNT PARAMENTERS
                this.methodClass = command[2];
                this.onFailure = command[3];
                this.timeout = Integer.parseInt(command[4]);
                this.methodName = command[5];
                this.prioritary = Boolean.parseBoolean(command[6]);
                this.reduce = false;
                this.reduceChunkSize = 0;
                this.hasTarget = Boolean.parseBoolean(command[7]);
                this.numReturns = Integer.parseInt(command[8]);
                this.parameterCount = Integer.parseInt(command[9]);
                String[] params = new String[command.length - 10];
                if (command.length > 10) {
                    System.arraycopy(command, 10, params, 0, params.length);
                }
                this.parameters = processParameters(params);
                break;
            }
            default:
                // Nothing to do
                break;
        }
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
