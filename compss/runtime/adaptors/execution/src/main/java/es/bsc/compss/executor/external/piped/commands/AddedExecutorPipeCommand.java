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
package es.bsc.compss.executor.external.piped.commands;

import es.bsc.compss.executor.external.commands.AddedExecutorExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;


/**
 * Class to describe a notification for an added executor.
 */
public class AddedExecutorPipeCommand extends AddedExecutorExternalCommand implements PipeCommand {

    private boolean success;
    private final String inPipe;
    private final String outPipe;
    private int pid;


    /**
     * Executor added notification constructor.
     * 
     * @param line String array with the in/out pipe names and the process identifier
     */
    public AddedExecutorPipeCommand(String[] line) {
        success = (line[0].compareTo(CommandType.ADDED_EXECUTOR.toString()) == 0);
        inPipe = line[1];
        outPipe = line[2];
        pid = Integer.parseInt(line[3]);
    }

    /**
     * Executor added notification constructor.
     * 
     * @param pp PipePair expected to use the added executor
     */
    public AddedExecutorPipeCommand(PipePair pp) {
        success = false;
        inPipe = pp.getInboundPipe();
        outPipe = pp.getOutboundPipe();
    }

    @Override
    public String getAsString() {
        return super.getAsString() + " " + inPipe + " " + outPipe + (success ? " PID " + pid : " FAILED");
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            value = inPipe.compareTo(((AddedExecutorPipeCommand) t).inPipe);
        }
        if (value == 0) {
            value = outPipe.compareTo(((AddedExecutorPipeCommand) t).outPipe);
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        success = ((AddedExecutorPipeCommand) receivedCommand).success;
        pid = ((AddedExecutorPipeCommand) receivedCommand).pid;
    }

    public int getPid() {
        return pid;
    }

    public boolean isSuccess() {
        return success;
    }

}
