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

import es.bsc.compss.executor.external.commands.AddedExecutorExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;


public class AddedExecutorPipeCommand extends AddedExecutorExternalCommand implements PipeCommand {

    private final String inPipe;
    private final String outPipe;
    private int pid;


    public AddedExecutorPipeCommand(String[] line) {
        inPipe = line[1];
        outPipe = line[2];
        pid = Integer.parseInt(line[3]);
    }

    public AddedExecutorPipeCommand(PipePair pp) {
        inPipe = pp.getInboundPipe();
        outPipe = pp.getOutboundPipe();
    }

    @Override
    public String getAsString() {
        return super.getAsString() + " " + inPipe + " " + outPipe;
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
        pid = ((AddedExecutorPipeCommand) receivedCommand).pid;
    }

    public int getPid() {
        return pid;
    }
}
