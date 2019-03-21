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

import es.bsc.compss.executor.external.commands.ExecutorPIDReplyExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;
import java.util.LinkedList;
import java.util.List;


public class ExecutorPIDReplyPipeCommand extends ExecutorPIDReplyExternalCommand implements PipeCommand {

    private final String inPipe;
    private final String outPipe;
    private final List<Integer> pids;


    public ExecutorPIDReplyPipeCommand(String[] line) {
        this.inPipe = line[1];
        this.outPipe = line[2];
        this.pids = new LinkedList<>();
        for (int idx = 3; idx < line.length; idx++) {
            this.pids.add(Integer.parseInt(line[idx]));
        }
    }

    public ExecutorPIDReplyPipeCommand(PipePair pp) {
        this.inPipe = pp.getInboundPipe();
        this.outPipe = pp.getOutboundPipe();
        this.pids = new LinkedList<>();
    }

    @Override
    public String getAsString() {
        return super.getAsString() + " " + this.inPipe + " " + this.outPipe;
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            ExecutorPIDReplyPipeCommand rt = (ExecutorPIDReplyPipeCommand) t;
            value = this.inPipe.compareTo(rt.inPipe);
        }
        if (value == 0) {
            ExecutorPIDReplyPipeCommand rt = (ExecutorPIDReplyPipeCommand) t;
            value = this.outPipe.compareTo(rt.outPipe);
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        this.pids.addAll(((ExecutorPIDReplyPipeCommand) receivedCommand).pids);
    }

    public List<Integer> getPids() {
        return this.pids;
    }
}
