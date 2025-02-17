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

import es.bsc.compss.executor.external.commands.WorkerStartedExternalCommand;


public class WorkerStartedPipeCommand extends WorkerStartedExternalCommand implements PipeCommand {

    private int pid;


    public WorkerStartedPipeCommand(String[] command) {
        this.pid = Integer.parseInt(command[1]);
    }

    public WorkerStartedPipeCommand() {

    }

    public int getPid() {
        return pid;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(super.getAsString());
        sb.append(TOKEN_SEP);
        sb.append(pid);
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        return Integer.compare(this.getType().ordinal(), t.getType().ordinal());
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        this.pid = ((WorkerStartedPipeCommand) receivedCommand).pid;
    }

    @Override
    public String toString() {
        return "WORKER_STARTED " + pid;
    }
}
