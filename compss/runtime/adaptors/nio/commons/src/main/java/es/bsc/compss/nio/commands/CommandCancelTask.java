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
package es.bsc.compss.nio.commands;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.nio.NIOAgent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandCancelTask extends RetriableCommand {

    // Job description
    private int jobId;


    /**
     * Creates a new CommandNewTask for externalization.
     */
    public CommandCancelTask() {
        super();
    }

    /**
     * Creates a new CommandNewTask instance.
     *
     * @param jobId Id of the job.
     */
    public CommandCancelTask(int jobId) {
        this.jobId = jobId;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.cancelRunningTask((NIONode) c.getNode(), this.jobId);
        c.finishConnection();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.jobId = (int) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.jobId);
    }

    @Override
    public String toString() {
        return "New Task with job ID " + this.jobId;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleCancellingTaskCommandError(c, this);

    }

}
