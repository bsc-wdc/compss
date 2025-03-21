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
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOTaskProfile;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.worker.COMPSsException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandNIOTaskDone extends RetriableCommand {

    private boolean successful;
    private NIOTaskResult tr;
    private String jobHistory;
    private COMPSsException compssException;
    private NIOTaskProfile profile;


    /**
     * Creates a new CommandNIOTaskDone for externalization.
     */
    public CommandNIOTaskDone() {
        super();
        compssException = null;
    }

    /**
     * Creates a new CommandNIOTaskDone instance.
     *
     * @param tr Task result.
     * @param successful Whether the task has successfully finished or not.
     * @param profile NIOTask execution times report
     */
    public CommandNIOTaskDone(NIOTaskResult tr, boolean successful, NIOTaskProfile profile, String jobHistory,
        COMPSsException e) {
        this.tr = tr;
        this.jobHistory = jobHistory;
        this.successful = successful;
        this.compssException = e;
        this.profile = profile;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.receivedNIOTaskDone(c, this.tr, this.profile, this.successful, this.compssException);
    }

    public COMPSsException getCompssException() {
        return this.compssException;
    }

    public void setCompssException(COMPSsException e) {
        this.compssException = e;
    }

    public boolean isSuccessful() {
        return this.successful;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.successful = in.readBoolean();
        this.tr = (NIOTaskResult) in.readObject();
        this.jobHistory = in.readUTF();
        this.compssException = (COMPSsException) in.readObject();
        this.profile = (NIOTaskProfile) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(this.successful);
        out.writeObject(this.tr);
        out.writeUTF(this.jobHistory);
        out.writeObject(this.compssException);
        out.writeObject(this.profile);
    }

    @Override
    public String toString() {
        return "Job " + this.tr.getJobId() + " finishes " + (this.successful ? "properly" : "with some errors")
            + " with task results: " + tr.toString();
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleTaskDoneCommandError(c, this);

    }

    public NIOTaskResult getTaskResult() {
        return this.tr;
    }

    public String getJobHistory() {
        return this.jobHistory;
    }

}
