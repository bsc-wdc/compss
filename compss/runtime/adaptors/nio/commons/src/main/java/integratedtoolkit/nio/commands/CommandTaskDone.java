package integratedtoolkit.nio.commands;

import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOTask;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;

public class CommandTaskDone extends Command implements Externalizable {

    private int jobID;
    private boolean successful;
    private NIOTask nt;

    public CommandTaskDone() {
    }

    public CommandTaskDone(NIOAgent ng, int jobID, boolean successful) {
        super(ng);
        this.jobID = jobID;
        this.successful = successful;
    }
    
    public CommandTaskDone(NIOAgent ng, NIOTask nt, boolean successful) {
        super(ng);
        this.jobID = nt.getJobId();
        this.successful = successful;
        this.nt = nt;
    }

    @Override
    public CommandType getType() {
        return CommandType.TASK_DONE;
    }

    @Override
    public void handle(Connection c) {
        NIOAgent nm = (NIOAgent) agent;
        nm.receivedTaskDone(c, jobID, nt, successful);
    }

    public boolean isSuccessful() {
        return successful;
    }

    public int getJobID() {
        return jobID;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobID = in.readInt();
        successful = in.readBoolean();
        nt = (NIOTask) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(jobID);
        out.writeBoolean(successful);
        out.writeObject(nt);
    }

    @Override
    public String toString() {
        return "Job" + jobID + " finishes " + (successful ? "properly" : "with some errors");
    }
}
