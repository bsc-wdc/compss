package integratedtoolkit.nio.commands;

import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOTaskResult;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandTaskDone extends Command implements Externalizable {

    private boolean successful;
    private NIOTaskResult tr;


    public CommandTaskDone() {
    }

    public CommandTaskDone(NIOAgent ng, NIOTaskResult tr, boolean successful) {
        super(ng);
        this.tr = tr;
        this.successful = successful;
    }

    @Override
    public CommandType getType() {
        return CommandType.TASK_DONE;
    }

    @Override
    public void handle(Connection c) {
        NIOAgent nm = (NIOAgent) agent;
        nm.receivedTaskDone(c, tr, successful);
    }

    public boolean isSuccessful() {
        return successful;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        successful = in.readBoolean();
        tr = (NIOTaskResult) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeObject(tr);
    }

    @Override
    public String toString() {
        return "Job" + tr.getTaskId() + " finishes " + (successful ? "properly" : "with some errors");
    }

}
