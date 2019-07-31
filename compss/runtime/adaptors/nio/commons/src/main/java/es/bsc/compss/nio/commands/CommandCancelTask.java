package es.bsc.compss.nio.commands;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.nio.NIOAgent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandCancelTask implements Command {

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
        try {
            this.jobId = (int) in.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.jobId);
    }

    @Override
    public String toString() {
        return "New Task with job ID " + this.jobId;
    }

}
