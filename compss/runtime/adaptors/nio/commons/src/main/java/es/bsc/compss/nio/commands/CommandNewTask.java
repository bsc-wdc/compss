package es.bsc.compss.nio.commands;

import es.bsc.compss.nio.NIOTask;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;


public class CommandNewTask extends Command implements Externalizable {

    // List of the data to erase
    private List<String> obsolete;
    // Job description
    private NIOTask task;


    public CommandNewTask() {
        super();
    }

    public CommandNewTask(NIOTask t, List<String> obsolete) {
        super();
        this.task = t;
        this.obsolete = obsolete;
    }

    @Override
    public CommandType getType() {
        return CommandType.NEW_TASK;
    }

    @Override
    public void handle(Connection c) {
        agent.receivedNewTask((NIONode) c.getNode(), task, obsolete);
        c.finishConnection();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        obsolete = (List<String>) in.readObject();
        task = (NIOTask) in.readObject();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(obsolete);
        out.writeObject(task);
    }

    @Override
    public String toString() {
        return "new Task " + task.toString();
    }

}
