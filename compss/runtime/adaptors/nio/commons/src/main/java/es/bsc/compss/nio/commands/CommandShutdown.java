package es.bsc.compss.nio.commands;

import es.bsc.compss.nio.NIOAgent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

import es.bsc.comm.Connection;


public class CommandShutdown extends Command implements Externalizable {

    // List of files to send to the master before shutting down
    private LinkedList<Data> filesToSend;


    public CommandShutdown() {
    }

    public CommandShutdown(NIOAgent agent, LinkedList<Data> l) {
        super(agent);
        this.filesToSend = l;
    }

    @Override
    public CommandType getType() {
        return CommandType.STOP_WORKER;
    }

    @Override
    public void handle(Connection c) {
        agent.receivedShutdown(c, filesToSend);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        filesToSend = (LinkedList<Data>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(filesToSend);
    }

    @Override
    public String toString() {
        return "Shutdown";
    }

}
