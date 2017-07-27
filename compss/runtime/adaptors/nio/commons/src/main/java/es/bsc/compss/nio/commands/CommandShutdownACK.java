package es.bsc.compss.nio.commands;

import es.bsc.comm.Connection;
import es.bsc.compss.nio.NIOAgent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandShutdownACK extends Command implements Externalizable {

    public CommandShutdownACK() {
        super();
    }

    public CommandShutdownACK(NIOAgent ng) {
        super(ng);
    }

    @Override
    public CommandType getType() {
        return CommandType.STOP_WORKER_ACK;
    }

    @Override
    public void handle(Connection c) {
        agent.shutdownNotification(c);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to read
    }

    @Override
    public String toString() {
        return "ShutdownACK";
    }

}
