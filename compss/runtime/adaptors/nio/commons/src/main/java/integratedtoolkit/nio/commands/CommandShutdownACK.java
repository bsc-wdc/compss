package integratedtoolkit.nio.commands;

import es.bsc.comm.Connection;
import integratedtoolkit.nio.NIOAgent;

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

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
    
    @Override
    public String toString() {
        return "ShutdownACK";
    }

}
