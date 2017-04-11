package integratedtoolkit.nio.commands;

import es.bsc.comm.Connection;
import integratedtoolkit.nio.NIOAgent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class CommandExecutorShutdown extends Command implements Externalizable {

    public CommandExecutorShutdown() {
    }

    public CommandExecutorShutdown(NIOAgent agent) {
        super(agent);
    }

    @Override
    public CommandType getType() {
        return CommandType.STOP_EXECUTOR;
    }

    @Override
    public void handle(Connection c) {
        agent.shutdownExecutionManager(c);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public String toString() {
        return "ExecutorShutdown";
    }

}
