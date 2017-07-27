package es.bsc.compss.nio.commands.tracing;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;
import es.bsc.compss.nio.commands.Command;


public class CommandGenerateDone extends Command implements Externalizable {

    public CommandGenerateDone() {
        super();
    }

    @Override
    public CommandType getType() {
        return CommandType.GEN_TRACE_PACKAGE_DONE;
    }

    @Override
    public void handle(Connection c) {
        agent.notifyTracingPackageGeneration();
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
        return "GeneratingTraceCommandDone";
    }

}
