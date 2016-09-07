package integratedtoolkit.nio.commands.tracing;

import integratedtoolkit.nio.commands.Command;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandGeneratePackage extends Command implements Externalizable {

    public CommandGeneratePackage() {
        super();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to write
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to read
    }

    @Override
    public CommandType getType() {
        return CommandType.GEN_TRACE_PACKAGE;
    }

    @Override
    public void handle(Connection c) {
        agent.generatePackage(c);
    }

    @Override
    public String toString() {
        return "GenerateTraceCommand";
    }

}
