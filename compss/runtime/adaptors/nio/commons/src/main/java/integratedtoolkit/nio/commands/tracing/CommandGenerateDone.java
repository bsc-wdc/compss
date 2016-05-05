package integratedtoolkit.nio.commands.tracing;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;
import integratedtoolkit.nio.commands.Command;


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

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
    
    @Override
    public String toString() {
        return "GeneratingTraceCommandDone";
    }

}
