package integratedtoolkit.nio.commands.tracing;

import integratedtoolkit.nio.commands.Command;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandGeneratePackage extends Command implements Externalizable {

	private String host;
	private String installDir;
	private String workingDir;
	private String name;

	public CommandGeneratePackage() {
		super();
	}
	
	public CommandGeneratePackage(String host, String installDir, String workingDir, String name) {
		super();
		this.host=host;
		this.installDir=installDir;
		this.workingDir=workingDir;
		this.name=name;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		host = (String)in.readObject();
		installDir = (String)in.readObject();
		workingDir = (String)in.readObject();
		name = (String)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(host);
		out.writeObject(installDir);
		out.writeObject(workingDir);
		out.writeObject(name);
		
	}

	@Override
	public CommandType getType() {
		return CommandType.GEN_TRACE_PACKAGE;
	}

	@Override
	public void handle(Connection c) {
		agent.generatePackage(c, host, installDir, workingDir, name);
	}
	
    @Override
    public String toString() {
        return "GenerateTraceCommand";
    }

}
