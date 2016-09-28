package integratedtoolkit.nio.master.configuration;

import java.io.File;

import integratedtoolkit.nio.master.NIOAdaptor;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class NIOConfiguration extends MethodConfiguration {

    // Remote executors
    public static final String SSH_REMOTE_EXECUTION_COMMAND = "ssh";
    public static final String SRUN_REMOTE_EXECUTION_COMMAND = "srun";
    public static final String DEFAULT_REMOTE_EXECUTION_COMMAND = SSH_REMOTE_EXECUTION_COMMAND;

    private String sandboxWorkingDir;
    private int minPort;
    private int maxPort;
    private String remoteExecutionCommand;


    public NIOConfiguration(String adaptorName) {
        super(adaptorName);
    }

	@Override
	public void setWorkingDir(String workingDir) {
		super.setWorkingDir(workingDir);
		String host = this.getHost().replace("/", "_").replace(":", "_"); // Replace nasty characters
		String sandboxWorkingDir = this.getWorkingDir() + NIOAdaptor.DEPLOYMENT_ID + File.separator + host + File.separator;
		this.setSandboxWorkingDir(sandboxWorkingDir);
	}

    public String getSandboxWorkingDir() {
        return sandboxWorkingDir;
    }

    public void setSandboxWorkingDir(String sandboxWorkingDir) {
        this.sandboxWorkingDir = sandboxWorkingDir;
    }

    @Override
    public int getMinPort() {
        return minPort;
    }

    public void setMinPort(int minPort) {
        this.minPort = minPort;
    }

    @Override
    public int getMaxPort() {
        return maxPort;
    }

    public void setMaxPort(int maxPort) {
        this.maxPort = maxPort;
    }

    public String getRemoteExecutionCommand() {
        return this.remoteExecutionCommand;
    }

    public void setRemoteExecutionCommand(String remoteExecutionCommand) {
        this.remoteExecutionCommand = remoteExecutionCommand;
    }

}
