package integratedtoolkit.nio.master.configuration;

import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class NIOConfiguration extends MethodConfiguration {

    // Remote executors
    public static final String SSH_REMOTE_EXECUTION_COMMAND = "ssh";
    public static final String SRUN_REMOTE_EXECUTION_COMMAND = "srun";
    public static final String DEFAULT_REMOTE_EXECUTION_COMMAND = SSH_REMOTE_EXECUTION_COMMAND;

    private int minPort;
    private int maxPort;
    private String remoteExecutionCommand;


    public NIOConfiguration(String adaptorName) {
        super(adaptorName);
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
