package integratedtoolkit.nio.master.configuration;

import java.util.Arrays;
import java.util.List;

import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class NIOConfiguration extends MethodConfiguration {

    // Remote executors
    public static final String SSH_REMOTE_EXECUTION_COMMAND = "ssh";
    public static final String SRUN_REMOTE_EXECUTION_COMMAND = "srun";
    public static final String BLAUNCH_REMOTE_EXECUTION_COMMAND = "blaunch";
    public static final String QRSH_REMOTE_EXECUTION_COMMAND = "qrsh";
    public static final String NO_REMOTE_EXECUTION_COMMAND = "none";
    public static final List<String> AVAILABLE_REMOTE_EXECUTION_COMMANDS = Arrays.asList(new String[] { SSH_REMOTE_EXECUTION_COMMAND,
            SRUN_REMOTE_EXECUTION_COMMAND, BLAUNCH_REMOTE_EXECUTION_COMMAND, QRSH_REMOTE_EXECUTION_COMMAND, NO_REMOTE_EXECUTION_COMMAND });
    public static final String DEFAULT_REMOTE_EXECUTION_COMMAND = SSH_REMOTE_EXECUTION_COMMAND;

    private int minPort;
    private int maxPort;
    private String remoteExecutionCommand;


    public NIOConfiguration(String adaptorName) {
        super(adaptorName);
    }

    public NIOConfiguration(NIOConfiguration clone) {
        super(clone);
        this.minPort = clone.minPort;
        this.maxPort = clone.maxPort;
        this.remoteExecutionCommand = clone.remoteExecutionCommand;
    }

    @Override
    public MethodConfiguration copy() {
        return new NIOConfiguration(this);
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

    public String[] getRemoteExecutionCommand(String user, String resource, String[] command) {
        String[] cmd = null;
        switch (this.remoteExecutionCommand) {
            case NO_REMOTE_EXECUTION_COMMAND:
                // No remote execution command returning null;
                break;
            case QRSH_REMOTE_EXECUTION_COMMAND:
                cmd = new String[3 + command.length];
                cmd[0] = "qrsh";
                cmd[1] = "-inherit";
                cmd[2] = resource;
                System.arraycopy(command, 0, cmd, 3, command.length);
                break;
            case BLAUNCH_REMOTE_EXECUTION_COMMAND:
                cmd = new String[2 + command.length];
                cmd[0] = "blaunch";
                cmd[1] = resource;
                System.arraycopy(command, 0, cmd, 2, command.length);
                break;
            case SRUN_REMOTE_EXECUTION_COMMAND:
                cmd = new String[4 + command.length];
                cmd[0] = "srun";
                cmd[1] = "-n1";
                cmd[2] = "-N1";
                cmd[3] = "--nodelist=" + resource;
                System.arraycopy(command, 0, cmd, 4, command.length);
                break;
            case SSH_REMOTE_EXECUTION_COMMAND:
            default:
                // SSH OR Default
                cmd = new String[5 + command.length];
                cmd[0] = "ssh";
                cmd[1] = "-o StrictHostKeyChecking=no";
                cmd[2] = "-o BatchMode=yes";
                cmd[3] = "-o ChallengeResponseAuthentication=no";
                cmd[4] = ((user == null || user.isEmpty()) ? "" : user + "@") + resource;
                System.arraycopy(command, 0, cmd, 5, command.length);
                break;
        }
        return cmd;
    }

    public void setRemoteExecutionCommand(String remoteExecutionCommand) {
        this.remoteExecutionCommand = remoteExecutionCommand;
    }

}
