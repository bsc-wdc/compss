/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.nio.master.configuration;

import es.bsc.compss.types.resources.configuration.MethodConfiguration;

import java.util.Arrays;
import java.util.List;


public class NIOConfiguration extends MethodConfiguration {

    // Remote executors
    public static final String SSH_REMOTE_EXECUTION_COMMAND = "ssh";
    public static final String SRUN_REMOTE_EXECUTION_COMMAND = "srun";
    public static final String BLAUNCH_REMOTE_EXECUTION_COMMAND = "blaunch";
    public static final String QRSH_REMOTE_EXECUTION_COMMAND = "qrsh";
    public static final String NO_REMOTE_EXECUTION_COMMAND = "none";
    private static final List<String> AVAILABLE_REMOTE_EXECUTION_COMMANDS =
        Arrays.asList(new String[] { SSH_REMOTE_EXECUTION_COMMAND, // SSH
            SRUN_REMOTE_EXECUTION_COMMAND, // SRUN
            BLAUNCH_REMOTE_EXECUTION_COMMAND, // BLAUNCH
            QRSH_REMOTE_EXECUTION_COMMAND, // QRSH
            NO_REMOTE_EXECUTION_COMMAND // NONE
        });
    public static final String DEFAULT_REMOTE_EXECUTION_COMMAND = SSH_REMOTE_EXECUTION_COMMAND;

    private String remoteExecutionCommand;


    /**
     * Creates a new NIOConfiguration instance.
     * 
     * @param adaptorName Associated adaptor name.
     */
    public NIOConfiguration(String adaptorName) {
        super(adaptorName);
    }

    /**
     * Clones the given NIOConfiguration.
     * 
     * @param clone NIOConfiguration to clone.
     */
    public NIOConfiguration(NIOConfiguration clone) {
        super(clone);
        this.remoteExecutionCommand = clone.remoteExecutionCommand;
    }

    @Override
    public MethodConfiguration copy() {
        return new NIOConfiguration(this);
    }

    /**
     * Returns the remote execution command.
     * 
     * @param user User.
     * @param resource Resource name.
     * @param command Specific command.
     * @return Complete remote execution command.
     */
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
                cmd = new String[6 + command.length];
                cmd[0] = "ssh";
                cmd[1] = "-o StrictHostKeyChecking=no";
                cmd[2] = "-o BatchMode=yes";
                cmd[3] = "-o ChallengeResponseAuthentication=no";
                cmd[4] = "-p " + this.getSpawnerPort();
                cmd[5] = ((user == null || user.isEmpty()) ? "" : user + "@") + resource;
                System.arraycopy(command, 0, cmd, 6, command.length);
                break;
        }
        return cmd;
    }

    /**
     * Sets a new value for the remote execution command.
     * 
     * @param remoteExecutionCommand New remote execution command.
     */
    public void setRemoteExecutionCommand(String remoteExecutionCommand) {
        this.remoteExecutionCommand = remoteExecutionCommand;
    }

    public static List<String> getAvailableRemoteExecutionCommands() {
        return AVAILABLE_REMOTE_EXECUTION_COMMANDS;
    }

}
