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
package es.bsc.compss.gos.master.sshutils.staticmethods;

import com.jcraft.jsch.JSchException;
import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.gos.master.GOSJobDescription;
import es.bsc.compss.gos.master.exceptions.GOSException;
import es.bsc.compss.gos.master.exceptions.GOSWarningException;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;


public class SSHCommand {

    /**
     * Execute command batch channel.
     *
     * @param host the host
     * @param jd the jd
     * @return the channel
     */
    public static SSHChannel executeCommandBatch(SSHHost host, GOSJobDescription jd, String outputFile,
        String errorFile) throws JSchException {
        String executable = jd.getExecutable();
        String options = jd.getCommandArgsBatch();
        String arguments = jd.getArgumentsString();
        final String executionCommand = executable + " " + options + " " + arguments;
        SSHChannel channel = host.openChannel("exec", jd.getID() + " commBatch");
        channel.setCommand(executionCommand);

        channel.setInputStream(null);
        OutputStream outputStream;
        OutputStream errorStream;
        try {
            outputStream = new FileOutputStream(outputFile);
            errorStream = new FileOutputStream(errorFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        channel.setOutputStream(outputStream);
        channel.setErrStream(errorStream);
        channel.connect();
        return channel;
    }

    /**
     * Execute command and waits for its resolution. If the command exits with an exit code 100, it does not raise an
     * error fatal, only a warning with
     *
     * @param sshHost the ssh host
     * @param command the command
     */
    public static BufferedReader executeCommand(SSHHost sshHost, String command)
        throws JSchException, GOSWarningException {

        SSHChannel channel = sshHost.openChannel("exec", "executeCommand " + command);
        channel.setCommand(command);
        BufferedReader reader = null;
        BufferedReader errReader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            errReader = new BufferedReader(new InputStreamReader(channel.getErrStream()));
        } catch (IOException ex) {
            ErrorManager.error("Error in executing command", ex);
        }
        channel.connect();
        boolean running = true;
        while (running) {
            running = (channel.getExitStatus() == -1);
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        channel.disconnect();
        if (channel.getExitStatus() != 0) {
            GOSException e = new GOSException("Error in command : " + command + " to " + sshHost.getFullHostName());
            StringBuilder output = new StringBuilder();
            try {
                while (errReader.ready()) {
                    output.append(errReader.readLine());
                }
            } catch (IOException ex) {
                //
            }
            if (channel.getExitStatus() == 100) { // especial exit status
                throw new GOSWarningException("Non-fatal error: " + output);
            }
            ErrorManager.error(sshHost.getFullHostName() + " Error in ssh command. Output: \n" + output, e);

        }
        return reader;
    }

    /**
     * Execute command.
     *
     * @param sshHost the ssh host
     * @param command the command
     * @param outputFile outfile
     * @param errorFile errorFile
     * @throws JSchException the j sch exception
     */
    public static SSHChannel executeCommand(SSHHost sshHost, String command, String executionArguments,
        String outputFile, String errorFile, String reason) throws JSchException {

        final String executionCommand = command + " " + executionArguments;
        String r = "executeCommand arraylist";
        if (!reason.equals(null)) {
            r = reason;
        }
        SSHChannel channel = sshHost.openChannel("exec", r);

        channel.setCommand(executionCommand);

        channel.setInputStream(null);
        OutputStream outputStream;
        OutputStream errorStream;
        try {
            outputStream = new FileOutputStream(outputFile);
            errorStream = new FileOutputStream(errorFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        channel.setOutputStream(outputStream);
        channel.setErrStream(errorStream);
        channel.connect();
        return channel;
    }

    public static SSHChannel executeCommand(GOSJobDescription jd) throws JSchException {
        return executeCommand(jd.getSSHHost(), jd.getExecutable(), jd.getArgumentsString(), jd.getOutput(),
            jd.getOutputError(), jd.getID() + " command");
    }

    /**
     * Kill job.
     *
     * @param sshHost the ssh host
     * @param killCommand the kill command
     * @param pathResponse the path response
     */
    public static SSHChannel killJob(SSHHost sshHost, String killCommand, String pathResponse, GOSJob job,
        String outFile, String errFile) throws JSchException, FileNotFoundException {

        SSHChannel channel = sshHost.openChannel("exec", "cancelJob " + job.getCompositeID());

        String executionCommand = "sh " + killCommand + " " + pathResponse;
        channel.setCommand(executionCommand);
        OutputStream outputStream = new FileOutputStream(outFile, true);
        OutputStream errorStream = new FileOutputStream(errFile, true);
        channel.setErrStream(errorStream);
        channel.setOutputStream(outputStream);
        channel.connect();
        return channel;
    }

}
