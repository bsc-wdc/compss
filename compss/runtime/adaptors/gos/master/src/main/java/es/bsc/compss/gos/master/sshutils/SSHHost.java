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
package es.bsc.compss.gos.master.sshutils;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

import com.jcraft.jsch.SftpException;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.gos.master.GOSAdaptor;
import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.gos.master.GOSJobDescription;
import es.bsc.compss.gos.master.configuration.GOSConfiguration;
import es.bsc.compss.gos.master.exceptions.GOSWarningException;
import es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor.GOSJschTransferMonitor;
import es.bsc.compss.gos.master.sshutils.staticmethods.SSHCommand;
import es.bsc.compss.gos.master.sshutils.staticmethods.SSHFileSystem;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SSHHost {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private final String dbgPrefix;
    JSch jsch = new JSch();
    private final String host;
    private final String username;
    private Integer port = GOSConfiguration.DEFAULT_SSH_PORT;
    private final SSHSessionManager sessionManager;


    protected SSHHost(String user, String host) {
        if (user == null || user.isEmpty()) {
            LOGGER.warn("No username given defaulting to master user.");
            user = GOSAdaptor.getMasterUser();
        }
        this.host = host;
        this.username = user;
        this.dbgPrefix = "[SSHHost " + user + "@" + host + "] ";
        LOGGER.info(dbgPrefix + "Creating SSHost with user: " + user + " host: " + host);

        configJSCH();
        sessionManager = new SSHSessionManager(this);

        // debugging();
    }

    /**
     * Open channel to in a Session with less than max_Channels_per_session, if a session does not exist that matches
     * that condition creates a new one.
     * 
     * @param type type of channel to open
     * @return returns a channel, not connected
     * @throws JSchException If there is an error creating or opening a session or channel
     */
    public SSHChannel openChannel(String type, String reason) throws JSchException {
        SSHChannel ch;
        synchronized (sessionManager) {
            ch = sessionManager.openChannel(type, reason);
        }
        return ch;
    }

    private void configJSCH() {
        List<String> possibleIdentityFiles = new ArrayList<>();
        List<String> identityFiles = new ArrayList<>();
        String homeDir = System.getProperty("user.home");
        possibleIdentityFiles.add(homeDir + "/.ssh/id_rsa");
        possibleIdentityFiles.add(homeDir + "/.ssh/id_ecdsa");
        for (String file : possibleIdentityFiles) {
            File f = new File(file);
            if (f.exists()) {
                identityFiles.add(file);
            }
        }
        if (identityFiles.isEmpty()) {
            String list = Arrays.toString(possibleIdentityFiles.toArray());
            ErrorManager.fatal("No identity found in any of theses locations: " + list + ".");
            return;
        }
        try {
            jsch.setKnownHosts("~/.ssh/known_hosts");
            for (String file : identityFiles) {
                jsch.addIdentity(file);
            }
        } catch (JSchException e) {
            ErrorManager.fatal("Error in creating and configuring the SSHHost sessions", e);
        }
    }

    public String getUser() {
        return this.username;
    }

    public String getHost() {
        return this.host;
    }

    public String getFullHostName() {
        return "ssh://" + username + "@" + host;
    }

    /**
     * Execute command given a JobDescription. and set the channel to the given job for monitoring.
     *
     * @param jd the GOSJobDescription
     * @return open channel
     */
    public SSHChannel executeCommandInteractive(GOSJobDescription jd) throws JSchException {
        return SSHCommand.executeCommand(jd);
    }

    /**
     * Execute command given a JobDescription. and set the channel to the given job for monitoring.
     *
     * @param jd the GOSJobDescription
     * @return open channel
     */
    public SSHChannel executeCommandBatch(GOSJobDescription jd) throws JSchException {
        return SSHCommand.executeCommandBatch(jd.getSSHHost(), jd, jd.getOutput(), jd.getOutputError());

    }

    /**
     * Cancel job.
     *
     * @param job the job
     * @param cancelScript the path kill command
     */
    public SSHChannel killJob(GOSJob job, String cancelScript, String outFile, String errFile)
        throws JSchException, FileNotFoundException {
        String pathResponse = job.getResponseFile();
        LOGGER.info(dbgPrefix + "Executing kill Job " + cancelScript + " " + pathResponse);
        return SSHCommand.killJob(this, cancelScript, pathResponse, job, outFile, errFile);
    }

    /**
     * Execute starter command.
     *
     * @param command the command
     */
    public void executeStarterCommand(String command) throws InitNodeException {
        LOGGER.info(dbgPrefix + "Executing Start command " + command);
        try {
            SSHCommand.executeCommand(this, command);
        } catch (JSchException e) {
            LOGGER.warn(e);
            throw new InitNodeException(e);
        } catch (GOSWarningException e) {
            ErrorManager.warn(dbgPrefix + "Initialization command raised a non fatal error: " + e.getMessage() + ".");
        }
    }

    public void executeBlockingCommand(String command) throws JSchException, GOSWarningException {
        LOGGER.info(dbgPrefix + "Executing command " + command);
        SSHCommand.executeCommand(this, command);
    }

    /**
     * Execute command given and returns live channel.
     *
     * @param command the command
     * @return the channel exec
     * @throws JSchException the j sch exception
     */
    public BufferedReader executeCommand(String command) throws JSchException, GOSWarningException {
        LOGGER.info(dbgPrefix + "Executing command " + command);
        return SSHCommand.executeCommand(this, command);
    }

    /**
     * Append files.
     *
     * @param srcFiles the src files
     * @param dstFiles the dst files
     * @throws JSchException the j sch exception
     */
    public void appendFiles(ArrayList<String> srcFiles, ArrayList<String> dstFiles) throws JSchException {
        assert srcFiles.size() == dstFiles.size();
        for (int i = 0; i < srcFiles.size(); i++) {
            try {
                SSHFileSystem.appendFile(this, srcFiles.get(i), dstFiles.get(i));
            } catch (SftpException e) {
                ErrorManager.warn("Could not successfully bring " + srcFiles.get(i) + " skipping this file", e);
            }
        }
    }

    public void releaseAllResources() {
        sessionManager.releaseAllResources();
    }

    public Collection<SSHSession> getSessions() {
        return sessionManager.getSessions();
    }

    public Collection<SSHChannel> getAllChannels() {
        return sessionManager.getAllChannels();
    }

    /**
     * Remove file in the given path.
     */
    public void removeFile(String path) throws JSchException, SftpException {
        SSHChannel ch = openChannel("sftp", "removeChannel");
        ch.delete(path);
        ch.disconnect();
    }

    /**
     * Gets file from remote. It's a blocking operation. Use class GOSCOPY for asynchronous transfers.
     *
     * @param src the src in the remote machine
     * @param dst the dst in this machine
     */
    public void getFile(String src, String dst) throws JSchException, SftpException {
        SSHChannel ch = openChannel("sftp", "getFile");
        ch.connect();
        ch.cd("/");
        GOSJschTransferMonitor monitor = new GOSJschTransferMonitor();
        try {
            ch.get(src, dst, monitor);
        } catch (SftpException e) {
            throw new RuntimeException(e);
        }
        while (!monitor.finished) {
            try {
                TimeUnit.MILLISECONDS.sleep(10000);
            } catch (InterruptedException e) {
                //
            }
        }
        ch.disconnect();
    }

    protected int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
