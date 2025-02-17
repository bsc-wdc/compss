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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor.GOSJschTransferMonitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class SSHChannel {

    private static final String DEFAULT_STARTING_PATH = "/";
    private final Channel ch;
    private final SSHSession s;
    private final String type;
    public String reason;


    /**
     * Instantiates a new SSHChannel of type @type.
     *
     * @param sshSession the corresponding session
     * @param type the type of channel to open
     */
    public SSHChannel(SSHSession sshSession, String type, String reason) throws JSchException {
        this.s = sshSession;
        this.type = type;
        if (!s.isConnected()) {
            s.connect();
        }
        this.ch = s.getSession().openChannel(type);
        this.reason = reason;

    }

    /**
     * Disconnect the channel and informs the session.
     */
    public void disconnect() {
        s.updateOpenChannels(this);
        releaseResources();
    }

    /**
     * Connect.
     *
     * @throws JSchException the j sch exception
     */
    public void connect() throws JSchException {
        if (!ch.isConnected()) {
            ch.connect();
        }
    }

    /**
     * Gets exit status. the exitstatus returned by the remote command, or -1, if the command not yet terminated (or
     * this channel type has no command).
     * 
     * @return the exit status
     */
    public int getExitStatus() {
        return ch.getExitStatus();
    }

    public boolean isConnected() {
        return ch.isConnected();
    }

    /**
     * Sets command.
     *
     * @param command the command
     * @throws JSchException the j sch exception
     */
    public void setCommand(String command) throws JSchException {
        ChannelExec exec;
        try {
            exec = (ChannelExec) ch;
        } catch (Exception e) {
            throw new JSchException("Channel was not of type Exec");
        }
        exec.setCommand(command);
    }

    /**
     * Cd.
     *
     * @param path the path
     * @throws SftpException the sftp exception
     */
    public void cd(String path) throws SftpException {
        ChannelSftp sftp;
        try {
            sftp = (ChannelSftp) ch;
        } catch (Exception e) {
            throw new SftpException(1, "Channel was not of type Sftp");
        }
        sftp.cd(path);
    }

    /**
     * Gain information of a given file in the path.
     *
     * @param path the path
     * @return the sftp attrs
     * @throws SftpException the sftp exception
     */
    public SftpATTRS lstat(String path) throws SftpException {
        ChannelSftp sftp;
        try {
            sftp = (ChannelSftp) ch;
        } catch (Exception e) {
            throw new SftpException(1, "Channel was not of type Sftp");
        }
        return sftp.lstat(path);
    }

    public void setOutputStream(OutputStream out) {
        ch.setOutputStream(out);
    }

    public void setInputStream(InputStream in) {
        ch.setInputStream(in);
    }

    /**
     * Sets err stream.
     *
     * @param err ErrorStream
     * @throws JSchException the j sch exception
     */
    public void setErrStream(OutputStream err) throws JSchException {
        ChannelExec exec;
        try {
            exec = (ChannelExec) ch;
        } catch (Exception e) {
            throw new JSchException("Channel was not of type Exec");
        }
        exec.setErrStream(err);
    }

    public boolean isClosed() {
        return ch.isClosed();
    }

    public InputStream getInputStream() throws IOException {
        return ch.getInputStream();
    }

    /**
     * Gets err stream.
     *
     * @return the err stream
     * @throws JSchException the j sch exception
     * @throws IOException the io exception
     */
    public InputStream getErrStream() throws JSchException, IOException {
        ChannelExec exec;
        try {
            exec = (ChannelExec) ch;
        } catch (Exception e) {
            throw new JSchException("Channel was not of type Exec");
        }
        return exec.getErrStream();
    }

    public void put(String src, String dst, GOSJschTransferMonitor monitor) throws SftpException {
        ChannelSftp sftp = (ChannelSftp) ch;
        sftp.put(src, dst, monitor);
    }

    /**
     * Get.
     *
     * @param src the src
     * @param dst the dst
     * @param monitor the monitor
     * @throws SftpException the sftp exception
     */
    public void get(String src, String dst, GOSJschTransferMonitor monitor) throws SftpException {
        ChannelSftp sftp = (ChannelSftp) ch;
        sftp.get(src, dst, monitor);
    }

    /**
     * Get.
     *
     * @param src the src
     * @param dst the dst
     * @param monitor the monitor
     * @param mode the mode
     * @throws SftpException the sftp exception
     */
    public void get(String src, String dst, GOSJschTransferMonitor monitor, int mode) throws SftpException {
        ChannelSftp sftp = (ChannelSftp) ch;
        sftp.get(src, dst, monitor, mode);
    }

    public String pwd() throws SftpException {
        ChannelSftp sftp = (ChannelSftp) ch;
        return sftp.pwd();
    }

    /**
     * Release resources.
     */
    public void releaseResources() {
        if (ch.isConnected()) {
            ch.disconnect();
        }
    }

    public SSHHost getHost() {
        return s.getHost();
    }

    public SSHChannel recreateChannel() throws JSchException {
        this.releaseResources();
        return s.recreateChannel(this, type);
    }

    public void delete(String path) throws SftpException {
        ChannelSftp channelSftp = (ChannelSftp) ch;
        channelSftp.rm(path);
    }

}
