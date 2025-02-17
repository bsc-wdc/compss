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
package es.bsc.compss.gos.master.monitoring.transfermonitor;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.gos.master.GOSCopy;
import es.bsc.compss.gos.master.sshutils.SSHChannel;

import java.io.BufferedReader;
import java.io.IOException;


public class GOSSameHostCopyMonitor implements GOSTransferMonitor {

    public static final int SSH_FX_NO_SUCH_FILE = 2;
    private final GOSCopy copy;
    private final SSHChannel exec;
    private SSHChannel sftp;
    private final BufferedReader errStream;
    private final long targetSize;
    private final String dst;
    private boolean firstNotFound;


    /**
     * Instantiates a new Gos same host copy monitor.
     *
     * @param copy the copy
     * @param exec the exec
     * @param sftp the sftp
     * @param errStream the errStream
     * @param size the size
     */
    public GOSSameHostCopyMonitor(GOSCopy copy, SSHChannel exec, SSHChannel sftp, BufferedReader errStream, String dst,
        long size) {
        this.copy = copy;
        this.exec = exec;
        this.sftp = sftp;
        this.errStream = errStream;
        this.dst = dst;
        this.targetSize = size;
        this.firstNotFound = false;
    }

    private long getRemoteFileSize(String dst) throws SftpException, JSchException {
        if (!sftp.isConnected()) {
            sftp = sftp.recreateChannel();
            sftp.connect();
        }
        return sftp.lstat(dst).getSize();
    }

    @Override
    public boolean monitor() {
        long fileSize;
        try {
            fileSize = getRemoteFileSize(dst);
        } catch (SftpException e) {
            e.printStackTrace();
            if (e.id == SSH_FX_NO_SUCH_FILE) {
                if (!firstNotFound) {
                    // skips first time so that the command has time to create the file and avoid race conditions
                    firstNotFound = true;
                    return false;
                }
                copy.logError("File not found a second time.", e);
            } else {
                copy.logError("File detected but error", e);
                StringBuilder sb = new StringBuilder();
                // create a reader for tmpFile
                String str;
                while (true) {
                    try {
                        if ((str = errStream.readLine()) == null) {
                            break;
                        }
                        sb.append(str).append("\n");
                    } catch (IOException ex) {
                        copy.logError("File not found a second time.", ex);
                        break;
                    }
                }
                copy.logError(sb.toString());

            }
            copy.markAsFinished(false);
            copy.notifyEnd();
            return true;

        } catch (JSchException e) {
            copy.logError("Could not reconnect sftp channel.", e);
            copy.markAsFinished(false);
            copy.notifyEnd();
            return true;
        }
        if (fileSize == targetSize) {
            releaseResources();
            copy.markAsFinished(true);
            copy.notifyEnd();
            return true;
        }
        return false;

    }

    @Override
    public int getID() {
        return copy.getId();
    }

    @Override
    public void releaseResources() {
        if (exec != null) {
            exec.disconnect();
        }
        if (sftp != null) {
            sftp.disconnect();
        }
    }

    @Override
    public void shutdown() {
        releaseResources();
        copy.setState(GOSCopy.GOSCopyState.FAILED);
    }

    @Override
    public String getType() {
        return "Complex";
    }
}
