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

import com.jcraft.jsch.SftpException;
import es.bsc.compss.gos.master.GOSCopy;
import es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor.GOSJschTransferMonitor;
import es.bsc.compss.gos.master.sshutils.SSHChannel;

import java.io.File;


public class GOSDifferentHostsTransfer implements GOSTransferMonitor {

    private final GOSJschTransferMonitor transferMonitor;
    private final GOSCopy copy;
    private SSHChannel channelGet;
    private final SSHChannel channelSend;
    private final String dst;
    private final File tmpFile;

    private State state;


    private enum State {
        GET, SEND,
    }


    /**
     * Instantiates a new Gos different hosts transfer.
     */
    public GOSDifferentHostsTransfer(GOSCopy copy, SSHChannel channelGet, SSHChannel channelSend, String src,
        String dst, File tmp, GOSJschTransferMonitor transferMonitor) {
        this.copy = copy;
        this.channelGet = channelGet;
        this.channelSend = channelSend;
        this.dst = dst;
        this.tmpFile = tmp;
        this.transferMonitor = transferMonitor;
        state = State.GET;
    }

    @Override
    public boolean monitor() {
        if (state.equals(State.GET)) {
            if (transferMonitor.finished) {
                launchSend();
            }
            return false;
        }
        if (transferMonitor.finished) {
            releaseResources();
            copy.markAsFinished(transferMonitor.success);
            copy.notifyEnd();
            return true;
        }
        return false;
    }

    private void launchSend() {
        channelGet.disconnect();
        channelGet = null;
        state = State.SEND;
        GOSJschTransferMonitor m = new GOSJschTransferMonitor();
        try {
            channelSend.put(tmpFile.getPath(), dst, m);
        } catch (SftpException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getID() {
        return copy.getId();
    }

    @Override
    public void releaseResources() {
        if (channelSend != null) {
            channelSend.disconnect();
        }
        if (channelGet != null) {
            channelGet.disconnect();
        }
        boolean delete = tmpFile.delete();
        if (delete) {
            copy.logWarn("Could not delete temporary file " + tmpFile.getPath() + ".");
        }
    }

    @Override
    public void shutdown() {
        releaseResources();
        copy.setState(GOSCopy.GOSCopyState.FAILED);
    }

    @Override
    public String getType() {
        return "DifferentHosts";
    }
}
