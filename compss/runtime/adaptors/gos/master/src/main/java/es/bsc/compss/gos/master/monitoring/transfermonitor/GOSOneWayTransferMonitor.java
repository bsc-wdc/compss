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

import es.bsc.compss.gos.master.GOSCopy;
import es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor.GOSJschTransferMonitor;
import es.bsc.compss.gos.master.sshutils.SSHChannel;


public class GOSOneWayTransferMonitor implements GOSTransferMonitor {

    private final GOSJschTransferMonitor transferMonitor;
    private final SSHChannel channel;
    int id;
    GOSCopy copy;


    /**
     * Instantiates a new Gos one way transfer monitor.
     *
     * @param copy the copy
     * @param monitor the monitor
     * @param channelSftp the channel
     */
    public GOSOneWayTransferMonitor(GOSCopy copy, GOSJschTransferMonitor monitor, SSHChannel channelSftp) {
        this.id = copy.getId();
        this.copy = copy;
        this.transferMonitor = monitor;
        this.channel = channelSftp;
    }

    /**
     * Monitor transfer if it has ended.
     *
     * @return whether it has ended the transfer in channel
     */
    public boolean monitor() {
        if (transferMonitor.finished) {
            releaseResources();
            copy.markAsFinished(transferMonitor.success);
            copy.notifyEnd();
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "GOSSimpleTransferMonitor_forCopy" + id;
    }

    @Override
    public int getID() {
        return id;
    }

    @Override
    public void releaseResources() {
        if (channel != null) {
            channel.disconnect();
        }
    }

    @Override
    public void shutdown() {
        releaseResources();
        copy.setState(GOSCopy.GOSCopyState.FAILED);
    }

    @Override
    public String getType() {
        return "ONE Way Transfer";
    }
}
