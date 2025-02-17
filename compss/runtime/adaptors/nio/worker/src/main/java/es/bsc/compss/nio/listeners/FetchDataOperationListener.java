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
package es.bsc.compss.nio.listeners;

import es.bsc.comm.Connection;
import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.worker.NIOWorker;


public class FetchDataOperationListener extends MultiOperationFetchListener {

    private final int transferId;
    private final NIOWorker nw;


    /**
     * Creates a new Fetch Data operation listener.
     * 
     * @param transferId Transfer data Id.
     * @param nw Associated NIO Worker.
     */
    public FetchDataOperationListener(int transferId, NIOWorker nw) {
        this.transferId = transferId;
        this.nw = nw;
    }

    @Override
    public void doCompleted() {
        CommandDataReceived cdr = new CommandDataReceived(this.transferId);
        Connection c = this.nw.startConnection();
        NIOAgent.registerOngoingCommand(c, cdr);
        c.sendCommand(cdr);
        c.finishConnection();
    }

    @Override
    public void doFailure(String failedDataId, Exception cause) {
        // Nothing to do
    }

}
