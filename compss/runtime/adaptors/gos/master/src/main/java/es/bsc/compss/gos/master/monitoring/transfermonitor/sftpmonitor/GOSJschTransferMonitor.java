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
package es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor;

import com.jcraft.jsch.SftpProgressMonitor;


public class GOSJschTransferMonitor implements SftpProgressMonitor {

    public boolean finished = false;
    private long count;
    public String dst;
    private int op;
    public String src;
    private long fileSize;
    public boolean success;


    public GOSJschTransferMonitor() {
    }

    @Override
    public void init(int op, String src, String dst, long max) {
        this.count = 0;
        this.op = op;
        this.src = src;
        this.dst = dst;
        this.fileSize = max;
    }

    @Override
    public boolean count(long l) {
        this.count = l;
        return true;
    }

    @Override
    public void end() {
        finished = true;
        if (fileSize == UNKNOWN_SIZE) {
            success = true;
        } else {
            success = (fileSize == count);
        }
    }

    /**
     * Gets operation type.
     *
     * @return the operation type
     */
    public String getOperationType() {
        if (op == GET) {
            return "GET";
        }
        return "PUT";
    }
}
