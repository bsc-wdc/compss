/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.log.Loggers;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSGlobalTransferMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private final HashMap<Integer, GOSTransferMonitor> activeTransfers;


    public GOSGlobalTransferMonitor() {
        activeTransfers = new HashMap<>();
    }

    public void addTransferMonitor(GOSTransferMonitor m) {
        activeTransfers.put(m.getID(), m);
    }

    /**
     * Monitor if there has been changes of active transfers.
     * 
     * @return if there is activeTransfers
     */
    public boolean monitor() {
        for (Object o : activeTransfers.values().toArray()) {
            GOSTransferMonitor tm = (GOSTransferMonitor) o;
            if (tm.monitor()) {
                removeTransferMonitor(tm.getID());
            }
        }
        if (existsActiveTransfers()) {
            return true;
        }
        return false;
    }

    public synchronized void removeTransferMonitor(int id) {
        activeTransfers.remove(id);
    }

    public boolean existsActiveTransfers() {
        return !activeTransfers.isEmpty();
    }

    /**
     * End all active transfers.
     */
    public void end() {
        LOGGER.info("Ending global transfer monitor");
        for (GOSTransferMonitor tm : activeTransfers.values()) {
            tm.shutdown();
        }
        activeTransfers.clear();
    }
}
