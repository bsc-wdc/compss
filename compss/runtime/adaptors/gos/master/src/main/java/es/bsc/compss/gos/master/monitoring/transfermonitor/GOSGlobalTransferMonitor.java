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

import es.bsc.compss.log.Loggers;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSGlobalTransferMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private final ConcurrentHashMap<Integer, GOSTransferMonitor> activeTransfers;
    private static final String DBG_PREFIX = "[GOS Transfer Monitor]";


    public GOSGlobalTransferMonitor() {
        activeTransfers = new ConcurrentHashMap<>();
    }

    /**
     * Add a transfer monitor to check.
     * 
     * @param m GOS transfer monitor
     */
    public void addTransferMonitor(GOSTransferMonitor m) {
        synchronized (activeTransfers) {
            activeTransfers.put(m.getID(), m);
        }
    }

    /**
     * Monitor if there has been changes of active transfers.
     * 
     * @return if there is activeTransfers
     */
    public boolean monitor() {
        LOGGER.debug(DBG_PREFIX + "Monitoring GOS transfers");
        if (!existsActiveTransfers()) {
            LOGGER.debug(DBG_PREFIX + "No more active transfers");
            return false;
        } else {
            GOSTransferMonitor[] transfers = null;
            synchronized (activeTransfers) {
                Collection<GOSTransferMonitor> colTransfers = activeTransfers.values();
                if (!colTransfers.isEmpty()) {
                    transfers = colTransfers.toArray(new GOSTransferMonitor[colTransfers.size()]);
                }
            }
            if (transfers != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(DBG_PREFIX + "Transfers to monitor: " + transfers.length);
                }
                for (GOSTransferMonitor tm : transfers) {
                    if (tm.monitor()) {
                        removeTransferMonitor(tm.getID());
                    }
                }
            }
            if (existsActiveTransfers()) {
                LOGGER.debug(DBG_PREFIX + "There are more active transfers. Keep running...");
                return true;
            } else {
                LOGGER.debug(DBG_PREFIX + "No more active transfers");
                return false;
            }
        }
    }

    /**
     * Remove Transfer monitor.
     * 
     * @param id Identifier of the monitor.
     */
    public void removeTransferMonitor(int id) {
        synchronized (activeTransfers) {
            activeTransfers.remove(id);
        }
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
