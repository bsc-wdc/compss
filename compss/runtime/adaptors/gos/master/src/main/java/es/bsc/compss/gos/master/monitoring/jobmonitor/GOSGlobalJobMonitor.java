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
package es.bsc.compss.gos.master.monitoring.jobmonitor;

import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.log.Loggers;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSGlobalJobMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private final HashMap<String, GOSHostsManager> hostsMonitor = new HashMap<>();
    private static final String DBG_PREFIX = "[GOS Transfer Monitor]";


    /**
     * Monitor running hosts.
     * 
     * @return if there is active jobs running
     */
    public boolean monitor() {
        LOGGER.debug(DBG_PREFIX + "Monitoring GOS jobs");
        for (GOSHostsManager hm : hostsMonitor.values()) {
            hm.monitor();
        }
        return existsRunningJobs();
    }

    /**
     * add Job monitor.
     * 
     * @param job job to monitor.
     */
    public void addJobMonitor(GOSJob job) {
        String hostID = job.getHostName();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(DBG_PREFIX + job.jobPrefix + "Added job monitor");
        }
        if (!hostsMonitor.containsKey(hostID)) {
            // Init HostJobManager
            if (job.isBatch()) {
                hostsMonitor.put(hostID, new GOSHostBatchMonitor(job));
            } else {
                hostsMonitor.put(hostID, new GOSHostsInteractiveManager(job));
            }
        } else {
            hostsMonitor.get(hostID).addJobMonitor(job);
        }
    }

    /**
     * Remove job monitor.
     * 
     * @param job Job to remove.
     */
    public void removeJobMonitor(GOSJob job) {
        String hostID = job.getHostName();
        if (hostID != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(DBG_PREFIX + job.jobPrefix + "Removing job monitor");
            }
            if (hostsMonitor.containsKey(hostID)) {
                hostsMonitor.get(hostID).removeJobMonitor(job);
            }
        }
    }

    /**
     * Returns if exists any job monitored.
     * 
     * @return true if there is a job been monitored.
     */
    public boolean existsRunningJobs() {
        for (GOSHostsManager hm : hostsMonitor.values()) {
            if (hm.existsRunningJobs()) {
                return true;
            }
        }
        LOGGER.debug(DBG_PREFIX + "No more jobs running.");
        return false;
    }

    /**
     * End.
     */
    public void end() {
        for (GOSHostsManager hm : hostsMonitor.values()) {
            hm.shutdown();
        }
        hostsMonitor.clear();
    }
}
