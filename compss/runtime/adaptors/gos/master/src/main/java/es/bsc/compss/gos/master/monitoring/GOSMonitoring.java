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
package es.bsc.compss.gos.master.monitoring;

import es.bsc.compss.gos.master.GOSAdaptor;
import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.gos.master.monitoring.jobmonitor.GOSGlobalJobMonitor;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSGlobalTransferMonitor;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSTransferMonitor;
import es.bsc.compss.log.Loggers;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSMonitoring {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final int SLEEP_TIME = 350;

    public Boolean awaken = false;

    private final GOSGlobalTransferMonitor globalTransferMonitor;
    private final GOSGlobalJobMonitor globalJobMonitor;
    public Boolean running = true;

    private final GOSAdaptor adaptor;
    public boolean shutdown = false;
    private Thread monitoringThread;


    /**
     * Instantiates a new Gos monitoring.
     *
     * @param adaptor the adaptor
     */
    public GOSMonitoring(GOSAdaptor adaptor) {
        this.adaptor = adaptor;
        this.globalTransferMonitor = new GOSGlobalTransferMonitor();
        this.globalJobMonitor = new GOSGlobalJobMonitor();
    }

    private boolean monitoringActiveJobs() {
        return globalJobMonitor.monitor();
    }

    private boolean monitoringActiveTransfers() {
        return globalTransferMonitor.monitor();
    }

    /**
     * Monitoring jobs and transfers.
     *
     * @return the boolean
     */
    public boolean monitoringJobsAndTransfers() {
        boolean ret;
        synchronized (this) {
            ret = monitoringActiveTransfers();
            ret = ret || monitoringActiveJobs();
        }
        return running && ret;
    }

    /**
     * Add job monitor.
     *
     * @param job the job
     */
    public void addJobMonitor(GOSJob job) {
        synchronized (this) {
            globalJobMonitor.addJobMonitor(job);
            if (!awaken) {
                awakenMonitoring();
            }
        }
    }

    /**
     * Add job monitor.
     *
     * @param job the job
     */
    public void removeJobMonitor(GOSJob job) {
        globalJobMonitor.removeJobMonitor(job);
    }

    /**
     * Add transfer monitor.
     *
     * @param monitor the monitor to add
     */
    public void addTransferMonitor(GOSTransferMonitor monitor) {
        synchronized (this) {
            globalTransferMonitor.addTransferMonitor(monitor);
            if (!awaken) {
                awakenMonitoring();
            }
        }
    }

    private void awakenMonitoring() {
        LOGGER.info("[GOSMonitoring] awaking monitoring jobs");
        awaken = true;
        GOSMonitoringThread thread = new GOSMonitoringThread(this, SLEEP_TIME);
        LOGGER.info("[GOSMonitoring] Monitoring AWAKEN");
        // Launch monitor in a thread and continuously run the monitor function
        monitoringThread = new Thread(thread);
        monitoringThread.start();
    }

    /**
     * End all current processes, and monitoring thread is interrupted.
     */
    public void end() {
        shutdown = true;
        synchronized (this) {
            globalJobMonitor.end();
            globalTransferMonitor.end();
            running = false;
        }
        if (monitoringThread != null) {
            monitoringThread.interrupt();
        }

    }

    /**
     * Make monitoring thread go dormant.
     */
    public void dormant() {
        synchronized (this) {
            awaken = false;
            LOGGER.info("Monitoring thread going dormant");
        }
    }

    /**
     * Is monitoring thread alive boolean.
     *
     * @return the boolean
     */
    public boolean isMonitoringThreadAlive() {
        if (monitoringThread == null) {
            return false;
        } else {
            return monitoringThread.isAlive();
        }
    }

}
