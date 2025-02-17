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
package es.bsc.compss.connectors.utils;

import es.bsc.compss.connectors.AbstractConnector;
import es.bsc.compss.connectors.VM;
import es.bsc.compss.log.Loggers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Deadline thread for VM timeout.
 */
public class DeadlineThread extends Thread {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CONNECTORS);

    // Timer properties
    private static final long MIN_DEADLINE_INTERVAL = 10_000L;
    private static final long MAX_DEADLINE_INTERVAL = 60_000L;
    private static final long DELETE_SAFETY_INTERVAL = 30_000L;
    private static final long INITIAL_SLEEP_TIME = 1_000L;

    private AbstractConnector ac;
    private boolean keepGoing;


    /**
     * Creates a new DeadlineThread with an associated Abstract Connector.
     * 
     * @param ac Associated AbstractConnector.
     */
    public DeadlineThread(AbstractConnector ac) {
        this.ac = ac;
        this.keepGoing = true;
    }

    /**
     * Returns the minimum deadline interval.
     * 
     * @return The minimum deadline interval.
     */
    public static long getMinDeadlineInterval() {
        return MIN_DEADLINE_INTERVAL;
    }

    /**
     * Returns the maximum deadline interval.
     * 
     * @return The maximum deadline interval.
     */
    public static long getMaxDeadlineInterval() {
        return MAX_DEADLINE_INTERVAL;
    }

    /**
     * Returns the safety interval for VM deletion.
     * 
     * @return The safety interval for VM deletion.
     */
    public static long getDeleteSafetyInterval() {
        return DELETE_SAFETY_INTERVAL;
    }

    @Override
    public void run() {
        Thread.currentThread()
            .setName("[Abstract Connector] Connector " + this.ac.getProvider().getName() + " deadline");

        long sleepTime = INITIAL_SLEEP_TIME;
        while (this.keepGoing) {
            // Sleep until next iteration
            try {
                LOGGER.debug("[Abstract Connector] Deadline thread sleeps " + sleepTime + " ms.");
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            // Recompute sleep time
            sleepTime = getSleepTime();

            // Retrieve data from AbstractConnector
            List<VM> vmsAlive = this.ac.getCopyOfAliveVms();

            // Check if iteration can be skipped
            if (vmsAlive.isEmpty()) {
                // Move to next iteration
                LOGGER.debug("[Abstract Connector] No VMs alive deadline sleep set to " + sleepTime + " ms.");
                continue;
            }

            // Otherwise, perform operations
            LOGGER.debug("[Abstract Connector] VMs alive initial sleep set to " + sleepTime + " ms.");

            for (VM vmInfo : vmsAlive) {
                long timeLeft = timeLeft(vmInfo.getStartTime());
                // LOGGER.info("MONITOR STATUS DEAD next VM " + vmInfo.ip + " @ " + vmInfo.startTime + " --> " +
                // timeLeft);
                if (timeLeft <= DELETE_SAFETY_INTERVAL) {
                    if (vmInfo.isToDelete()) {
                        LOGGER.info("[Abstract Connector] Deleting vm " + vmInfo.getName()
                            + " because is marked to delete" + " and it is on the safety delete interval");
                        // Ask AbstractConnector to remove it
                        this.ac.removeFromAliveVms(vmInfo);
                        this.ac.removeFromDeleteVms(vmInfo);
                        // Launch deletion thread for the VM
                        DeletionThread dt = new DeletionThread((Operations) this.ac, vmInfo);
                        dt.start();
                    }
                } else if (sleepTime > timeLeft - DELETE_SAFETY_INTERVAL) {
                    sleepTime = timeLeft - DELETE_SAFETY_INTERVAL;
                    LOGGER.debug("[Abstract Connector] Evaluating sleep time for " + vmInfo.getName()
                        + " because an interval near to finish " + sleepTime + " ms.");
                }
            }
        }
    }

    /**
     * Marks the deadline thread to terminate.
     */
    public void terminate() {
        this.keepGoing = false;
        this.interrupt();
    }

    private long getSleepTime() {
        long time = this.ac.getTimeSlot();
        if (time <= 0) {
            return MIN_DEADLINE_INTERVAL;
        } else {
            time = time - DELETE_SAFETY_INTERVAL;
            if (time > MAX_DEADLINE_INTERVAL) {
                return MAX_DEADLINE_INTERVAL;
            } else {
                return time;
            }
        }
    }

    private long timeLeft(long time) {
        long now = System.currentTimeMillis();
        long limit = this.ac.getTimeSlot();
        if (limit <= 0) {
            return 0;
        }
        long result = limit - ((now - time) % limit);
        LOGGER.debug("Calculating sleep time at " + time + " now is " + now + ": remaining --> " + limit + " - "
            + (now - time) + " % " + limit + " = " + result + " ms to deadline");
        return result;
    }

}
