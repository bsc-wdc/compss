/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.executor.external.piped;

import es.bsc.compss.executor.external.piped.commands.AliveGetPipeCommand;
import es.bsc.compss.executor.external.piped.commands.AliveReplyPipeCommand;
import es.bsc.compss.executor.external.piped.exceptions.ClosedPipeException;
import es.bsc.compss.log.Loggers;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MirrorMonitor {

    private static final long MONITORING_PERIOD = 5_000;


    public interface MonitorElementListener {

        /**
         * Notifies that the monitored process has died.
         */
        void onDead();
    }

    public static class MonitoredElementInfo {

        private final Integer pid;
        private final MonitorElementListener listener;


        public MonitoredElementInfo(Integer pid, MonitorElementListener listener) {
            this.listener = listener;
            this.pid = pid;
        }

        public int getPID() {
            return pid;
        }

        @Override
        public String toString() {
            return "MonitoredElement:PID" + pid;
        }
    }


    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    private final Thread monitorThread;

    private Process mainProcess;
    private ControlPipePair controlPipe;
    private boolean keepAlive = false;
    private boolean stopped = false;

    private List<MonitoredElementInfo> toMonitor = new LinkedList<>();
    private List<MonitoredElementInfo> toMonitorBackCopy = new LinkedList<>();


    /**
     * Mirror Monitor constructor.
     */
    public MirrorMonitor() {
        monitorThread = new Thread() {

            @Override
            public void run() {
                try {
                    monitor();
                } catch (Exception e) {
                    logger.error("Error: exception in monitor", e);
                }
                synchronized (MirrorMonitor.this) {
                    stopped = true;
                    MirrorMonitor.this.notify();
                }
            }
        };
    }

    public void mainProcess(Process piper, ControlPipePair pipe) {
        mainProcess = piper;
        controlPipe = pipe;
    }

    /**
     * Start a Mirror monitor.
     */
    public void start() {
        logger.debug("Starting monitor {}", this);
        synchronized (this) {
            this.keepAlive = true;
            monitorThread.start();
        }
    }

    /**
     * Stop a Mirror monitor.
     */
    public void stop() {
        logger.debug("Stopping Mirror monitor {}", this);
        synchronized (this) {
            keepAlive = false;
            monitorThread.interrupt();
            if (!stopped) {
                try {
                    logger.debug("Waiting monitor {} to stop...", this);
                    this.wait();
                } catch (InterruptedException ie) {
                    // Do nothing
                }
            }
        }
    }

    /**
     * Register a PID to be monitored.
     *
     * @param pid Process Identifier to monitor
     * @param listener listener to notify when the monitored element dies
     */
    public void monitor(int pid, MonitorElementListener listener) {
        MonitoredElementInfo info = new MonitoredElementInfo(pid, listener);
        synchronized (this) {
            toMonitor.add(info);
        }
    }

    private void monitor() {
        long lastMonitored = System.currentTimeMillis();

        while (keepAlive) {
            long sleepTime = (lastMonitored + MONITORING_PERIOD) - System.currentTimeMillis();
            if (sleepTime > 0) {
                try {
                    logger.debug("Piped Mirror monitor {} waits until next activation", this);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    break;
                }
            }
            if (Thread.interrupted()) {
                continue;
            }
            lastMonitored = System.currentTimeMillis();

            // Check whether the main Process is still alive or not
            if (mainProcess != null && !mainProcess.isAlive()) {
                controlPipe.noLongerExists();
                controlPipe.delete();
                controlPipe = null;
                mainProcess = null;
                break;
            }
            if (Thread.interrupted()) {
                continue;
            }

            // Check whether worker processes are still alive or not
            List<MonitoredElementInfo> elementsInfo;
            logger.debug("Mirror monitor {} active", this);
            synchronized (this) {
                elementsInfo = toMonitor;
                toMonitorBackCopy.clear();
                toMonitor = toMonitorBackCopy;
                toMonitorBackCopy = elementsInfo;
            }
            logger.debug("Mirror monitor {} querying pids {}", this, elementsInfo);
            List<Integer> aliveProcesses;
            int elementsCount = elementsInfo.size();
            if (elementsCount > 0) {
                List<Integer> pids = new ArrayList<>(elementsCount);
                for (MonitoredElementInfo eInfo : elementsInfo) {
                    pids.add(eInfo.pid);
                }
                AliveGetPipeCommand aliveRequest = new AliveGetPipeCommand(pids);
                if (logger.isDebugEnabled()) {
                    StringBuilder aliveQuery =
                        new StringBuilder("Mirror monitor " + this + " obtaining alive processes. Checking processes ");
                    for (MonitoredElementInfo info : elementsInfo) {
                        aliveQuery.append(" ").append(info.getPID());
                    }
                    logger.debug(aliveQuery.toString());
                }
                if (controlPipe.sendCommand(aliveRequest)) {
                    AliveReplyPipeCommand reply = new AliveReplyPipeCommand();
                    try {
                        controlPipe.waitForCommand(reply);
                    } catch (ClosedPipeException ie) {
                        logger.debug("Mirror monitor {} could not obtain the alive processes - Pipe was closed", this);
                    }
                    aliveProcesses = reply.getAliveProcesses();
                } else {
                    logger.debug("Mirror monitor {} could not obtain the alive processes - Message couldn't be sent",
                        this);
                    continue;
                }

                List<MonitoredElementInfo> aliveElements = new LinkedList<>();
                for (MonitoredElementInfo element : elementsInfo) {
                    int pid = element.getPID();
                    if (aliveProcesses.contains(pid)) {
                        aliveElements.add(element);
                    } else {
                        logger.debug("Mirror monitor {} detected that process {} has died.", this, pid);
                        element.listener.onDead();
                    }
                }
                synchronized (this) {
                    logger.debug("Mirror monitor {} will keep querying {}", this, aliveElements);
                    toMonitor.addAll(aliveElements);
                }
            }
        }
    }

}
