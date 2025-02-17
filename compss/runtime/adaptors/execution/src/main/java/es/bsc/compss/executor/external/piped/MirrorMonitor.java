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

package es.bsc.compss.executor.external.piped;

import es.bsc.compss.executor.external.piped.commands.AliveGetPipeCommand;
import es.bsc.compss.executor.external.piped.commands.AliveReplyPipeCommand;
import es.bsc.compss.executor.external.piped.exceptions.ClosedPipeException;
import es.bsc.compss.log.Loggers;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MirrorMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    private static final long MONITORING_PERIOD = 5_000;
    private final Thread monitorThread;

    private Process mainProcess;
    private ControlPipePair controlPipe;
    private boolean keepAlive = false;
    private boolean stopped = false;
    private Map<String, PipeWorkerInfo> workers = new TreeMap<>();
    private Map<String, PipeExecutorInfo> executors = new TreeMap<>();
    private final List<String> unremovedElements = new LinkedList<>();


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
                    LOGGER.error("Error: exception in monitor", e);
                }
                synchronized (MirrorMonitor.this) {
                    stopped = true;
                    MirrorMonitor.this.notify();
                }
            }
        };
    }

    private void monitor() {
        long lastMonitored = System.currentTimeMillis();

        while (keepAlive) {
            long sleepTime = (lastMonitored + MONITORING_PERIOD) - System.currentTimeMillis();
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    break;
                }
            }
            if (Thread.interrupted()) {
                break;
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
                break;
            }

            // Check whether worker processes are still alive or not
            Map<String, PipeWorkerInfo> workers;
            Map<String, PipeExecutorInfo> executors;
            synchronized (this) {
                workers = this.workers;
                executors = this.executors;
                this.workers = new TreeMap<>();
                this.executors = new TreeMap<>();
                unremovedElements.clear();
            }

            LinkedList<PipeElementInfo> elementsInfo = new LinkedList<>();
            List<Integer> aliveProcesses;
            elementsInfo.addAll(workers.values());
            elementsInfo.addAll(executors.values());
            if (elementsInfo.size() > 0) {

                AliveGetPipeCommand aliveRequest = new AliveGetPipeCommand(elementsInfo);
                if (LOGGER.isDebugEnabled()) {
                    StringBuilder aliveQuery =
                        new StringBuilder("Piped mirrors monitor obtaining alive processes. Checking processes ");
                    for (PipeElementInfo info : elementsInfo) {
                        aliveQuery.append(" ").append(info.getPID());
                    }
                    LOGGER.debug(aliveQuery.toString());
                }
                if (controlPipe.sendCommand(aliveRequest)) {
                    AliveReplyPipeCommand reply = new AliveReplyPipeCommand();
                    try {
                        controlPipe.waitForCommand(reply);
                    } catch (ClosedPipeException ie) {
                        LOGGER.debug("Piped mirrors monitor could not obtain the alive processes - Pipe was closed");
                    }
                    aliveProcesses = reply.getAliveProcesses();
                } else {
                    LOGGER
                        .debug("Piped mirrors monitor could not obtain the alive processes - Message couldn't be sent");
                    continue;
                }

                // Check whether worker processes are still alive or not
                Iterator<Entry<String, PipeWorkerInfo>> workerItr = workers.entrySet().iterator();
                while (workerItr.hasNext() && !Thread.currentThread().isInterrupted()) {
                    Entry<String, PipeWorkerInfo> pair = workerItr.next();
                    PipeWorkerInfo info = pair.getValue();
                    if (aliveProcesses.contains(info.getPID())) {
                        synchronized (this) {
                            this.workers.put(pair.getKey(), pair.getValue());
                        }
                    } else {
                        boolean removed;
                        synchronized (this) {
                            String id = pair.getKey();
                            removed = this.unremovedElements.remove(id);
                        }
                        if (!removed) {
                            LOGGER.debug("Piped mirrors monitor has detected that worker process " + info.getPID()
                                + " has died.");
                            ControlPipePair workerPipe = info.getPipe();
                            workerPipe.noLongerExists();
                            workerPipe.delete();
                        }
                        info.getMirror().workerProcessEnded();
                    }
                }

                // Check whether executor processes are still alive or not
                Iterator<Entry<String, PipeExecutorInfo>> executorItr = executors.entrySet().iterator();
                while (executorItr.hasNext() && !Thread.currentThread().isInterrupted()) {
                    Entry<String, PipeExecutorInfo> pair = executorItr.next();
                    PipeExecutorInfo info = pair.getValue();
                    if (aliveProcesses.contains(info.getPID())) {
                        synchronized (this) {
                            this.executors.put(pair.getKey(), pair.getValue());
                        }
                    } else {
                        boolean removed;
                        synchronized (this) {
                            String id = pair.getKey();
                            removed = this.unremovedElements.remove(id);
                        }
                        if (!removed) {
                            // If it was not removed yet
                            PipePair workerPipe = info.getPipe();
                            System.err.println("Piped mirrors monitor has detected that executor process "
                                + info.getPID() + " has died.");
                            LOGGER.error("Piped mirrors monitor has detected that executor process " + info.getPID()
                                + " has died.");
                            workerPipe.noLongerExists();
                            workerPipe.delete();
                            PipedMirror mirror = info.getMirror();
                            String executorId = info.getExecutorId();
                            mirror.unregisterExecutor(executorId);
                        }
                    }
                }
            }
            if (Thread.interrupted()) {
                break;
            }
        }
    }

    /**
     * Start a Mirror monitor.
     */
    public void start() {
        synchronized (this) {
            this.keepAlive = true;
            monitorThread.start();
        }
    }

    /**
     * Stop a Mirror monitor.
     */
    public void stop() {
        LOGGER.debug("Stopping monitor");
        synchronized (this) {
            keepAlive = false;
            monitorThread.interrupt();
            if (!stopped) {
                try {
                    LOGGER.debug("Waiting monitor to stop...");
                    this.wait();
                } catch (InterruptedException ie) {
                    // Do nothing
                }
            }
        }
    }

    public void mainProcess(Process piper, ControlPipePair pipe) {
        mainProcess = piper;
        controlPipe = pipe;
    }

    /**
     * Register a worker to the Mirror monitor.
     * 
     * @param worker External Worker
     * @param workerPID Worker Process Identifier
     * @param workerControlPipe Worker control pipe
     */
    public void registerWorker(PipedMirror worker, int workerPID, ControlPipePair workerControlPipe) {
        PipeWorkerInfo info = new PipeWorkerInfo(worker, workerPID, workerControlPipe);
        synchronized (this) {
            String workerName = worker.getMirrorId();
            this.workers.put(workerName, info);
        }
    }

    /**
     * Unregister a worker to the Mirror monitor.
     * 
     * @param workerName Worker name
     */
    public void unregisterWorker(String workerName) {
        synchronized (this) {
            PipeWorkerInfo info = this.workers.remove(workerName);
            if (info == null) {
                this.unremovedElements.add(workerName);
            }
        }
    }

    /**
     * Register executor to the Mirror monitor.
     * 
     * @param mirror Piped mirror
     * @param executorId Executor Identifier
     * @param executorPId Executor Process identifier
     * @param pipe Pipe attached to executor
     */
    public void registerExecutor(PipedMirror mirror, String executorId, int executorPId, PipePair pipe) {
        PipeExecutorInfo info = new PipeExecutorInfo(mirror, executorId, executorPId, pipe);
        synchronized (this) {
            this.executors.put(mirror.getMirrorId() + "_" + executorId, info);
        }
    }

    /**
     * Unregister executor from the Mirror monitor.
     * 
     * @param mirror Piped Mirror
     * @param executorId Executor Identifier
     */
    public void unregisterExecutor(PipedMirror mirror, String executorId) {
        String id = mirror.getMirrorId() + "_" + executorId;
        synchronized (this) {
            PipeExecutorInfo info = this.executors.remove(id);
            if (info == null) {
                this.unremovedElements.add(id);
            }
        }
    }


    private static class PipeWorkerInfo extends PipeElementInfo {

        private final ControlPipePair pipe;


        public PipeWorkerInfo(PipedMirror mirror, Integer pid, ControlPipePair pipe) {
            super(mirror, pid);
            this.pipe = pipe;
        }

        public ControlPipePair getPipe() {
            return pipe;
        }

    }

    private static class PipeExecutorInfo extends PipeElementInfo {

        private final String executorId;
        private final PipePair pipe;


        public PipeExecutorInfo(PipedMirror mirror, String executorId, Integer pid, PipePair pipe) {
            super(mirror, pid);
            this.executorId = executorId;
            this.pipe = pipe;
        }

        public String getExecutorId() {
            return executorId;
        }

        public PipePair getPipe() {
            return pipe;
        }

    }
}
