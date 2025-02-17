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
package es.bsc.compss.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.commands.CommandCheckWorker;
import es.bsc.compss.nio.master.handlers.Ender;
import es.bsc.compss.nio.master.handlers.ProcessOut;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.util.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WorkerStarter {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Deployment ID
    private static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    // Scripts configuration

    private static final String CLEAN_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "nio" + File.separator;
    private static final String CLEAN_SCRIPT_NAME = "persistent_worker_clean.sh";
    // Connection related parameters
    private static final long START_WORKER_INITIAL_WAIT = 100;
    private static final long WAIT_TIME_UNIT = 500;
    private static final long MAX_WAIT_FOR_SSH =
        Long.parseLong(System.getenv().getOrDefault(COMPSsConstants.COMPSS_SSH_TIMEOUT, "160000"));
    private static final long MAX_WAIT_FOR_INIT =
        Long.parseLong(System.getenv().getOrDefault(COMPSsConstants.COMPSS_WORKER_INIT_TIMEOUT, "90000"));
    private static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";

    // Starting workers
    private static final Map<String, WorkerStarter> ADDRESS_TO_WORKER_STARTER = new TreeMap<>();

    // Instance attributes
    private boolean workerIsReady = false;
    private boolean toStop = false;
    private final NIOWorkerNode nw;


    /**
     * Instantiates a new WorkerStarter for a given Worker.
     *
     * @param nw Worker node.
     */
    public WorkerStarter(NIOWorkerNode nw) {
        this.nw = nw;
    }

    /**
     * Returns the WorkerStarter registered to a given address.
     *
     * @param address Worker address.
     * @return WorkerStrarter object for the current address.
     */
    public static WorkerStarter getWorkerStarter(String address) {
        return ADDRESS_TO_WORKER_STARTER.get(address);
    }

    /**
     * Marks the worker as ready.
     */
    public void setWorkerIsReady() {
        LOGGER.debug("[WorkerStarter] Worker " + nw.getName() + " set to ready.");
        this.workerIsReady = true;
    }

    /**
     * Marks the worker to be stopped.
     */
    public void setToStop() {
        this.toStop = true;
    }

    /**
     * Starts the current worker.
     *
     * @return The corresponding worker node.
     * @throws InitNodeException If any internal error occurs.
     */
    public NIONode startWorker() throws InitNodeException {
        String name = this.nw.getName();
        String user = this.nw.getUser();
        int minPort = this.nw.getConfiguration().getMinPort();
        int maxPort = this.nw.getConfiguration().getMaxPort();
        int workerport = minPort;
        String masterName = COMPSsNode.getMasterName();

        // Solves exit error 143
        synchronized (ADDRESS_TO_WORKER_STARTER) {
            ADDRESS_TO_WORKER_STARTER.put(name, this);
            LOGGER.debug("[WorkerStarter] Worker starter for " + name + " registers in the hashmap");
        }

        NIONode n = null;
        int pid = -1;
        while (workerport <= maxPort && !this.toStop) {
            // Kill previous worker processes if any
            killPreviousWorker(user, name, pid);

            // Instantiate the node
            n = new NIONode(name, workerport);

            // Start the worker
            pid = startWorker(user, name, workerport, masterName);

            // Check worker status
            LOGGER.info("[WorkerStarter] Worker process started. Checking connectivity...");
            checkWorker(n, name);

            // Check received ack
            LOGGER.debug("[WorkerStarter] Retries for " + name + " have finished.");
            if (!this.workerIsReady) {
                // Try next port
                ++workerport;
            } else {
                // Success, return node
                try {
                    Runtime.getRuntime().addShutdownHook(new Ender(this, pid));
                } catch (IllegalStateException e) {
                    LOGGER.warn("Tried to shutdown vm while it was already being shutdown", e);
                }
                return n;
            }
        }

        // The loop has finished because there is no available node.
        // This can be because node is stopping or because we reached the maximum available ports
        if (this.toStop) {
            String msg = "[STOP]: Worker " + name + " stopped during creation because application is stopped";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        } else if (!this.workerIsReady) {
            String msg =
                "[TIMEOUT]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        } else {
            String msg =
                "[UNKNOWN]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        }
    }

    private int startWorker(String user, String name, int workerPort, String masterName) throws InitNodeException {
        // Initial wait
        try {
            Thread.sleep(START_WORKER_INITIAL_WAIT);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        long timer = START_WORKER_INITIAL_WAIT;

        // Try to launch the worker until we receive the PID or we timeout
        int pid = -1;
        String tracingHostId = "NoTracinghostID";
        if (Tracer.isActivated()) {
            // NumSlots per host is ignored --> 0
            tracingHostId = String.valueOf(NIOTracer.registerHost(this.nw.getName(), 0));

        }
        String[] command = generateStartCommand(workerPort, masterName, tracingHostId);
        do {
            boolean error = false;
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER.debug("Worker process started in resource " + name + " by queue system.");
                pid = 0;
            } else if (po.getExitValue() == 0) {
                // Success
                String output = po.getOutput();
                try {
                    if (!output.isEmpty()) {
                        String[] lines = output.split("\n");
                        pid = Integer.parseInt(lines[lines.length - 1]);
                    } else {
                        throw new Exception("Output is empty.");
                    }
                } catch (Exception e) {
                    LOGGER.warn("Incorrect Worker starter response: " + e.getMessage());
                    pid = -1;
                    error = true;
                }
            } else {
                error = true;
            }
            if (error) {
                if (timer > MAX_WAIT_FOR_SSH) {
                    // Timeout
                    throw new InitNodeException(
                        "[START_CMD_ERROR]: Could not start the NIO worker in resource " + name + " through user "
                            + user + ".\n" + "OUTPUT:" + po.getOutput() + "\n" + "ERROR:" + po.getError() + "\n");
                }
                LOGGER.warn(" Worker process failed to start in resource " + name + ". Retrying...");
            }

            // Sleep between retries
            try {
                Thread.sleep(4 * WAIT_TIME_UNIT);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            timer = timer + (4 * WAIT_TIME_UNIT);
        } while (pid < 0 && !this.toStop);

        return pid;
    }

    private void killPreviousWorker(String user, String name, int pid) throws InitNodeException {
        if (pid != -1) {
            // Command was started but it is not possible to contact to the worker
            String[] command = getStopCommand(pid);
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER
                    .error("[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource "
                        + name + ". Retries not available in this option.");
                throw new InitNodeException(
                    "[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource " + name
                        + ". Retries not available in this option.");
            } else {
                if (po.getExitValue() != 0) {
                    // Normal starting process
                    LOGGER.error(ERROR_SHUTTING_DOWN_RETRY);
                }
            }
        }
    }

    private void checkWorker(NIONode n, String name) {
        long delay = WAIT_TIME_UNIT;
        long totalWait = 0;
        CommandCheckWorker cmd = new CommandCheckWorker(DEPLOYMENT_ID, name);

        do {
            if (DEBUG) {
                LOGGER.debug("[WorkerStarter] Sending check command to worker " + name);
            }

            // Send command check
            Connection c = NIOAdaptor.getTransferManager().startConnection(n);
            NIOAgent.registerOngoingCommand(c, cmd);
            c.sendCommand(cmd);
            c.receive();
            c.finishConnection();

            // Sleep before next iteration
            try {
                LOGGER.debug("[WorkerStarter] Waiting to send next check worker command with delay " + delay);
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            totalWait += delay;
            delay = (delay < 3_900) ? delay * 2 : 4_000;
        } while (!this.workerIsReady && totalWait < MAX_WAIT_FOR_INIT && !this.toStop);
    }

    // Arguments needed for persistent_worker.sh
    private String[] generateStartCommand(int workerPort, String masterName, String hostId) throws InitNodeException {
        final String workingDir = this.nw.getWorkingDir();
        final String installDir = this.nw.getInstallDir();
        final String appDir = this.nw.getAppDir();
        String classpathFromFile = this.nw.getClasspath();
        String pythonpathFromFile = this.nw.getPythonpath();
        String libPathFromFile = this.nw.getLibPath();
        String envScriptPathFromFile = this.nw.getEnvScriptPaht();
        String pythonInterpreterFromFile = this.nw.getPythonInterpreter();
        String workerName = this.nw.getName();
        int totalCPU = this.nw.getTotalComputingUnits();
        int totalGPU = this.nw.getTotalGPUs();
        int totalFPGA = this.nw.getTotalFPGAs();

        int limitOfTasks = this.nw.getLimitOfTasks();
        try {
            return new NIOStarterCommand(workerName, workerPort, masterName, workingDir, installDir, appDir,
                classpathFromFile, pythonpathFromFile, libPathFromFile, envScriptPathFromFile,
                pythonInterpreterFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks, hostId).getStartCommand();
        } catch (Exception e) {
            throw new InitNodeException(e);
        }
    }

    private String[] getCleanWorkerWorkingDir(String workingDir) {
        String[] cmd = new String[3];
        // Send SIGTERM to allow ShutdownHooks on Worker
        cmd[0] = "rm";
        cmd[1] = "-rf";
        cmd[2] = workingDir;
        return cmd;
    }

    private String[] getStopCommand(int pid) {
        String[] cmd = new String[2];
        String installDir = this.nw.getInstallDir();

        // Send SIGTERM to allow ShutdownHooks on Worker...
        // Send SIGKILL to all child processes of 'pid'
        // and send a SIGTERM to the parent process
        // ps --ppid 2796 -o pid= | awk '{ print $1 }' | xargs kill -15 <--- kills all childs of ppid
        // kill -15 2796 kills the parentpid
        // necessary to check whether it has file separator or not? /COMPSs////Runtime == /COMPSs/Runtime in bash
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + CLEAN_SCRIPT_PATH
            + CLEAN_SCRIPT_NAME;
        cmd[1] = String.valueOf(pid);

        return cmd;
    }

    private ProcessOut executeCommand(String user, String resource, String[] command) {
        ProcessOut processOut = new ProcessOut();
        String[] cmd = this.nw.getConfiguration().getRemoteExecutionCommand(user, resource, command);
        if (cmd == null) {
            LOGGER.warn("Worker configured to be sarted by queue system.");
            return null;
        }
        // Log command
        StringBuilder sb = new StringBuilder("");
        for (String param : cmd) {
            sb.append(param).append(" ");
        }
        LOGGER.debug("COMM CMD: " + sb.toString());

        // Execute command
        try {
            ProcessBuilder pb = new ProcessBuilder();

            // Setup process environment -- Tracing entries
            Tracer.prepareEnvironment(pb.environment(), false);

            pb.command(cmd);
            Process process = pb.start();

            final InputStream stderr = process.getErrorStream();
            final InputStream stdout = process.getInputStream();

            process.getOutputStream().close();

            process.waitFor();
            processOut.setExitValue(process.exitValue());

            BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
            String line;
            while ((line = reader.readLine()) != null) {
                processOut.appendOutput(line);
                LOGGER.debug("COMM CMD OUT: " + line);
            }
            reader = new BufferedReader(new InputStreamReader(stderr));
            while ((line = reader.readLine()) != null) {
                processOut.appendError(line);
                LOGGER.debug("COMM CMD ERR: " + line);
            }
        } catch (Exception e) {
            LOGGER.error("Exception initializing worker ", e);
        }
        return processOut;
    }

    /**
     * Ender function called from the JVM Ender Hook.
     *
     * @param pid Process PID.
     */
    public void ender(int pid) {
        if (pid > 0) {

            // Clean worker working directory
            String jvmWorkerOpts = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
            String removeWDFlagDisabled = COMPSsConstants.WORKER_REMOVE_WD + "=false";
            if (jvmWorkerOpts != null && jvmWorkerOpts.contains(removeWDFlagDisabled)) {
                // User requested not to clean workers WD
                LOGGER.warn("RemoveWD set to false. Not Cleaning " + this.nw.getName() + " working directory");
            } else {
                // Regular clean up
                String sandboxWorkingDir = this.nw.getWorkingDir();
                String[] command = getCleanWorkerWorkingDir(sandboxWorkingDir);
                LOGGER.info("getCleanWorkerWorkingDir generated this: " + command);
                if (command != null) {
                    executeCommand(this.nw.getUser(), this.nw.getName(), command);
                }
            }

            // Execute stop command
            String[] command = getStopCommand(pid);
            LOGGER.info("getStopCommand generated this: " + command);
            if (command != null) {
                executeCommand(this.nw.getUser(), this.nw.getName(), command);
            }

        }
    }

}
