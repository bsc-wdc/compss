/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.commands.CommandCheckWorker;
import es.bsc.compss.nio.master.handlers.Ender;
import es.bsc.compss.nio.master.handlers.ProcessOut;
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

    // Static Environment variables
    private static final String LIB_SEPARATOR = ":";
    private static final String CLASSPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_CP) != null
            && !System.getProperty(COMPSsConstants.WORKER_CP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_CP) : "";

    private static final String PYTHONPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_PP) != null
            && !System.getProperty(COMPSsConstants.WORKER_PP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_PP) : "";

    private static final String LIBPATH_FROM_ENVIRONMENT = (System.getenv(COMPSsConstants.LD_LIBRARY_PATH) != null
            && !System.getenv(COMPSsConstants.LD_LIBRARY_PATH).isEmpty()) ? System.getenv(COMPSsConstants.LD_LIBRARY_PATH) : "";

    private static final boolean IS_CPU_AFFINITY_DEFINED = System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY).isEmpty();
    private static final String CPU_AFFINITY = IS_CPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY)
            : NIOAdaptor.BINDER_DISABLED;

    private static final boolean IS_GPU_AFFINITY_DEFINED = System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY).isEmpty();
    private static final String GPU_AFFINITY = IS_GPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY)
            : NIOAdaptor.BINDER_DISABLED;

    private static final boolean IS_FPGA_AFFINITY_DEFINED = System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY).isEmpty();
    private static final String FPGA_AFFINITY = IS_FPGA_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY)
            : NIOAdaptor.BINDER_DISABLED;

    // Deployment ID
    private static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    // Master name
    private static final String MASTER_NAME_PROPERTY = System.getProperty(COMPSsConstants.MASTER_NAME);
    
    // Scripts configuration
    private static final String STARTER_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator
            + "adaptors" + File.separator + "nio" + File.separator;
    private static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";
    
    private static final String CLEAN_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator
            + "adaptors" + File.separator + "nio" + File.separator;
    private static final String CLEAN_SCRIPT_NAME = "persistent_worker_clean.sh";
    // Connection related parameters
    private static final long START_WORKER_INITIAL_WAIT = 100;
    private static final long WAIT_TIME_UNIT = 500;
    private static final long MAX_WAIT_FOR_SSH = 160_000;
    private static final long MAX_WAIT_FOR_INIT = 20_000;
    private static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static Map<String, WorkerStarter> addressToWorkerStarter = new TreeMap<>();
    private boolean workerIsReady = false;
    private boolean toStop = false;
    private final NIOWorkerNode nw;


    /**
     * Instantiates a new WorkerStarter for a given Worker
     *
     * @param nw
     */
    public WorkerStarter(NIOWorkerNode nw) {
        this.nw = nw;
    }

    /**
     * Returns the WorkerStarter registered to a given address
     *
     * @param address
     * @return
     */
    public static WorkerStarter getWorkerStarter(String address) {
        return addressToWorkerStarter.get(address);
    }

    /**
     * Marks the worker as ready
     *
     */
    public void setWorkerIsReady() {
        LOGGER.debug("[WorkerStarter] Worker " + nw.getName() + " set to ready.");
        this.workerIsReady = true;
    }

    /**
     * Marks the worker to be stopped
     *
     */
    public void setToStop() {
        this.toStop = true;
    }

    /**
     * Starts the current worker
     *
     * @return
     * @throws InitNodeException
     */
    public NIONode startWorker() throws InitNodeException {
        String name = this.nw.getName();
        String user = this.nw.getUser();
        int minPort = this.nw.getConfiguration().getMinPort();
        int maxPort = this.nw.getConfiguration().getMaxPort();
        int port = minPort;
        String masterName = "null";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals("")) && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            masterName = MASTER_NAME_PROPERTY;
        }
        // Solves exit error 143
        synchronized (addressToWorkerStarter) {
            addressToWorkerStarter.put(name, this);
            LOGGER.debug("[WorkerStarter] Worker starter for " + name + " registers in the hashmap");
        }

        NIONode n = null;
        int pid = -1;
        while (port <= maxPort && !this.toStop) {
            // Kill previous worker processes if any
            killPreviousWorker(user, name, pid);

            // Instantiate the node
            n = new NIONode(name, port);

            // Start the worker
            pid = startWorker(user, name, port, masterName);

            // Check worker status
            LOGGER.info("[WorkerStarter] Worker process started. Checking connectivity...");
            checkWorker(n, name);

            // Check received ack
            LOGGER.debug("[WorkerStarter] Retries for " + name + " have finished.");
            if (!this.workerIsReady) {
                // Try next port
                ++port;
            } else {
                // Success, return node
                try {
                    Runtime.getRuntime().addShutdownHook(new Ender(this, this.nw, pid));
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
            String msg = "[TIMEOUT]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        } else {
            String msg = "[UNKNOWN]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        }
    }

    private void killPreviousWorker(String user, String name, int pid) throws InitNodeException {
        if (pid != -1) {
            // Command was started but it is not possible to contact to the worker
            String[] command = getStopCommand(pid);
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER.error("[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource " + name
                        + ". Retries not available in this option.");
                throw new InitNodeException("[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource "
                        + name + ". Retries not available in this option.");
            } else if (po.getExitValue() != 0) {
                // Normal starting process
                LOGGER.error(ERROR_SHUTTING_DOWN_RETRY);
            }
        }
    }

    private int startWorker(String user, String name, int port, String masterName) throws InitNodeException {
        // Initial wait
        try {
            Thread.sleep(START_WORKER_INITIAL_WAIT);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        long timer = START_WORKER_INITIAL_WAIT;

        // Try to launch the worker until we receive the PID or we timeout
        int pid = -1;
        String[] command = getStartCommand(port, masterName);
        do {
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER.debug("Worker process started in resource " + name + " by queue system.");
                pid = 0;
            } else if (po.getExitValue() == 0) {
                // Success
                String output = po.getOutput();
                String[] lines = output.split("\n");
                pid = Integer.parseInt(lines[lines.length - 1]);
            } else {
                if (timer > MAX_WAIT_FOR_SSH) {
                    // Timeout
                    throw new InitNodeException("[START_CMD_ERROR]: Could not start the NIO worker in resource " + name + " through user "
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
        } while (pid < 0);

        return pid;
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
    private String[] getStartCommand(int workerPort, String masterName) throws InitNodeException {
        String workingDir = this.nw.getWorkingDir();
        String installDir = this.nw.getInstallDir();
        String appDir = this.nw.getAppDir();

        // Merge command classpath and worker defined classpath
        String workerClasspath = "";
        String classpathFromFile = this.nw.getClasspath();
        if (!classpathFromFile.isEmpty()) {
            if (!CLASSPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerClasspath = classpathFromFile + LIB_SEPARATOR + CLASSPATH_FROM_ENVIRONMENT;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            workerClasspath = CLASSPATH_FROM_ENVIRONMENT;
        }

        // Merge command pythonpath and worker defined pythonpath
        String workerPythonpath = "";
        String pythonpathFromFile = this.nw.getPythonpath();
        if (!pythonpathFromFile.isEmpty()) {
            if (!PYTHONPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + PYTHONPATH_FROM_ENVIRONMENT;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = PYTHONPATH_FROM_ENVIRONMENT;
        }

        // Merge command libpath and machine defined libpath
        String workerLibPath = "";
        String libPathFromFile = this.nw.getLibPath();
        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        // Get JVM Flags
        String workerJVMflags = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
        String[] jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            jvmFlags = workerJVMflags.split(",");
        }

        // Get FPGA reprogram args
        String workerFPGAargs = System.getProperty(COMPSsConstants.WORKER_FPGA_REPROGRAM);
        String[] FPGAargs = new String[0];
        if (workerFPGAargs != null && !workerFPGAargs.isEmpty()) {
            FPGAargs = workerFPGAargs.split(" ");
        }

        // Configure worker debug level
        String workerDebug = Boolean.toString(LogManager.getLogger(Loggers.WORKER).isDebugEnabled());

        // Configure storage
        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.EXECUTION_INTERNAL;
            LOGGER.warn("No executionType passed");
        }

        // configure persistent_worker_c execution
        String worker_persistent_c = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (worker_persistent_c == null || worker_persistent_c.isEmpty() || worker_persistent_c.equals("null")) {
            worker_persistent_c = COMPSsConstants.DEFAULT_PERSISTENT_C;
            LOGGER.warn("No persistent c passed");
        }

        // Configure python interpreter
        String python_interpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (python_interpreter == null || python_interpreter.isEmpty() || python_interpreter.equals("null")) {
            python_interpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
            LOGGER.warn("No python interpreter passed");
        }

        // Configure python version
        String python_version = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (python_version == null || python_version.isEmpty() || python_version.equals("null")) {
            python_version = COMPSsConstants.DEFAULT_PYTHON_VERSION;
            LOGGER.warn("No python version passed");
        }

        // Configure python virtual environment
        String python_virtual_environment = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (python_virtual_environment == null || python_virtual_environment.isEmpty() || python_virtual_environment.equals("null")) {
            python_virtual_environment = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
            LOGGER.warn("No python virtual environment passed");
        }
        String python_propagate_virtual_environment = System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (python_propagate_virtual_environment == null || python_propagate_virtual_environment.isEmpty() || python_propagate_virtual_environment.equals("null")) {
            python_propagate_virtual_environment = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
            LOGGER.warn("No python propagate virtual environment passed");
        }


        /*
         * ************************************************************************************************************
         * BUILD COMMAND
         * ************************************************************************************************************
         */
        String[] cmd = new String[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + NIOAdaptor.NUM_PARAMS_NIO_WORKER + jvmFlags.length + 1
                + FPGAargs.length];

        /* SCRIPT ************************************************ */
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + STARTER_SCRIPT_PATH + STARTER_SCRIPT_NAME;

        /* Values ONLY for persistent_worker.sh ****************** */
        cmd[1] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[2] = appDir.isEmpty() ? "null" : appDir;
        cmd[3] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[4] = String.valueOf(jvmFlags.length);
        for (int i = 0; i < jvmFlags.length; ++i) {
            cmd[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + i] = jvmFlags[i];
        }

        int FPGAposition = NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + jvmFlags.length;
        cmd[FPGAposition] = String.valueOf(FPGAargs.length);
        for (int i = 0; i < FPGAargs.length; ++i) {
            cmd[FPGAposition + i + 1] = FPGAargs[i];
        }

        /* Values for NIOWorker ********************************** */
        int nextPosition = NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + jvmFlags.length + 1 + FPGAargs.length;
        cmd[nextPosition++] = workerDebug;

        // Internal parameters
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = this.nw.getName();
        cmd[nextPosition++] = String.valueOf(workerPort);
        cmd[nextPosition++] = masterName;
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);

        // Worker parameters
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalComputingUnits());
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalGPUs());
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalFPGAs());
        cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(GPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(FPGA_AFFINITY);
        cmd[nextPosition++] = String.valueOf(this.nw.getLimitOfTasks());

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = System.getProperty(COMPSsConstants.LANG);
        cmd[nextPosition++] = workingDir;
        cmd[nextPosition++] = this.nw.getInstallDir();
        cmd[nextPosition++] = appDir.isEmpty() ? "null" : appDir;
        cmd[nextPosition++] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[nextPosition++] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[nextPosition++] = workerPythonpath.isEmpty() ? "null" : workerPythonpath;

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.getLevel());
        cmd[nextPosition++] = NIOTracer.getExtraeFile();
        if (Tracer.isActivated()) {
            // NumSlots per host is ignored --> 0
            Integer hostId = NIOTracer.registerHost(this.nw.getName(), 0);
            cmd[nextPosition++] = String.valueOf(hostId.toString());
        } else {
            cmd[nextPosition++] = "NoTracinghostID";
        }

        // Storage parameters
        cmd[nextPosition++] = storageConf;
        cmd[nextPosition++] = executionType;

        // persistent_c parameter
        cmd[nextPosition++] = worker_persistent_c;

        // Python interpreter parameter
        cmd[nextPosition++] = python_interpreter;
        // Python interpreter version
        cmd[nextPosition++] = python_version;
        // Python virtual environment parameter
        cmd[nextPosition++] = python_virtual_environment;
        // Python propagate virtual environment parameter
        cmd[nextPosition++] = python_propagate_virtual_environment;

        if (cmd.length != nextPosition) {
            throw new InitNodeException("ERROR: Incorrect number of parameters. Expected: " + cmd.length +
            ". Got: " + nextPosition);
        }

        return cmd;
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
        //ps --ppid 2796 -o pid= | awk '{ print $1 }' | xargs kill -15 <--- kills all childs of ppid
        //kill -15 2796 kills the parentpid

        //necessary to check whether it has file separator or not? /COMPSs////Runtime == /COMPSs/Runtime in bash 
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + CLEAN_SCRIPT_PATH + CLEAN_SCRIPT_NAME;
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
            pb.environment().remove(Tracer.LD_PRELOAD);
            pb.command(cmd);
            Process process = pb.start();

            InputStream stderr = process.getErrorStream();
            InputStream stdout = process.getInputStream();

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
     * Ender function called from the JVM Ender Hook
     *
     * @param node
     * @param pid
     */
    public void ender(NIOWorkerNode node, int pid) {
        if (pid > 0) {
            String user = node.getUser();
            
            // Clean worker working directory
            String jvmWorkerOpts = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
            String removeWDFlagDisabled = COMPSsConstants.WORKER_REMOVE_WD + "=false";
            if (jvmWorkerOpts != null && jvmWorkerOpts.contains(removeWDFlagDisabled)) {
                // User requested not to clean workers WD
                LOGGER.warn("RemoveWD set to false. Not Cleaning " + node.getName() + " working directory");
            } else {
                // Regular clean up
                String sandboxWorkingDir = node.getWorkingDir();
                String[] command = getCleanWorkerWorkingDir(sandboxWorkingDir);
                if (command != null) {
                    executeCommand(user, node.getName(), command);
                }
            }

            // Execute stop command
            String[] command = getStopCommand(pid);
            LOGGER.info("getStopCommand generated this: " + command);
            if (command != null) {
                executeCommand(user, node.getName(), command);
            }

        }
    }

}
