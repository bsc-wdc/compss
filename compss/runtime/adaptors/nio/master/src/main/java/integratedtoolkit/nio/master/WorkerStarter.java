package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.ITConstants;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.commands.CommandCheckWorker;
import integratedtoolkit.nio.master.handlers.Ender;
import integratedtoolkit.nio.master.handlers.ProcessOut;
import integratedtoolkit.util.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WorkerStarter {

    // Static Environment variables
    private static final String LIB_SEPARATOR = ":";
    private static final String classpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_CP) != null
            && !System.getProperty(ITConstants.IT_WORKER_CP).equals("")) ? System.getProperty(ITConstants.IT_WORKER_CP) : "";

    private static final String pythonpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_PP) != null
            && !System.getProperty(ITConstants.IT_WORKER_PP).equals("")) ? System.getProperty(ITConstants.IT_WORKER_PP) : "";

    private static final String libPathFromEnvironment = (System.getenv(ITConstants.LD_LIBRARY_PATH) != null
            && !System.getenv(ITConstants.LD_LIBRARY_PATH).equals("")) ? System.getenv(ITConstants.LD_LIBRARY_PATH) : "";

    // Deployment ID
    private static final String DEPLOYMENT_ID = System.getProperty(ITConstants.IT_DEPLOYMENT_ID);

    // Scripts configuration
    private static final String STARTER_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator
            + "adaptors" + File.separator + "nio" + File.separator;
    private static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";

    // Connection related parameters
    private static final long MAX_WAIT_FOR_SSH = 160_000;
    private static final long MAX_WAIT_FOR_INIT = 20_000;
    private static final long WAIT_TIME_UNIT = 500;
    private static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static TreeMap<String, WorkerStarter> addresstoWorkerStarter = new TreeMap<>();
    private boolean workerIsReady = false;
    private boolean toStop = false;
    private NIOWorkerNode nw;


    public WorkerStarter(NIOWorkerNode nw) {
        this.nw = nw;
    }

    public static WorkerStarter getWorkerStarter(String address) {
        return addresstoWorkerStarter.get(address);
    }

    public void setWorkerIsReady() {
        LOGGER.debug("[WorkerStarter] Worker " + nw.getName() + " set to ready.");
        workerIsReady = true;
    }

    public void setToStop() {
        toStop = true;
    }

    public NIONode startWorker() throws InitNodeException {
        String name = nw.getName();
        String user = nw.getUser();
        int minPort = nw.getConfiguration().getMinPort();
        int maxPort = nw.getConfiguration().getMaxPort();
        int port = minPort;
        // Solves exit error 143
        synchronized (addresstoWorkerStarter) {
            addresstoWorkerStarter.put(name, this);
            LOGGER.debug("[WorkerStarter] Worker starter for " + name + " registers in the hashmap");
        }
        NIONode n = null;
        int pid = -1;
        while (port <= maxPort && !toStop) {
            String[] command;
            if (pid != -1) {
                // Command was started but it is not possible to contact to the worker
                command = getStopCommand(pid);
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
                pid = -1; // Setting pid to -1 to enter to start command after killing the old process

            }

            n = new NIONode(name, port);

            command = getStartCommand(nw, port);
            long timer = 0;
            while (pid < 0) {

                timer = timer + (WAIT_TIME_UNIT * 4);
                try {
                    Thread.sleep(WAIT_TIME_UNIT * 4);
                } catch (Exception e) {
                    // Nothing to do
                }
                ProcessOut po = executeCommand(user, name, command);
                if (po == null) {
                    // Queue System managed worker starter
                    LOGGER.debug("Worker process started in resource " + name + " by queue system.");
                    pid = 0;
                } else if (po.getExitValue() == 0) {
                    String output = po.getOutput();
                    String[] lines = output.split("\n");
                    pid = Integer.parseInt(lines[lines.length - 1]);
                } else {
                    if (timer > MAX_WAIT_FOR_SSH) {
                        throw new InitNodeException("[START_CMD_ERROR]: Could not start the NIO worker in resource " + name
                                + " through user " + user + ".\n" + "OUTPUT:" + po.getOutput() + "\n" + "ERROR:" + po.getError() + "\n");
                    }
                    LOGGER.debug(" Worker process failed to start in resource " + name + ". Retrying...");

                }
            }
            long delay = WAIT_TIME_UNIT;
            long totalWait = 0;

            LOGGER.debug("[WorkerStarter] Worker process started. Checking connectivity...");

            CommandCheckWorker cmd = new CommandCheckWorker(DEPLOYMENT_ID, name);
            while ((!workerIsReady) && (totalWait < MAX_WAIT_FOR_INIT) && !toStop) {
                try {
                    LOGGER.debug("[WorkerStarter] Waiting to send next check worker command with delay " + delay);
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                if (!workerIsReady) {
                    if (DEBUG) {
                        LOGGER.debug("[WorkerStarter] Sending check command to worker " + name);
                    }
                    Connection c = NIOAdaptor.tm.startConnection(n);
                    c.sendCommand(cmd);
                    c.receive();
                    c.finishConnection();

                    totalWait += delay;
                    delay = (delay < 3900) ? delay * 2 : 4000;
                }
            }
            LOGGER.debug("[WorkerStarter] Retries for " + name + " have finished.");
            if (!workerIsReady) {
                ++port;
            } else {
                try {
                    Runtime.getRuntime().addShutdownHook(new Ender(this, nw, pid));
                } catch (IllegalStateException e) {
                    LOGGER.warn("Tried to shutdown vm while it was already being shutdown", e);
                }
                return n;
            }
        }

        if (toStop) {
            String msg = "[STOP]: Worker " + name + " stopped during creation because application is stopped";
            LOGGER.warn(msg);
            throw new InitNodeException(msg);
        } else if (!workerIsReady) {
            String msg = "[TIMEOUT]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            throw new InitNodeException(msg);
        } else {
            String msg = "[UNKNOWN]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            throw new InitNodeException(msg);
        }
    }

    // Ender function called from the JVM Ender Hook
    public void ender(NIOWorkerNode node, int pid) {
        if (pid > 0) {
            String user = node.getUser();

            // Execute stop command
            String[] command = getStopCommand(pid);
            if (command != null) {
                executeCommand(user, node.getName(), command);
            }
        }
    }

    // Arguments needed for persistent_worker.sh
    private static String[] getStartCommand(NIOWorkerNode node, int workerPort) {
        String workingDir = node.getWorkingDir();
        String installDir = node.getInstallDir();
        String appDir = node.getAppDir();

        // Merge command classpath and worker defined classpath
        String workerClasspath = "";
        String classpathFromFile = node.getClasspath();
        if (!classpathFromFile.isEmpty()) {
            if (!classpathFromEnvironment.isEmpty()) {
                workerClasspath = classpathFromFile + LIB_SEPARATOR + classpathFromEnvironment;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            workerClasspath = classpathFromEnvironment;
        }

        // Merge command pythonpath and worker defined pythonpath
        String workerPythonpath = "";
        String pythonpathFromFile = node.getPythonpath();
        if (!pythonpathFromFile.isEmpty()) {
            if (!pythonpathFromEnvironment.isEmpty()) {
                workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + pythonpathFromEnvironment;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = pythonpathFromEnvironment;
        }

        // Merge command libpath and machine defined libpath
        String workerLibPath = "";
        String libPathFromFile = node.getLibPath();
        if (!libPathFromFile.isEmpty()) {
            if (!libPathFromEnvironment.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + libPathFromEnvironment;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = libPathFromEnvironment;
        }

        // Get JVM Flags
        String workerJVMflags = System.getProperty(ITConstants.IT_WORKER_JVM_OPTS);
        String[] jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            jvmFlags = workerJVMflags.split(",");
        }

        // Configure worker debug level
        String workerDebug = Boolean.toString(LogManager.getLogger(Loggers.WORKER).isDebugEnabled());

        // Configure storage
        String storageConf = System.getProperty(ITConstants.IT_STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        String executionType = System.getProperty(ITConstants.IT_TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = ITConstants.EXECUTION_INTERNAL;
            LOGGER.warn("No executionType passed");
        }

        /*
         * ************************************************************************************************************
         * BUILD COMMAND
         * ************************************************************************************************************
         */
        String[] cmd = new String[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + NIOAdaptor.NUM_PARAMS_NIO_WORKER + jvmFlags.length];

        /* SCRIPT ************************************************ */
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + STARTER_SCRIPT_PATH + STARTER_SCRIPT_NAME;

        /* Values ONLY for persistent_worker.sh ****************** */
        cmd[1] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[2] = appDir.isEmpty() ? "null" : appDir;
        cmd[3] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[4] = String.valueOf(jvmFlags.length);
        for (int i = 0; i < jvmFlags.length; ++i) {
            cmd[5 + i] = jvmFlags[i];
        }

        /* Values for NIOWorker ********************************** */
        int nextPosition = 5 + jvmFlags.length;
        cmd[nextPosition++] = workerDebug;

        // Internal parameters
        int workerThreadSlots;
        int limitOfTasks = node.getLimitOfTasks();
        int cus = node.getTotalComputingUnits();
        if (limitOfTasks < 0) {
            workerThreadSlots = cus;
        } else {
            workerThreadSlots = Math.min(limitOfTasks, cus);
        }
        cmd[nextPosition++] = String.valueOf(workerThreadSlots);

        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = node.getName();
        cmd[nextPosition++] = String.valueOf(workerPort);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = System.getProperty(ITConstants.IT_LANG);
        cmd[nextPosition++] = workingDir;
        cmd[nextPosition++] = node.getInstallDir();
        cmd[nextPosition++] = appDir.isEmpty() ? "null" : appDir;
        cmd[nextPosition++] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[nextPosition++] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[nextPosition++] = workerPythonpath.isEmpty() ? "null" : workerPythonpath;

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.getLevel());
        cmd[nextPosition++] = NIOTracer.getExtraeFile();
        if (Tracer.isActivated()) {
            // NumSlots per host is ignored --> 0
            Integer hostId = NIOTracer.registerHost(node.getName(), 0);
            cmd[nextPosition++] = String.valueOf(hostId.toString());
        } else {
            cmd[nextPosition++] = "NoTracinghostID";
        }

        // Storage parameters
        cmd[nextPosition++] = storageConf;
        cmd[nextPosition++] = executionType;

        // GPU parameters
        cmd[nextPosition++] = String.valueOf(node.getTotalGPUs());

        // TODO: check if values have been set in runcompss or other configuration file
        // This is the insertion point of the current implementation
        // CPU parameters
        cmd[nextPosition++] = "0";
        cmd[nextPosition++] = "-";

        return cmd;
    }

    private static String[] getStopCommand(int pid) {
        String[] cmd = new String[3];
        // Send SIGTERM to allow ShutdownHooks on Worker
        cmd[0] = "kill";
        cmd[1] = "-15";
        cmd[2] = String.valueOf(pid);
        return cmd;
    }

    protected ProcessOut executeCommand(String user, String resource, String[] command) {

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

}
