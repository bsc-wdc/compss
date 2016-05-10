package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.commands.CommandCheckWorker;
import integratedtoolkit.nio.master.handlers.Ender;
import integratedtoolkit.nio.master.handlers.ProcessOut;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeMap;

import org.apache.log4j.Logger;


public class WorkerStarter {
    
    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0
            ? true : false;
    protected static final int tracing_level = Integer.parseInt(System.getProperty(ITConstants.IT_TRACING));
    
    private static final String DEPLOYMENT_ID = System.getProperty(ITConstants.IT_DEPLOYMENT_ID);
       
    private static final String STARTER_SCRIPT_PATH = "scripts" + File.separator + "system" 
    								+ File.separator + "adaptors" + File.separator + "nio" + File.separator;
    private static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";
    
    private static final int NUM_THREADS = 16;
    private static final long MAX_WAIT_FOR_INIT = 20000;
    private static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";
    
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    protected static final boolean debug = logger.isDebugEnabled();
    
    private static TreeMap<String, WorkerStarter> addresstoWorkerStarter = new TreeMap<String, WorkerStarter>();    
    private boolean workerIsReady = false;
    private NIOWorkerNode nw;
    
    
    public WorkerStarter(NIOWorkerNode nw) {
        this.nw = nw;
    }
    
    public static WorkerStarter getWorkerStarter(String address){
        return addresstoWorkerStarter.get(address);
    }

    public void setWorkerIsReady(){
        workerIsReady = true;
    }
    
    public NIONode startWorker() throws Exception {
        String name = nw.getName();
        String user = nw.getUser();
        int minPort = nw.getConfiguration().getMinPort();
        int maxPort = nw.getConfiguration().getMaxPort();
        int port = minPort;
        
        
        NIONode n = null;
        int pid = -1;
        while (port <= maxPort){
            String[] command;
            if (pid != -1) {
                command = getStopCommand(pid);
                ProcessOut po = executeCommand(user, name, command);
                if (po.getExitValue() != 0) {
                	logger.error(ERROR_SHUTTING_DOWN_RETRY);
                }
            }
            
            command = getStartCommand(nw, port);
            ProcessOut po = executeCommand(user, name, command);
            if (po.getExitValue() == 0) {
                String output = po.getOutput();
                String[] lines = output.split("\n");
                pid = Integer.parseInt(lines[lines.length - 1]);

                long delay = 30;
                long totalWait = 0;

                n = new NIONode(name, port);
                String nodeName = nw.getName();

                addresstoWorkerStarter.put( nodeName, this);
                
                logger.debug("Worker process started. Checking connectivity...");
                
                CommandCheckWorker cmd = new CommandCheckWorker(DEPLOYMENT_ID, nodeName);
                while ((!workerIsReady) && (totalWait < MAX_WAIT_FOR_INIT)) { 
                	Thread.sleep(delay);
                	if (debug){
                		logger.debug("Sending check command to worker "+ nodeName);
                	}
                    Connection c = NIOAdaptor.tm.startConnection(n);
                    c.sendCommand(cmd);
                    c.receive();
                    c.finishConnection();

                    totalWait += delay;
                    delay = (delay < 1900 )? delay*2 : 2000;
                }
            } else {
                throw new Exception("[START_CMD_ERROR]: Could not start the NIO worker in resource " + name + " through user " + user + ".\n"
                        + "OUTPUT:" + po.getOutput() + "\n"
                        + "ERROR:" + po.getError() + "\n");
            }
            if (!workerIsReady) {
                ++port;
            } else {
                Runtime.getRuntime().addShutdownHook(new Ender(nw, pid));
                return n;
            } 
        }
        
        if (!workerIsReady) {
            throw new Exception("[TIMEOUT]: Could not start the NIO worker on resource " + name + " through user " + user + ".");
        } else {
            return n; // should be unreachable statment 
        }
    }

    // Ender function called from the JVM Ender Hook
    public static void ender(NIOWorkerNode node, int pid) {
        String user = node.getUser();
        String wD = node.getWorkingDir();
        
        // Execute stop command
        String[] command = getStopCommand(pid);
        executeCommand(user, node.getName(), command);
        
        // Execute clean command
        command = getCleanCommand(wD);
        executeCommand(user, node.getName(), command);
    }

    // Arguments needed for persistent_worker.sh
    private static String[] getStartCommand(NIOWorkerNode node, int workerPort) {

        String libPath = node.getLibPath();
        String appDir = node.getAppDir();
        String workerJVMflags = (System.getProperty(ITConstants.IT_WORKER_JVM_OPTS) != null) ? System.getProperty(ITConstants.IT_WORKER_JVM_OPTS) : "";
        String[] jvmFlags = workerJVMflags.split(",");
        String workingDir = node.getWorkingDir();
        String cp = (System.getProperty(ITConstants.IT_WORKER_CP) != null && System.getProperty(ITConstants.IT_WORKER_CP).compareTo("") != 0) ? System.getProperty(ITConstants.IT_WORKER_CP) : "\"\"";
        String installDir = node.getInstallDir();
        String workerDebug = Boolean.toString(Logger.getLogger(Loggers.WORKER).isDebugEnabled());
        String storageConf = System.getProperty(ITConstants.IT_STORAGE_CONF);
        if (( storageConf == null ) || ( storageConf.compareTo("") == 0 ) || ( storageConf.compareTo("null") == 0 )) {
        	storageConf = "null";
        	logger.warn("No storage configuration file passed");
        }
        String executionType = System.getProperty(ITConstants.IT_TASK_EXECUTION);

        /* ********************************************************
         * BUILD COMMAND
         * ********************************************************/
        String[] cmd = new String[19 + jvmFlags.length];
        
        /* SCRIPT *************************************************/
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + STARTER_SCRIPT_PATH + STARTER_SCRIPT_NAME;
        
        /* Values ONLY for persistent_worker.sh *******************/
        cmd[1] = libPath.isEmpty() ? "null" : libPath;
        cmd[2] = appDir.isEmpty() ? "null" : appDir;
        cmd[3] = cp.isEmpty() ? "null" : cp;
        cmd[4] = String.valueOf(jvmFlags.length);
        for (int i = 0; i < jvmFlags.length; ++i) {
        	cmd[5 + i] = jvmFlags[i];
        }        
        
        /* Values for NIOWorker ***********************************/
        int nextPosition = 5 + jvmFlags.length;
        cmd[nextPosition++] = workerDebug;
        
        // Internal parameters
        cmd[nextPosition++] = (node.getLimitOfTasks() >= 0) ? String.valueOf(node.getLimitOfTasks()) : String.valueOf(NUM_THREADS);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = node.getName();
        cmd[nextPosition++] = String.valueOf(workerPort);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);
        
        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = workingDir;
        cmd[nextPosition++] = node.getInstallDir();
        
        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(tracing_level);
        if (tracing) {
            Integer hostId = NIOTracer.registerHost(node.getName(), NUM_THREADS);
            cmd[nextPosition++] = String.valueOf(hostId.toString());
        } else {
        	cmd[nextPosition++] ="NoTracinghostID";
        }
        
        // Storage parameters
        cmd[nextPosition++] = storageConf;
        cmd[nextPosition++] = executionType;
        
        return cmd;
    }

    private static String[] getStopCommand(int pid) {
        String[] cmd = new String[3];
        cmd[0] = "kill";
        cmd[1] = "-9";
        cmd[2] = String.valueOf(pid);
        return cmd;
    }

    private static String[] getCleanCommand(String wDir) {
    	// The working dir is an application sandbox within the base working directory
    	// We can erase it all
        wDir = wDir.substring(0, wDir.indexOf(DEPLOYMENT_ID) + DEPLOYMENT_ID.length());
        wDir = wDir.endsWith(File.separator) ? wDir : wDir + File.separator;
        String[] cmd = new String[3];
        cmd[0] = "rm";
        cmd[1] = "-rf";
        cmd[2] = wDir;

        return cmd;
    }

    protected static ProcessOut executeCommand(String user, String resource, String[] command) {        
        ProcessOut processOut = new ProcessOut();

        String[] cmd = new String[5 + command.length];
        cmd[0] = "ssh";
        cmd[1] = "-o StrictHostKeyChecking=no";
        cmd[2] = "-o BatchMode=yes";
        cmd[3] = "-o ChallengeResponseAuthentication=no";
        cmd[4] = ((user == null || user.isEmpty()) ? "" : user + "@") + resource;
        System.arraycopy(command, 0, cmd, 5, command.length);

        StringBuilder sb = new StringBuilder("");
        for (String param : cmd) {
            sb.append(param).append(" ");
        }
        logger.debug("COMM CMD: " + sb.toString());
        try {
            
            ProcessBuilder pb = new ProcessBuilder();
            pb.environment().remove("LD_PRELOAD");
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
                logger.debug("COMM CMD OUT: " + line);
            }
            reader = new BufferedReader(new InputStreamReader(stderr));
            while ((line = reader.readLine()) != null) {
                processOut.appendError(line);
                logger.debug("COMM CMD ERR: " + line);
            }
        } catch (Exception e) {
        	logger.error("Exception initializing worker ", e);
        }
        return processOut;
    }

}
