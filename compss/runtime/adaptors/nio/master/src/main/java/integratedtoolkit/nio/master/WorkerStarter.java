package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.commands.CommandCheckWorker;
import integratedtoolkit.nio.master.handlers.Ender;
import integratedtoolkit.nio.master.handlers.ProcessOut;
import integratedtoolkit.util.ErrorManager;

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
    
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    protected static final boolean debug = logger.isDebugEnabled();
    private static final int NUM_THREADS = 16;
    private static final long MAX_WAIT_FOR_INIT = 20000;
    private static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";
    
    private static TreeMap<String, WorkerStarter> addresstoWorkerStarter = new TreeMap<String, WorkerStarter>();    
    private boolean workerIsReady = false;
    private NIOWorkerNode nw;
    private TreeMap<String, AdaptorDescription> adaptorsDesc;
    
    
    public WorkerStarter(NIOWorkerNode nw, TreeMap<String, AdaptorDescription> adaptorsDesc) {
        this.nw = nw;
        this.adaptorsDesc = adaptorsDesc;
    }
    
    public static WorkerStarter getWorkerStarter(String address){
        return addresstoWorkerStarter.get(address);
    }

    public void setWorkerIsReady(){
        workerIsReady = true;
    }
    
    public NIONode startWorker() throws Exception {
        String name = nw.getName();
        AdaptorDescription ad = adaptorsDesc.get(AdaptorDescription.NIOAdaptor);
        if (ad == null){
        	ErrorManager.warn("Error getting the NIOAdaptor description for worker "+ name);
        	return null;
        }
        int minPort = adaptorsDesc.get(AdaptorDescription.NIOAdaptor).getPortRange()[0];
        int maxPort = adaptorsDesc.get(AdaptorDescription.NIOAdaptor).getPortRange()[1];
        int port = minPort;
        String user = nw.getUser();
        
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

    // Arguments needed for worker.sh
    // lang workingDir libpath appDir classpath installDir debug workingDir numThreads maxSend maxReceive name workerPort masterPort 
    private static String[] getStartCommand(NIOWorkerNode node, int workerPort) {

        String libPath = node.getLibPath();
        String appDir = node.getAppDir();
        String workingDir = node.getWorkingDir();
        String cp = (System.getProperty(ITConstants.IT_WORKER_CP) != null && System.getProperty(ITConstants.IT_WORKER_CP).compareTo("") != 0) ? System.getProperty(ITConstants.IT_WORKER_CP) : "\"\"";
        String installDir = node.getInstallDir();
        String workerDebug = Boolean.toString(Logger.getLogger(Loggers.WORKER).isDebugEnabled());
        
        // Gets the max cores of the machine
        // int numThreads = r.getMaxTaskCount();
        String[] cmd = new String[16];
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + "adaptors/nio/persistent_worker.sh";
        cmd[1] = libPath.isEmpty() ? "null" : libPath;
        cmd[2] = appDir.isEmpty() ? "null" : appDir;
        cmd[3] = cp.isEmpty() ? "null" : cp;
        cmd[4] = workerDebug;
        cmd[5] = workingDir;
        if (node.getLimitOfTasks()!= 0){
        	cmd[6] = String.valueOf(node.getLimitOfTasks());
        }else{
        	cmd[6] = String.valueOf(NUM_THREADS);
        }
        cmd[7] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[8] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[9] = node.getName();
        cmd[10] = String.valueOf(workerPort);
        cmd[11] = String.valueOf(NIOAdaptor.MASTER_PORT);
        cmd[12] = String.valueOf(tracing_level);
        
        if (tracing) {
            Integer hostId = NIOTracer.registerHost(node.getName(), NUM_THREADS);
            cmd[13] = String.valueOf(hostId.toString());
        } else {
        	cmd[13] ="NoTracinghostID";
        }
        cmd[14] = node.getInstallDir();
        cmd[15] = DEPLOYMENT_ID;
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
        cmd[4] = ((user == null) ? "" : user + "@") + resource;
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
