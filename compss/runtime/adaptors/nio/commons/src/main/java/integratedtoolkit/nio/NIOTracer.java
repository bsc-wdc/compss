package integratedtoolkit.nio;

import integratedtoolkit.ITConstants;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.util.Tracer;
import integratedtoolkit.util.StreamGobbler;
import es.bsc.cepbatools.extrae.Wrapper;

import java.io.File;
import java.io.IOException;

import static java.lang.Math.abs;


public class NIOTracer extends Tracer {

    private static String scriptDir = "";
    private static String workingDir = "";
    private static String nodeName = "master"; // while no worker sets the Tracer info we assume we are on master
    private static final int ID = 121; // Random value

    public static final String TRANSFER_END = "0";


    public static void init(int level) {
        if (debug) {
            logger.debug("Initializing NIO tracing");
        }
        tracing_level = level;
    }

    public static void startTracing(String workerName, String workerUser, String workerHost, Integer numThreads) {
        if (numThreads <= 0) {
            if (debug) {
                logger.debug("Resource " + workerName + " has 0 slots, it won't appear in the trace");
            }
            return;
        }

        if (debug) {
            logger.debug("NIO uri File: " + Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(ITConstants.IT_APP_LOG_DIR) + traceOutRelativePath);
            logger.debug(Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(ITConstants.IT_APP_LOG_DIR) + traceOutRelativePath);
        }
    }

    public static void setWorkerInfo(String scriptDir, String nodeName, String workingDir, int hostID) {
        NIOTracer.scriptDir = scriptDir;
        NIOTracer.workingDir = workingDir;
        NIOTracer.nodeName = nodeName;

        synchronized (Tracer.class) {
            Wrapper.SetTaskID(hostID);
            Wrapper.SetNumTasks(hostID + 1);
        }

        if (debug) {
            logger.debug("Tracer worker for host " + hostID + " and: " + NIOTracer.scriptDir + ", " 
                            + NIOTracer.workingDir + ", " + NIOTracer.nodeName);
        }
    }

    public static void emitDataTransferEvent(String data) {
        boolean dataTransfer = !(data.startsWith("worker")) && !(data.startsWith("tracing"));

        int transferID = (data.equals(TRANSFER_END)) ? 0 : abs(data.hashCode());

        if (dataTransfer) {
            emitEvent(transferID, getDataTransfersType());
        }

        if (debug) {
            logger.debug((dataTransfer ? "E" : "Not E") + "mitting synchronized data transfer event [name, id] = [" + data + " , "
                    + transferID + "]");
        }
    }

    public static void emitCommEvent(boolean send, int partnerID, int tag) {
        emitCommEvent(send, partnerID, tag, 0);
    }

    public static void emitCommEvent(boolean send, int partnerID, int tag, long size) {
        synchronized (Tracer.class) {
            Wrapper.Comm(send, tag, (int) size, partnerID, ID);
        }

        if (debug) {
            logger.debug("Emitting communication event [" + (send ? "SEND" : "REC") + "] " + tag + ", " + size + ", " + partnerID + ", "
                    + ID + "]");
        }
    }

    public static void generatePackage() {
        emitEvent(Event.STOP.getId(), Event.STOP.getType());
        if (debug) {
            logger.debug("Generating package of " + nodeName + ", with " + scriptDir);
        }
        emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());

        synchronized (Tracer.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);

            // End wrapper
            Wrapper.Fini();
        }
        // Generate package
        ProcessBuilder pb = new ProcessBuilder(scriptDir + TRACE_SCRIPT_PATH, "package", workingDir, nodeName);
        pb.environment().remove(LD_PRELOAD);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            logger.error("Error generating " + nodeName + " package", e);
            return;
        }

        // Only capture output/error if debug level (means 2 more threads)
        if (debug) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, logger);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, logger);
            outputGobbler.start();
            errorGobbler.start();
            logger.debug("Created globbers");
        }

        try {
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                logger.error("Error generating " + nodeName + " package, exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            logger.error("Error generating " + nodeName + " package (interruptedException) : " + e.getMessage());
        }
        if (debug) {
            logger.debug("Finish generating");
        }
    }

}
