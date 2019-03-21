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
package es.bsc.compss.nio;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.cepbatools.extrae.Wrapper;

import java.io.File;
import java.io.IOException;

import static java.lang.Math.abs;


public class NIOTracer extends Tracer {

    private static String scriptDir = "";
    private static String workingDir = "";
    private static String nodeName = "master"; // while no worker sets the Tracer info we assume we are on master
    private static String hostID = "0"; // while no worker sets the Tracer info we assume we are on master

    // Random value for the transfer events
    private static final int ID = 121;
    // Id for the end of a transfer event
    public static final String TRANSFER_END = "0";


    /**
     * Initializes the tracing level
     * 
     * @param level
     */
    public static void init(int level) {
        LOGGER.debug("Initializing NIO tracing level [" + level +  "]");
        tracingLevel = level;
    }

    /**
     * Initializes the tracing structures
     * 
     * @param scriptDir
     * @param nodeName
     * @param workingDir
     * @param hostID
     */
    public static void setWorkerInfo(String scriptDir, String nodeName, String workingDir, int hostID) {
        NIOTracer.scriptDir = scriptDir;
        NIOTracer.workingDir = workingDir;
        NIOTracer.nodeName = nodeName;
        NIOTracer.hostID = String.valueOf(hostID);

        synchronized (Tracer.class) {
            Wrapper.SetTaskID(hostID);
            Wrapper.SetNumTasks(hostID + 1);
        }

        if (DEBUG) {
            LOGGER.debug("Tracer worker for host " + hostID + " and: " + NIOTracer.scriptDir + ", " + NIOTracer.workingDir + ", "
                    + NIOTracer.nodeName);
        }
    }

    /**
     * Starts the tracing system
     * 
     * @param workerName
     * @param workerUser
     * @param workerHost
     * @param numThreads
     */
    public static void startTracing(String workerName, String workerUser, String workerHost, Integer numThreads) {
        if (numThreads <= 0) {
            if (DEBUG) {
                LOGGER.debug("Resource " + workerName + " has 0 slots, it won't appear in the trace");
            }
            return;
        }

        if (DEBUG) {
            LOGGER.debug("NIO uri File: " + Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(COMPSsConstants.APP_LOG_DIR)
                    + traceOutRelativePath);
            LOGGER.debug(
                    Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(COMPSsConstants.APP_LOG_DIR) + traceOutRelativePath);
        }
    }

    /**
     * Returns the host ID
     * 
     * @return
     */
    public static String getHostID() {
        return hostID;
    }

    /**
     * Emits a new data transfer event
     * 
     * @param data
     */
    public static void emitDataTransferEvent(String data) {
        boolean dataTransfer = !(data.startsWith("worker")) && !(data.startsWith("tracing"));

        int transferID = (data.equals(TRANSFER_END)) ? 0 : abs(data.hashCode());

        if (dataTransfer) {
            emitEvent(transferID, getDataTransfersType());
        }

        if (DEBUG) {
            LOGGER.debug((dataTransfer ? "E" : "Not E") + "mitting synchronized data transfer event [name, id] = [" + data + " , "
                    + transferID + "]");
        }
    }

    /**
     * Emits a new communication event
     * 
     * @param send
     * @param partnerID
     * @param tag
     */
    public static void emitCommEvent(boolean send, int partnerID, int tag) {
        emitCommEvent(send, partnerID, tag, 0);
    }

    /**
     * Emits a new communication event
     * 
     * @param send
     * @param partnerID
     * @param tag
     * @param size
     */
    public static void emitCommEvent(boolean send, int partnerID, int tag, long size) {
        synchronized (Tracer.class) {
            Wrapper.Comm(send, tag, (int) size, partnerID, ID);
        }

        if (DEBUG) {
            LOGGER.debug("Emitting communication event [" + (send ? "SEND" : "REC") + "] " + tag + ", " + size + ", " + partnerID + ", "
                    + ID + "]");
        }
    }

    /**
     * Generates the tracing package on the worker side
     * 
     */
    public static void generatePackage() {
        emitEvent(Event.STOP.getId(), Event.STOP.getType());
        if (DEBUG) {
            LOGGER.debug("[NIOTracer] Generating trace package of " + nodeName);
        }
        emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        // Stopping Extrae
        synchronized (Tracer.class) {
            LOGGER.debug("[NIOTracer] Disabling pthreads");
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);

            // End wrapper
            LOGGER.debug("[NIOTracer] Finishing extrae");
            Wrapper.Fini();
        }

        // Generate package
        if (DEBUG) {
            LOGGER.debug("[NIOTracer] Executing command " + scriptDir + TRACE_SCRIPT_PATH + " package " + workingDir + " " + nodeName + " "
                    + hostID);
        }

        ProcessBuilder pb = new ProcessBuilder(scriptDir + TRACE_SCRIPT_PATH, "package", workingDir, nodeName, hostID);
        pb.environment().remove(LD_PRELOAD);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            LOGGER.error("Error generating " + nodeName + " package", e);
            return;
        }

        // Only capture output/error if debug level (means 2 more threads)
        if (DEBUG) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, LOGGER);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, LOGGER);
            outputGobbler.start();
            errorGobbler.start();
            LOGGER.debug("Created globbers");
        }

        // Wait completion
        try {
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                LOGGER.error("Error generating " + nodeName + " package, exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Error generating " + nodeName + " package, interruptedException", e);
        }

        // End
        LOGGER.debug("Finish generating");
    }

}
