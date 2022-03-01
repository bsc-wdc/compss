/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

import static java.lang.Math.abs;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.util.TraceEvent;
import es.bsc.compss.util.Tracer;

import java.io.File;


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
     * Initializes the tracing at the given level.
     *
     * @param level Tracing level.
     */
    public static void init(int level, boolean tracingTaskDep) {
        LOGGER.debug("Initializing NIO tracing level [" + level + "," + tracingTaskDep + "]");
        Tracer.enabled = (level != 0);
        tracingTaskDependencies = tracingTaskDep;
    }

    /**
     * Initializes the tracing structures.
     *
     * @param scriptDir COMPSs scripts directory.
     * @param nodeName Node name.
     * @param workingDir Node working directory.
     * @param hostID Tracing host Id.
     */
    public static void setWorkerInfo(String scriptDir, String nodeName, String workingDir, int hostID) {
        NIOTracer.scriptDir = scriptDir;
        NIOTracer.workingDir = workingDir;
        NIOTracer.nodeName = nodeName;
        NIOTracer.hostID = String.valueOf(hostID);

        Tracer.setUpWrapper(hostID, hostID + 1);

        if (DEBUG) {
            LOGGER.debug("Tracer worker for host " + hostID + " and: " + NIOTracer.scriptDir + ", "
                + NIOTracer.workingDir + ", " + NIOTracer.nodeName);
        }
    }

    /**
     * Starts the tracing system at a given worker.
     *
     * @param workerName Worker name.
     * @param workerUser User to connect to the worker.
     * @param workerHost Worker host name.
     * @param numThreads Worker number of threads.
     */
    public static void startTracing(String workerName, String workerUser, String workerHost, Integer numThreads) {
        if (numThreads <= 0) {
            if (DEBUG) {
                LOGGER.debug("Resource " + workerName + " has 0 slots, it won't appear in the trace");
            }
            return;
        }

        if (DEBUG) {
            LOGGER.debug("NIO uri File: " + ProtocolType.ANY_URI.getSchema() + File.separator
                + System.getProperty(COMPSsConstants.APP_LOG_DIR) + TRACE_OUT_RELATIVE_PATH);
            LOGGER.debug(ProtocolType.ANY_URI.getSchema() + File.separator
                + System.getProperty(COMPSsConstants.APP_LOG_DIR) + TRACE_OUT_RELATIVE_PATH);
        }
    }

    /**
     * Returns the host Id.
     *
     * @return The host Id.
     */
    public static String getHostID() {
        return hostID;
    }

    /**
     * Emits a new data transfer event for the given data.
     *
     * @param data Data code to emit the event.
     * @param end If the event is communication end.
     */
    public static void emitDataTransferEvent(String data, boolean end) {
        boolean dataTransfer = !(data.startsWith("worker")) && !(data.startsWith("tracing"))
            && !(data.startsWith("binding")) && !(data.startsWith("cache")) && !(data.endsWith("trace.tar.gz"));

        int transferID = abs(data.hashCode());

        if (dataTransfer) {
            if (end) {
                emitEvent(0, getDataTransfersType());
            } else {
                emitEvent(transferID, getDataTransfersType());
            }
        }

        if (DEBUG) {
            LOGGER.debug((dataTransfer ? "E" : "Not E") + "mitting synchronized data transfer event [name, id] = ["
                + data + " , " + transferID + "]");
        }
    }

    /**
     * Emits a new communication event.
     *
     * @param send Whether it is a send event or not.
     * @param partnerID Transfer partner Id.
     * @param tag Transfer tag.
     */
    public static void emitCommEvent(boolean send, int partnerID, int tag) {
        emitCommEvent(send, ID, partnerID, tag, 0);
    }

    /**
     * Emits a new communication event.
     *
     * @param send Whether it is a send event or not.
     * @param partnerID Transfer partner Id.
     * @param tag Transfer tag.
     * @param size Transfer size.
     */
    public static void emitCommEvent(boolean send, int partnerID, int tag, long size) {
        emitCommEvent(send, ID, partnerID, tag, size);
    }

    /**
     * Generates the tracing package on the worker side.
     */
    public static void generatePackage() {
        if (DEBUG) {
            LOGGER.debug("[NIOTracer] Generating trace package of " + nodeName);
        }
        emitEvent(TraceEvent.STOP.getId(), TraceEvent.STOP.getType());
        emitEvent(Tracer.EVENT_END, TraceEvent.STOP.getType());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e1) {
            // Nothing to do
        }
        Tracer.stopWrapper();

        generatePackage(scriptDir, workingDir, nodeName, hostID);

        // End
        LOGGER.debug("Finish generating");
    }

}
