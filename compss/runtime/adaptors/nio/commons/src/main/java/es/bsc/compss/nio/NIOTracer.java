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
package es.bsc.compss.nio;

import static java.lang.Math.abs;

import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.Tracer;
import java.util.Map;


public class NIOTracer extends Tracer {

    // Random value for the transfer events
    private static final int ID = 121;
    // Id for the end of a transfer event
    public static final String TRANSFER_END = "0";


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
                emitEventEnd(TraceEventType.DATA_TRANSFERS);
            } else {
                emitEvent(TraceEventType.DATA_TRANSFERS, transferID);
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
     * 
     * @param runtimeEvents pairs name-event id of events not registered in the runtime
     */
    public static void fini(Map<String, Integer> runtimeEvents) {
        emitEvent(TraceEvent.STOP);
        emitEventEnd(TraceEvent.STOP);

        Tracer.fini(runtimeEvents);
    }

}
