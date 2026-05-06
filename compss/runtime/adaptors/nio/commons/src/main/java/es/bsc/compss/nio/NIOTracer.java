/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.tracing.TransferType;
import es.bsc.compss.util.Tracer;

public class NIOTracer extends Tracer {

    // Random value for the transfer events
    private static final int ID = 121;


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
                emitEventEnd(TransferType.DATA_TRANSFERS);
            } else {
                emitEvent(TransferType.DATA_TRANSFERS, transferID);
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
        emitCommEvent(send, partnerID, ID, tag, 0);
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
        emitCommEvent(send, partnerID, ID, tag, size);
    }

    /**
     * Generates the tracing package on the worker side.
     */
    public static void fini() {
        Tracer.fini();
    }

}
