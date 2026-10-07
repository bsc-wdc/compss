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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.tracing.APRequestEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class GetAvailableRequest implements APRequest {

    private final Application app;
    private final List<DataParams> data;
    private final Semaphore sem;
    private int[] response;
    private final List<Integer> pendingIndexes = new ArrayList<>();


    /**
     * Creates a new request for already accessed data.
     *
     * @param app application accessing the value
     * @param data data whose availability is to be obtained
     */
    public GetAvailableRequest(Application app, List<DataParams> data) {
        this.app = app;
        this.sem = new Semaphore(0);
        this.data = data;
    }

    /**
     * Waits for the completion and returns the response message.
     *
     * @return indexes of the collection whose value is available.
     */
    public final int[] getResponse() {
        this.sem.acquireUninterruptibly();
        if (this.response == null) {
            // Nothing available. Release the CPU slot
            this.app.stalled();
            // Wait for an AvailabilityListener
            this.sem.acquireUninterruptibly();
            this.app.readyToContinue(this.sem);
            this.sem.acquireUninterruptibly();
        }
        return this.response;
    }


    public class AvailabilityListener implements DataVersion.WritersListener {

        private final int index;


        private AvailabilityListener(int index) {
            this.index = index;
        }

        public void written() {
            response = new int[] { index };
            sem.release();
        }
    }


    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        LOGGER.debug("Check availability: {}", data);
        int[] indexes = new int[data.size()];
        int currentIdx = 0;
        int count = 0;
        for (DataParams datum : data) {
            DataInfo dInfo = datum.getRegisteredData(this.app);
            if (dInfo != null) {
                if (!dInfo.addWriterListener(new AvailabilityListener(currentIdx))) {
                    indexes[count] = currentIdx;
                    count++;
                } else {
                    pendingIndexes.add(currentIdx);
                }
            } else {
                // Not registered. Already available
                indexes[count] = currentIdx;
                count++;
            }
            currentIdx++;
        }
        if (count > 0) {
            response = new int[count];
            System.arraycopy(indexes, 0, response, 0, count);
        }
        this.sem.release();
    }

    @Override
    public APRequestEvent getEvent() {
        return APRequestEvent.CHECK_AVAILABILITY;
    }

}
