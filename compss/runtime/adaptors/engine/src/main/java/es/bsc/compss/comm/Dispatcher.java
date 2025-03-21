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
package es.bsc.compss.comm;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Dispatcher extends RequestDispatcher<DataOperation> {

    // Log and debug
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    /**
     * Creates a new Dispatcher instance.
     * 
     * @param queue Associated request queue.
     */
    public Dispatcher(RequestQueue<DataOperation> queue) {
        super(queue);
    }

    @Override
    public void processRequests() {
        DataOperation fOp;
        while (true) {
            fOp = queue.dequeue();
            if (fOp == null) {
                break;
            }
            fOp.perform();
        }
    }

}
