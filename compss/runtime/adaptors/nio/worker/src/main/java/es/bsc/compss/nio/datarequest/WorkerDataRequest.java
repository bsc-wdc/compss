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
package es.bsc.compss.nio.datarequest;

import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.types.annotations.parameter.DataType;


public class WorkerDataRequest extends DataRequest {

    private final FetchDataListener listener;


    /**
     * Creates a new WorkerDataRequest instance.
     * 
     * @param task FetchDataListener task.
     * @param type Data type.
     * @param source NIO source data.
     * @param target Target path.
     */
    public WorkerDataRequest(FetchDataListener task, DataType type, NIOData source, String target) {
        super(type, source, target);
        this.listener = task;
    }

    /**
     * Returns the associated fetch data listener task.
     *
     * @return The associated fetch data listener task.
     */
    public FetchDataListener getListener() {
        return this.listener;
    }

}
