/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.tracing.TraceEvent;


public class SetObjectVersionValueRequest extends APRequest {

    private final String renaming;
    private final Object value;


    /**
     * Creates a new request to set a new value for an specific renaming.
     * 
     * @param renaming Data renaming.
     * @param value New value.
     */
    public SetObjectVersionValueRequest(String renaming, Object value) {
        this.renaming = renaming;
        this.value = value;
    }

    /**
     * Returns the data renaming.
     * 
     * @return The data renaming.
     */
    public String getRenaming() {
        return this.renaming;
    }

    /**
     * Returns the new value.
     * 
     * @return The new value.
     */
    public Object getValue() {
        return this.value;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.setObjectVersionValue(this.renaming, this.value);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.SET_OBJECT_VERSION_VALUE;
    }
}
