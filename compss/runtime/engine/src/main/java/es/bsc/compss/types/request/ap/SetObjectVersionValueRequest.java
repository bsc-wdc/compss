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


    /**
     * Creates a new request to set a new value for an specific renaming.
     * 
     * @param renaming Data renaming.
     */
    public SetObjectVersionValueRequest(String renaming) {
        this.renaming = renaming;
    }

    /**
     * Returns the data renaming.
     * 
     * @return The data renaming.
     */
    public String getRenaming() {
        return this.renaming;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.setObjectVersionValue(this.renaming);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.SET_OBJECT_VERSION_VALUE;
    }
}
