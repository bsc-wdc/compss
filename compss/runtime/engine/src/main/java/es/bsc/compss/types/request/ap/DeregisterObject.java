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
//import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.ObjectInfo;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;


public class DeregisterObject extends APRequest {

    private final int hashCode;


    /**
     * Creates a new request to unregister an object.
     * 
     * @param o Object to unregister.
     */
    public DeregisterObject(Object o) {
        this.hashCode = o.hashCode();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.DEREGISTER_OBJECT;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException {
        ObjectInfo objectInfo = (ObjectInfo) dip.deleteData(this.hashCode, true);
        if (objectInfo == null) {
            LOGGER.info("The object with code: " + String.valueOf(this.hashCode) + " is not used by any task");

            return;
            // I think it's not possible to enter here, the problem we had was that
            // they were not deleted, but I think it's mandatory to log out what happens
        } else {
            LOGGER.info("Data of : " + String.valueOf(hashCode) + " deleted");
        }
        // At this point all the ObjectInfo versions (renamings) are
        // out of the DataInfoProvider data structures

        ta.deleteData(objectInfo, false);
    }

}
