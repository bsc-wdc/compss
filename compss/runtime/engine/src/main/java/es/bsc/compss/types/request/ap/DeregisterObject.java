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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
//import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.ObjectInfo;
import es.bsc.compss.types.request.exceptions.ShutdownException;


public class DeregisterObject extends APRequest {

    private int hash_code;


    public DeregisterObject(Object o) {
        hash_code = o.hashCode();
    }

    @Override
    public APRequestType getRequestType() {

        return APRequestType.DEREGISTER_OBJECT;

    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
            throws ShutdownException {

        ObjectInfo objectInfo = (ObjectInfo) dip.deleteData(hash_code);

        if (objectInfo == null) {
            LOGGER.info("The object with code: " + String.valueOf(hash_code) + " is not used by any task");

            return;
            // I think it's not possible to enter here, the problem we had was that
            // they were not deleted, but I think it's mandatory to log out what happens
        } else {
            LOGGER.info("Data of : " + String.valueOf(hash_code) + " deleted");
        }
        // At this point all the ObjectInfo versions (renamings) are
        // out of the DataInfoProvider data structures

        ta.deleteData(objectInfo);
    }

}
