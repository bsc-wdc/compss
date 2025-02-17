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
package es.bsc.compss.types.data.info;

import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.ObjectData;
import java.util.concurrent.Semaphore;


public class ObjectInfo extends StandardDataInfo<ObjectData> {

    /**
     * Creates a new ObjectInfo instance for the given object.
     *
     * @param object description of the object related to the info
     * @param owner owner of the fileInfo being created
     */
    public ObjectInfo(ObjectData object, DataOwner owner) {
        super(object, owner);
        int code = object.getCode();
        owner.registerObjectData(code, this);
    }

    /**
     * Returns the object hashcode.
     *
     * @return The object hashcode.
     */
    public int getCode() {
        return this.getParams().getCode();
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) {
        // Nothing to wait for
    }

}
