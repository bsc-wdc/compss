/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.data.DataAccessId;

import java.io.Serializable;


/**
 * Description of the access parameters to an object, file, stream, or binding-object.
 */
public abstract class AccessParams implements Serializable {

    public static enum AccessMode {
        R, // Read
        W, // Write
        RW, // ReadWrite
        C // Concurrent
    }


    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    protected final AccessMode mode;
    protected final DataInfoProvider dip;


    /**
     * Creates a new AccessParams instance.
     * 
     * @param mode Access Mode.
     */
    public AccessParams(AccessMode mode, DataInfoProvider dip) {
        this.mode = mode;
        this.dip = dip;
    }

    /**
     * Returns the access mode.
     * 
     * @return The access mode.
     */
    public AccessMode getMode() {
        return this.mode;
    }

    public abstract DataAccessId register();
}
