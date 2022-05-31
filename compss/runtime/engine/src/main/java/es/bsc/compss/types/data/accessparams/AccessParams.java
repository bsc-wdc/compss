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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
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
        C, // Concurrent
        CV // Commutative
    }


    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    protected final Application app;
    protected final AccessMode mode;


    /**
     * Creates a new AccessParams instance.
     *
     * @param app Id of the application accessing the value
     * @param mode Access Mode.
     */
    public AccessParams(Application app, AccessMode mode) {
        this.app = app;
        this.mode = mode;
    }

    /**
     * Returns the Id of the application accessing the value.
     * 
     * @return the Id of the application accessing the value
     */
    public Application getApp() {
        return app;
    }

    /**
     * Returns the access mode.
     *
     * @return The access mode.
     */
    public AccessMode getMode() {
        return this.mode;
    }

    /**
     * Registers the access on a given DataInfoProvider.
     * 
     * @param dip DataInfoProvider where to register the DataAccess
     * @return Description of the dataAccess
     */
    public abstract DataAccessId registerAccess(DataInfoProvider dip);

    /**
     * Registers the completion of the access on a given DataInfoProvider.
     * 
     * @param dip DataInfoProvider where to register the DataAccess
     */
    public abstract void registerAccessCompletion(DataInfoProvider dip);
}
