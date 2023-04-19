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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handling of an access from the main code to a data.
 */
public abstract class MainAccess<V extends Object, D extends DataParams, P extends AccessParams<D>> {

    // Component logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final P parameters;


    protected MainAccess(P params) {
        this.parameters = params;
    }

    /**
     * Returns the access parameters associated to the Access.
     *
     * @return parameters of the access
     */
    public final P getParameters() {
        return parameters;
    }

    /**
     * Returns the value expected to be returned when there is no available version for the data.
     * 
     * @return Returns the value expected to be returned when there is no available version for the data.
     */
    public abstract V getUnavailableValueResponse();

    /**
     * Fetches the last version of the accessed data.
     *
     * @param daId Data Access Id.
     * @return last version of the accessed data.
     */
    public abstract V fetch(DataAccessId daId);

    /**
     * Returns whether the registration of the access leads to its immediate finalization.
     *
     * @return {@literal true} if the finalization of the access is to be registers; {@literal false} otherwise.
     */
    public abstract boolean isAccessFinishedOnRegistration();

    protected static DataLocation createLocalLocation(SimpleURI targetURI) {
        DataLocation targetLocation = null;
        try {
            targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetURI, ioe);
        }
        return targetLocation;
    }

}
