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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handling of an access from the main code to a data.
 */
public abstract class MainAccess<V, D extends DataParams, P extends AccessParams<D>> {

    // Component logger
    protected static final Logger LOGGER_API = LogManager.getLogger(Loggers.API);
    protected static final boolean API_DEBUG = LOGGER_API.isDebugEnabled();
    protected static final Logger LOGGER_TD = LogManager.getLogger(Loggers.TD_COMP);
    protected static final boolean DEBUG_TD = LOGGER_TD.isDebugEnabled();

    private final Application app;
    private final P parameters;


    protected MainAccess(Application app, P params) {
        this.app = app;
        this.parameters = params;
    }

    /**
     * Returns the application performing the access.
     *
     * @return application performing the access.
     */
    public Application getApp() {
        return app;
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
     * Returns whether the result of the access should be marked as remaining on the Main process memory.
     *
     * @return {@literal true} if the result is to be marked; {@literal false} otherwise.
     */
    protected abstract boolean resultRemainOnMain();

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
    public abstract V fetch(EngineDataAccessId daId);

    /**
     * Registers the main access and detects the dependencies.
     *
     * @param rdar element to notify when dependencies are discovered
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public EngineDataAccessId register(RegisterDataAccessRequest<V, D, P> rdar) throws ValueUnawareRuntimeException {
        AccessParams<D> accessParams = this.parameters;
        if (DEBUG_TD) {
            Long appId = this.getApp().getId();
            LOGGER_TD.debug("Registering access " + accessParams.toString() + " from App " + appId + "'s main code");
        }
        accessParams.checkAccessValidity();
        EngineDataAccessId accessId = accessParams.register();
        if (accessId == null) {
            if (DEBUG_TD) {
                LOGGER_TD.debug("Accessing a canceled data from main code. Returning null");
            }
        } else {
            if (DEBUG_TD) {
                LOGGER_TD.debug("Registered main access {\"source\":{\"app\":{}},\"access\":{}}", this.app.getId(),
                    accessId.toDebugString());
            }

            if (accessId.isRead()) {
                EngineDataAccessId.ReadingDataAccessId rdaId = (EngineDataAccessId.ReadingDataAccessId) accessId;
                EngineDataInstanceId rdiID = rdaId.getReadDataInstance();
                Application app = this.getApp();
                app.getCP().mainAccess(rdiID);

                // Retrieve writers information
                DataInfo di = accessId.getAccessedDataInfo();
                di.mainAccess(rdar, accessId);
            }
        }
        return accessId;
    }

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

    /**
     * Marks the access from the main as finished.
     *
     * @param generatedData data resulting from the access
     */
    public void finish(EngineDataInstanceId generatedData) {
        if (generatedData != null && this.resultRemainOnMain()) {
            generatedData.getVersion().valueOnMain();
        }
        EngineDataAccessId daid = this.parameters.getLastRegisteredAccess();
        if (daid == null) {
            LOGGER_TD.warn("{} has not been accessed before", this.parameters.getDataDescription());
            return;
        }
        daid.commit();
    }

}
