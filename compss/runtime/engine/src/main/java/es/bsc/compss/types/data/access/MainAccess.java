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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.data.accessparams.AccessParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handling of an access from the main code to a data.
 */
public abstract class MainAccess<D extends DataParams, P extends AccessParams<D>> {

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
}
