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
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;

import java.io.Serializable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Description of the access parameters to an object, file, stream, or binding-object.
 */
public abstract class AccessParams<D extends DataParams> implements Serializable {

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

    // Component logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected final D data;
    protected final AccessMode mode;


    private static AccessMode getAccessMode(Direction d) {
        AccessMode am = AccessMode.R;
        switch (d) {
            case IN:
            case IN_DELETE:
                am = AccessParams.AccessMode.R;
                break;
            case OUT:
                am = AccessParams.AccessMode.W;
                break;
            case INOUT:
                am = AccessParams.AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessParams.AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessParams.AccessMode.CV;
                break;
        }
        return am;
    }

    /**
     * Creates a new AccessParams instance.
     *
     * @param data Data being accessed
     * @param dir operation performed.
     */
    protected AccessParams(D data, Direction dir) {
        this.data = data;
        this.mode = getAccessMode(dir);
    }

    /**
     * Returns the Id of the application accessing the value.
     * 
     * @return the Id of the application accessing the value
     */
    public final Application getApp() {
        return data.getApp();
    }

    /**
     * Returns the data being accessed.
     * 
     * @return data being accessed
     */
    public D getData() {
        return data;
    }

    /**
     * Returns the access mode.
     *
     * @return The access mode.
     */
    public final AccessMode getMode() {
        return this.mode;
    }

    public Integer getDataId(DataInfoProvider dip) {
        return data.getDataId(dip);
    }

    public String getDataDescription() {
        return data.getDescription();
    }

    /**
     * Verifies that the runtime is aware of the value and the access should be registered.
     *
     * @param dip DataInfoProvider
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public abstract void checkAccessValidity(DataInfoProvider dip) throws ValueUnawareRuntimeException;

    public abstract void registeredAsFirstVersionForData(DataInfo dInfo);

    /**
     * Returns whether the result of the access should be marked as remaining on the Main process memory.
     * 
     * @return {@literal true} if the result is to be marked; {@literal false} otherwise.
     */
    public abstract boolean resultRemainOnMain();

    /**
     * Registers the access into an external service.
     */
    public abstract void externalRegister();
}
