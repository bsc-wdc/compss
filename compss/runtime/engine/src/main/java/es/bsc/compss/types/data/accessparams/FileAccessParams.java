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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams.FileData;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;


public class FileAccessParams<D extends FileData> extends AccessParams<D> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new FileAccessParams instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param dir operation performed.
     * @param loc File location.
     * @return new FileAccessParams instance
     */
    public static final FileAccessParams constructFAP(Application app, Direction dir, DataLocation loc) {
        FileData fd = new FileData(app, loc);
        return new FileAccessParams(fd, dir);
    }

    protected FileAccessParams(D data, Direction dir) {
        super(data, dir);
    }

    /**
     * Returns the file location.
     * 
     * @return The file location.
     */
    public final DataLocation getLocation() {
        return this.data.getLocation();
    }

    @Override
    public void checkAccessValidity(DataInfoProvider dip) throws ValueUnawareRuntimeException {
        boolean alreadyAccessed = dip.alreadyAccessed(this.getData());
        if (!alreadyAccessed) {
            LOGGER.debug(this.getDataDescription() + " accessed before, returning the same location");
            throw new ValueUnawareRuntimeException();
        }
    }

    @Override
    public void registeredAsFirstVersionForData(DataInfo dInfo) {
        DataVersion dv = dInfo.getCurrentDataVersion();
        if (mode != AccessMode.W) {
            DataInstanceId lastDID = dv.getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerLocation(renaming, this.getLocation());
        } else {
            dv.invalidate();
        }
    }

    @Override
    public boolean resultRemainOnMain() {
        return false;
    }

    @Override
    public void externalRegister() {
        // Do nothing. No need to register the access anywhere.
    }

    @Override
    public String toString() {
        return "[" + this.getApp() + ", " + this.mode + " ," + this.getLocation() + "]";
    }

}
