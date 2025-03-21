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
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.ObjectAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.ObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.util.concurrent.Semaphore;


/**
 * Handling of an access from the main code to an object.
 */
public class ObjectMainAccess<V, D extends ObjectData, P extends ObjectAccessParams<V, D>> extends MainAccess<V, D, P> {

    private static final String ERROR_OBJECT_LOAD = "ERROR: Cannot load object from storage (file or PSCO)";


    /**
     * Creates a new ObjectMainAccess instance for the given object.
     *
     * @param app Id of the application accessing the object.
     * @param dir operation performed.
     * @param value Associated object.
     * @param code Hashcode of the associated object.
     * @return new ObjectAccessParams instance
     */
    public static final <T> ObjectMainAccess<T, ObjectData, ObjectAccessParams<T, ObjectData>>
        constructOMA(Application app, Direction dir, T value, int code) {
        ObjectAccessParams<T, ObjectData> oap = ObjectAccessParams.constructObjectAP(app, dir, value, code);
        return new ObjectMainAccess<>(app, oap);
    }

    protected ObjectMainAccess(Application app, P params) {
        super(app, params);
    }

    @Override
    public boolean resultRemainOnMain() {
        return true;
    }

    @Override
    public V getUnavailableValueResponse() {
        return null;
    }

    @Override
    public V fetch(EngineDataAccessId daId) {
        if (API_DEBUG) {
            LOGGER_API.debug("Request object transfer " + daId.getDataId());
        }
        EngineDataInstanceId diId = ((ReadingDataAccessId) daId).getReadDataInstance();
        String sourceName = diId.getRenaming();
        if (API_DEBUG) {
            LOGGER_API.debug("Requesting getting object " + sourceName);
        }

        V newValue = null;
        EngineDataInstanceId wId = ((WritingDataAccessId) daId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        LogicalData ld = diId.getData();
        if (ld == null) {
            ErrorManager.error("Unregistered data " + sourceName);
        } else {
            try {
                newValue = fetchObject(ld, daId, sourceName);
                if (API_DEBUG) {
                    LOGGER_API.debug("Object retrieved. Set new version to: " + wRename);
                }
            } catch (Exception e) {
                String errMsg = ERROR_OBJECT_LOAD + ": " + ld.getName();
                LOGGER_API.fatal(errMsg, e);
                ErrorManager.fatal(errMsg, e);
            }
        }
        Comm.registerValue(wRename, newValue);
        return newValue;
    }

    private V fetchObject(LogicalData ld, EngineDataAccessId daId, String sourceName) throws CannotLoadException {
        if (ld.isInMemory()) {
            if (!daId.isPreserveSourceData() && ld.getKnownAlias().size() == 1) {
                return (V) ld.removeValue();
            } else {
                try {
                    ld.writeToStorage();
                } catch (Exception e) {
                    ErrorManager.error("Exception writing object to storage.", e);
                }
            }
        } else {
            if (API_DEBUG) {
                LOGGER_API.debug(
                    "Object " + sourceName + " not in memory. Requesting tranfers to " + Comm.getAppHost().getName());
            }
            DataLocation targetLocation = null;
            String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + sourceName;
            try {
                SimpleURI uri = new SimpleURI(path);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Semaphore sem = new Semaphore(0);
            Comm.getAppHost().getData(ld, targetLocation, new ObjectTransferable(), new OneOpWithSemListener(sem));
            sem.acquireUninterruptibly();
        }

        return (V) ld.readFromStorage();
    }

    @Override
    public boolean isAccessFinishedOnRegistration() {
        return true;
    }
}
