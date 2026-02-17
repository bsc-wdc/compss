/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api.impl;

import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.access.BindingObjectMainAccess;
import es.bsc.compss.types.data.access.ObjectMainAccess;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.params.BindingObjectData;
import es.bsc.compss.types.data.params.CollectionData;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.APIEvent;
import es.bsc.compss.types.tracing.APITracer;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WorkflowImpl extends Application implements Workflow {

    private static final String ERROR_BINDING_OBJECT_PARAMS =
        "ERROR: Incorrect number of parameters for external objects";
    private static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    private static AccessProcessor AP;


    public static void setAP(AccessProcessor ap) {
        WorkflowImpl.AP = ap;
    }

    public WorkflowImpl(String parallelismSource, ApplicationRunner runner) {
        super(parallelismSource, runner);
    }

    @Override
    public Long getId() {
        return super.getId();
    }

    @Override
    public void deregister() {
        APITracer.traced(APIEvent.DEREGISTER_APP, (Runnable) () -> {
            super.deregister();
            AP.deleteAllApplicationDataRequest(this);
        });
    }

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier) {
        APITracer.traced(APIEvent.OPEN_GROUP, (Runnable) () -> {
            AP.setCurrentTaskGroup(groupName, this);
        });
    }

    @Override
    public void closeTaskGroup(String groupName) {
        APITracer.traced(APIEvent.CLOSE_GROUP, (Runnable) () -> {
            AP.closeCurrentTaskGroup(this);
        });
    }

    @Override
    public void cancelTaskGroup(String groupName) throws COMPSsException {
        APITracer.traced(APIEvent.CANCEL_GROUP, (APITracer.ThrowingRunnable) () -> {
            AP.cancelTaskGroup(this, groupName);
            // This is required that changes in metadata have been applied before
            // generating new tasks
            AP.barrierGroup(this, groupName);
        });
    }

    @Override
    public void cancelApplicationTasks() {
        AP.cancelApplicationTasks(this);
    }

    @Override
    public void barrierGroup(String groupName) throws COMPSsException {
        APITracer.traced(APIEvent.WAIT_FOR_GROUP_TASKS, (APITracer.ThrowingRunnable) () -> {
            // Regular barrier
            AP.barrierGroup(this, groupName);
        });
    }

    @Override
    public void barrier() {
        barrier(false);
    }

    @Override
    public void barrier(boolean noMoreTasks) {
        APITracer.traced(APIEvent.WAIT_FOR_ALL_TASKS, (Runnable) () -> {
            // Wait until all tasks have finished
            LOGGER.info("Barrier for app " + this.getId() + " with noMoreTasks = " + noMoreTasks);
            if (noMoreTasks) {
                // No more tasks expected, we can unregister application
                handleNoMoreTasks();
            } else {
                // Regular barrier
                AP.barrier(this);
            }
        });
    }

    @Override
    public void noMoreTasks() {
        APITracer.traced(APIEvent.NO_MORE_TASKS, (Runnable) () -> {
            handleNoMoreTasks();
        });
    }

    /**
     * Notifies the runtime that an application will not produce more tasks.
     */
    public void handleNoMoreTasks() {
        LOGGER.info("No more tasks for app " + this.getId());
        // Wait until all tasks have finished
        AP.noMoreTasks(this);

        this.cancelTimerTask();
        // Retrieve result files
        LOGGER.debug("Getting Result Files for app" + this.getId());
        AP.getResultFiles(this);

    }

    @Override
    public void registerData(DataType type, Object stub, String data) {
        APITracer.traced(APIEvent.REGISTER_DATA, (Runnable) () -> {
            DataParams dp = null;
            switch (type) {
                case DIRECTORY_T:
                case FILE_T:
                    try {
                        String fileName = (String) stub;
                        // Parse arguments to internal structures
                        DataLocation loc;
                        try {
                            loc = COMPSsRuntimeImpl.createLocation(ProtocolType.FILE_URI, fileName);
                        } catch (IOException ioe) {
                            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
                            return;
                        }
                        dp = new FileData(loc);
                    } catch (NullPointerException npe) {
                        LOGGER.error(ERROR_FILE_NAME, npe);
                        ErrorManager.fatal(ERROR_FILE_NAME, npe);
                    }
                    break;
                case OBJECT_T:
                case PSCO_T:
                    int hashcode = System.identityHashCode(stub);
                    dp = new ObjectData(hashcode);
                    break;
                case STREAM_T:
                    // int streamCode = System.identityHashCode(stub);
                    throw new UnsupportedOperationException("Not implemented yet.");
                case EXTERNAL_STREAM_T:
                    try {
                        String fileName = (String) stub;
                        new File(fileName).getName();
                    } catch (NullPointerException npe) {
                        LOGGER.error(ERROR_FILE_NAME, npe);
                        ErrorManager.fatal(ERROR_FILE_NAME, npe);
                    }
                    throw new UnsupportedOperationException("Not implemented yet.");
                case EXTERNAL_PSCO_T:
                    // String id = (String) stub;
                    throw new UnsupportedOperationException("Not implemented yet.");
                case BINDING_OBJECT_T:
                    String value = (String) stub;
                    if (value.contains(":")) {
                        String[] fields = value.split(":");
                        // if (fields.length == 3) {
                        // String extObjectId = fields[0];
                        // int extObjectType = Integer.parseInt(fields[1]);
                        // int extObjectElements = Integer.parseInt(fields[2]);
                        // BindingObject bo = new BindingObject(extObjectId, extObjectType, extObjectElements);
                        // new BindingObject(extObjectId, extObjectType, extObjectElements);
                        // int externalCode = externalObjectHashcode(extObjectId);
                        // externalObjectHashcode(extObjectId);
                        // }
                        if (fields.length != 3) {
                            LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                            ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        }
                    } else {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                    throw new UnsupportedOperationException("Not implemented yet.");
                case COLLECTION_T:
                    dp = new CollectionData((String) stub);
                    break;
                case DICT_COLLECTION_T:
                    throw new UnsupportedOperationException("Not implemented yet.");
                default:
                    // Basic types (including String)
                    // Already passed in as a value
                    break;
            }
            if (dp != null) {
                AP.registerRemoteData(this, dp, data);
            }
        });
    }

    @Override
    public boolean bindExistingVersionToData(String fileName, String dataId) {
        return APITracer.traced(APIEvent.BIND_DATA_TO_VERSION, () -> {
            // Parse the file name
            DataLocation sourceLocation = null;
            try {
                sourceLocation = COMPSsRuntimeImpl.createLocation(ProtocolType.FILE_URI, fileName);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            }
            if (sourceLocation == null) {
                ErrorManager.fatal(ERROR_FILE_NAME);
            }

            FileData fd = new FileData(sourceLocation);
            return bindExistingVersionToData(fd, dataId);
        });
    }

    @Override
    public boolean bindExistingVersionToData(Object o, String dataId) {
        return APITracer.traced(APIEvent.BIND_DATA_TO_VERSION, () -> {
            int hashCode = System.identityHashCode(o);
            ObjectData od = new ObjectData(hashCode);
            return bindExistingVersionToData(od, dataId);
        });
    }

    private boolean bindExistingVersionToData(DataParams data, String dataId) {
        LOGGER.debug("Binding " + data.getDescription() + "'s last version to data " + dataId);
        LogicalData lastVersion = AP.getDataLastVersion(this, data);
        if (lastVersion != null) {
            LogicalData src = Comm.getData(dataId);
            try {
                LOGGER.debug("Binding " + src.getKnownAlias() + " to data " + dataId);
                LogicalData.link(src, lastVersion);
                return true;
            } catch (CommException e) {
                LOGGER.warn("Could not link " + dataId + " and " + lastVersion.getName());
            }
        }
        return false;
    }

    @Override
    public Object getObject(Object obj) {
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        return APITracer.traced(APIEvent.GET_OBJECT, () -> {
            int hashCode = System.identityHashCode(obj);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Getting object with hash code " + hashCode);
            }

            ObjectMainAccess<?, ?, ?> oap = ObjectMainAccess.constructOMA(this, Direction.INOUT, obj, hashCode);
            Object oUpdated;
            try {
                oUpdated = AP.mainAccess(oap);
            } catch (ValueUnawareRuntimeException e) {
                oUpdated = null;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
            }

            return oUpdated;
        });
    }

    @Override
    public String getBindingObject(String fileName) {
        return APITracer.traced(APIEvent.GET_BINDING_OBJECT, () -> {
            // Parse the file name
            LOGGER.debug(" Calling get binding object : " + fileName);
            BindingObject bo = BindingObject.generate(fileName);
            BindingObjectLocation boLoc = new BindingObjectLocation(Comm.getAppHost(), bo);
            String boId = boLoc.getId();
            int hashCode = COMPSsRuntimeImpl.externalObjectHashcode(boId);
            BindingObjectMainAccess boap = BindingObjectMainAccess.constructBOMA(this, Direction.INOUT, bo, hashCode);

            // Otherwise we request it from a task
            String finalPath;
            try {
                BindingObject newBO = AP.mainAccess(boap);
                String bindingObjectID = newBO.getName();
                finalPath = bindingObjectID;
            } catch (ValueUnawareRuntimeException e) {
                finalPath = bo.toString();
            }
            LOGGER.debug("Returning binding object as id: " + finalPath);
            return finalPath;
        });
    }

    @Override
    public boolean removeObject(Object o) {
        APITracer.traced(APIEvent.DELETE_OBJECT, (Runnable) () -> {
            int hashcode = System.identityHashCode(o);
            // This will remove the object from the Object Registry and the Data Info Provider
            // eventually allowing the garbage collector to free it (better use of memory)
            AP.deleteData(this, new ObjectData(hashcode), false, false);
        });
        return true;
    }

    @Override
    public boolean deleteBindingObject(String fileName) {
        // Emit event
        return APITracer.traced(APIEvent.DELETE_BIND_OBJECT, () -> {
            if (fileName == null || fileName.isEmpty()) {
                return false;
            }

            LOGGER.info("Deleting BindingObject " + fileName);

            // Parse the binding object name and translate the access mode
            BindingObject bo = BindingObject.generate(fileName);
            int hashCode = COMPSsRuntimeImpl.externalObjectHashcode(bo.getId());
            AP.deleteData(this, new BindingObjectData(hashCode), false, false);

            // Return deletion was successful
            return true;
        });
    }

    @Override
    public void snapshot() {
        APITracer.traced(APIEvent.SNAPSHOT_API, (Runnable) () -> {
            // Wait until all tasks have finished
            LOGGER.info("Requesting snapshot for application " + this.getId());
            AP.snapshot(this);
        });
    }

}
