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
import es.bsc.compss.api.Workflow;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.tracing.APIEvent;
import es.bsc.compss.types.tracing.APITracer;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WorkflowImpl extends Application implements Workflow {

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
    public void snapshot() {
        APITracer.traced(APIEvent.SNAPSHOT_API, (Runnable) () -> {
            // Wait until all tasks have finished
            LOGGER.info("Requesting snapshot for application " + this.getId());
            AP.snapshot(this);
        });
    }

}
