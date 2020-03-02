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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.TaskProducer;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.accessparams.BindingObjectAccessParams;
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.accessparams.ObjectAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.ap.AlreadyAccessedRequest;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.CancelApplicationTasksRequest;
import es.bsc.compss.types.request.ap.CancelTaskGroupRequest;
import es.bsc.compss.types.request.ap.CloseTaskGroupRequest;
import es.bsc.compss.types.request.ap.DeleteBindingObjectRequest;
import es.bsc.compss.types.request.ap.DeleteFileRequest;
import es.bsc.compss.types.request.ap.DeregisterObject;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.FinishBindingObjectAccessRequest;
import es.bsc.compss.types.request.ap.FinishFileAccessRequest;
import es.bsc.compss.types.request.ap.GetLastRenamingRequest;
import es.bsc.compss.types.request.ap.GetResultFilesRequest;
import es.bsc.compss.types.request.ap.IsObjectHereRequest;
import es.bsc.compss.types.request.ap.NewVersionSameValueRequest;
import es.bsc.compss.types.request.ap.OpenTaskGroupRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.ap.RegisterRemoteObjectDataRequest;
import es.bsc.compss.types.request.ap.SetObjectVersionValueRequest;
import es.bsc.compss.types.request.ap.ShutdownRequest;
import es.bsc.compss.types.request.ap.TaskAnalysisRequest;
import es.bsc.compss.types.request.ap.TaskEndNotification;
import es.bsc.compss.types.request.ap.TasksStateRequest;
import es.bsc.compss.types.request.ap.TransferBindingObjectRequest;
import es.bsc.compss.types.request.ap.TransferObjectRequest;
import es.bsc.compss.types.request.ap.TransferOpenDirectoryRequest;
import es.bsc.compss.types.request.ap.TransferOpenFileRequest;
import es.bsc.compss.types.request.ap.TransferRawFileRequest;
import es.bsc.compss.types.request.ap.UnblockResultFilesRequest;
import es.bsc.compss.types.request.ap.WaitForConcurrentRequest;
import es.bsc.compss.types.request.ap.WaitForDataReadyToDeleteRequest;
import es.bsc.compss.types.request.ap.WaitForTaskRequest;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component to handle the tasks accesses to files and object.
 */
public class AccessProcessor implements Runnable, TaskProducer {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_OBJECT_LOAD_FROM_STORAGE =
        "ERROR: Cannot load object" + " from storage (file or PSCO)";
    private static final String ERROR_QUEUE_OFFER = "ERROR: AccessProcessor queue offer error on ";

    // Other super-components
    protected TaskDispatcher taskDispatcher;

    // Subcomponents
    protected TaskAnalyser taskAnalyser;
    protected DataInfoProvider dataInfoProvider;

    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;

    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;


    /**
     * Creates a new Access Processor instance.
     *
     * @param td Associated TaskDispatcher component.
     */
    public AccessProcessor(TaskDispatcher td) {
        this.taskDispatcher = td;

        // Start Subcomponents
        this.taskAnalyser = new TaskAnalyser();
        this.dataInfoProvider = new DataInfoProvider();

        this.taskAnalyser.setCoWorkers(dataInfoProvider);
        this.requestQueue = new LinkedBlockingQueue<>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName("Access Processor");
        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        processor.start();
        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }
    }

    /**
     * Sets the GraphGenerator co-worker.
     *
     * @param gm co-worker.
     */
    public void setGM(GraphGenerator gm) {
        this.taskAnalyser.setGM(gm);
    }

    /**
     * Returns the internal DataInfoProvider instance.
     *
     * @return The internal DataInfoProvider instance.
     */
    public DataInfoProvider getDataInfoProvider() {
        return this.dataInfoProvider;
    }

    @Override
    public void run() {
        while (keepGoing) {
            APRequest request = null;
            try {
                request = this.requestQueue.take();

                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.getAcessProcessorRequestEvent(request.getRequestType().name()).getId(),
                        Tracer.getRuntimeEventsType());
                }
                request.process(this, this.taskAnalyser, this.dataInfoProvider, this.taskDispatcher);
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }

            } catch (ShutdownException se) {
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                ErrorManager.error("Exception", e);
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            }
        }

        LOGGER.info("AccessProcessor shutdown");
    }

    /**
     * Application: new Method Task.
     *
     * @param appId Application Id.
     * @param monitor Task monitor.
     * @param lang Application language.
     * @param signature Task signature.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of nodes.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed round-robin or not.
     * @param numReturns Number of task returns.
     * @param hasTarget Whether the task has a target object or not.
     * @param parameters Task parameters.
     * @param onFailure OnFailure mechanisms.
     * @param timeOut Time for a task timeOut.
     * @return Task Id.
     */
    public int newTask(Long appId, TaskMonitor monitor, Lang lang, String signature, boolean isPrioritary, int numNodes,
        boolean isReplicated, boolean isDistributed, boolean hasTarget, int numReturns, List<Parameter> parameters,
        OnFailure onFailure, long timeOut) {

        Task currentTask = new Task(appId, lang, signature, isPrioritary, numNodes, isReplicated, isDistributed,
            hasTarget, numReturns, parameters, monitor, onFailure, timeOut);

        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();

        LOGGER.debug("Requesting analysis of Task " + currentTask.getId());
        if (!this.requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new method task");
        }
        return currentTask.getId();
    }

    /**
     * Application: new Service task.
     *
     * @param appId Application Id.
     * @param monitor Task monitor.
     * @param namespace Service namespace.
     * @param service Service name.
     * @param port Service port.
     * @param operation Service operation.
     * @param priority Whether the task has priority or not.
     * @param hasTarget Whether the task has a target object or not.
     * @param numReturns Number of returns of the task.
     * @param parameters Task parameters.
     * @param onFailure OnFailure mechanisms.
     * @param timeOut Time for a task timeOut.
     * @return Task Id.
     */
    public int newTask(Long appId, TaskMonitor monitor, String namespace, String service, String port, String operation,
        boolean priority, boolean hasTarget, int numReturns, List<Parameter> parameters, OnFailure onFailure,
        long timeOut) {
        Task currentTask = new Task(appId, namespace, service, port, operation, priority, hasTarget, numReturns,
            parameters, monitor, onFailure, timeOut);

        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();

        LOGGER.debug("Requesting analysis of new service Task " + currentTask.getId());
        if (!this.requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new service task");
        }
        return currentTask.getId();
    }

    /**
     * Notifies the end of the given abstract task.
     * 
     * @param task Ended task.
     */
    public void notifyTaskEnd(AbstractTask task) {
        if (!this.requestQueue.offer(new TaskEndNotification(task))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "notify task end");
        }
    }

    /**
     * Marks an access to a file as finished.
     *
     * @param sourceLocation File location.
     * @param fap File Access parameters.
     * @param destDir Destination file location.
     */
    public void finishAccessToFile(DataLocation sourceLocation, FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before. Nothing to do");
            return;
        }

        // Tell the DM that the application wants to access a file.
        finishFileAccess(fap);

    }

    private void finishFileAccess(FileAccessParams fap) {
        if (!this.requestQueue.offer(new FinishFileAccessRequest(fap))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing file access");
        }
    }

    /**
     * Notifies a main access to a given file {@code sourceLocation} in mode {@code fap}.
     *
     * @param sourceLocation File location.
     * @param fap File Access Parameters.
     * @param destDir Destination file.
     * @return Final location.
     */
    public DataLocation mainAccessToFile(DataLocation sourceLocation, FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
            LOGGER.debug("File " + faId.getDataId() + " mode contains R, waiting until the last writer has finished");

            waitForTask(faId.getDataId(), AccessMode.R);
            if (this.taskAnalyser.dataWasAccessedConcurrent(faId.getDataId())) {
                waitForConcurrent(faId.getDataId(), fap.getMode());
                this.taskAnalyser.removeFromConcurrentAccess(faId.getDataId());
            }
            if (destDir == null) {
                tgtLocation = transferFileOpen(faId);
            } else {
                DataInstanceId daId;
                if (fap.getMode() == AccessMode.R) {
                    RAccessId ra = (RAccessId) faId;
                    daId = ra.getReadDataInstance();
                } else {
                    RWAccessId ra = (RWAccessId) faId;
                    daId = ra.getReadDataInstance();
                }

                String rename = daId.getRenaming();
                String path = ProtocolType.FILE_URI.getSchema() + destDir + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }

                transferFileRaw(faId, tgtLocation);
            }
        }

        if (fap.getMode() != AccessMode.R && fap.getMode() != AccessMode.C) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW || fap.getMode() == AccessMode.CV) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug("File " + faId.getDataId() + " located on " + tgtLocation.toString());
        }
        return tgtLocation;
    }

    /**
     * Notifies a main access to a given file {@code sourceLocation} in mode {@code fap}.
     *
     * @param sourceLocation DIrectory location.
     * @param fap File Access Parameters.
     * @param destDir Destination directory.
     * @return Final location.
     */
    public DataLocation mainAccessToDirectory(DataLocation sourceLocation, FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("Directory not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
            LOGGER.debug(
                "Directory " + faId.getDataId() + " mode contains R, waiting until the last writer has finished");

            waitForTask(faId.getDataId(), AccessMode.R);
            if (this.taskAnalyser.dataWasAccessedConcurrent(faId.getDataId())) {
                waitForConcurrent(faId.getDataId(), fap.getMode());
                this.taskAnalyser.removeFromConcurrentAccess(faId.getDataId());
            }
            tgtLocation = transferDirectoryOpen(faId);
        }

        if (fap.getMode() != AccessMode.R && fap.getMode() != AccessMode.C) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW || fap.getMode() == AccessMode.CV) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = ProtocolType.DIR_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug("Directory " + faId.getDataId() + " located on " + tgtLocation.toString());
        }
        return tgtLocation;
    }

    /**
     * Returns whether the value with hashCode {@code hashCode} is valid or obsolete.
     *
     * @param hashCode Object hashcode.
     * @return {@code true} if the object is valid, {@code false} otherwise.
     */
    public boolean isCurrentRegisterValueValid(int hashCode) {
        LOGGER.debug("Checking if value of object with hashcode " + hashCode + " is valid");

        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "valid object value");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Log response and return
        boolean isValid = request.getResponse();
        if (DEBUG) {
            if (isValid) {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is valid");
            } else {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is NOT valid");
            }
        }

        return isValid;
    }

    /**
     * Notifies a main access to an object {@code obj}.
     *
     * @param obj Object.
     * @param hashCode Object hashcode.
     * @return Synchronized object.
     */
    public Object mainAcessToObject(Object obj, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        ObjectAccessParams oap = new ObjectAccessParams(AccessMode.RW, this.dataInfoProvider, obj, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }

        // Defaut access is read because the binding object is removed after accessing it
        waitForTask(oaId.getDataId(), AccessMode.RW);
        if (this.taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            waitForConcurrent(oaId.getDataId(), AccessMode.RW);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                this.taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }
        // TODO: Check if the object was already piggybacked in the task notification
        // Ask for the object
        if (DEBUG) {
            LOGGER.debug("Request object transfer " + oaId.getDataId() + " with renaming " + wRename);
        }
        Object oUpdated = obtainObject(oaId);

        if (DEBUG) {
            LOGGER.debug("Object retrieved. Set new version to: " + wRename);
        }
        setObjectVersionValue(wRename, oUpdated);
        return oUpdated;
    }

    /**
     * Notifies a main access to an external PSCO {@code id}.
     *
     * @param id PSCO Id.
     * @param hashCode Object hashcode.
     * @return Location containing final the PSCO Id.
     */
    public String mainAcessToExternalPSCO(String id, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to external object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        ObjectAccessParams oap = new ObjectAccessParams(AccessMode.RW, this.dataInfoProvider, id, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }

        waitForTask(oaId.getDataId(), AccessMode.RW);
        if (this.taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            waitForConcurrent(oaId.getDataId(), AccessMode.RW);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                this.taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }

        // TODO: Check if the object was already piggybacked in the task notification
        String lastRenaming = ((RWAccessId) oaId).getReadDataInstance().getRenaming();
        String newId = Comm.getData(lastRenaming).getPscoId();

        return ProtocolType.PERSISTENT_URI.getSchema() + newId;
    }

    private String obtainBindingObject(RAccessId oaId) {
        // String lastRenaming = (oaId).getReadDataInstance().getRenaming();
        // TODO: Add transfer request similar than java object
        LOGGER.debug("[AccessProcessor] Obtaining binding object with id " + oaId);
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferBindingObjectRequest tor = new TransferBindingObjectRequest(oaId, sem);
        if (!this.requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();
        BindingObject bo = BindingObject.generate(tor.getTargetName());
        return bo.getName();
    }

    /**
     * Notifies a main access to an external binding object.
     *
     * @param bo Binding object.
     * @param hashCode Binding object's hashcode.
     * @return Location containing the binding's object final path.
     */
    public String mainAcessToBindingObject(BindingObject bo, int hashCode) {
        if (DEBUG) {
            LOGGER.debug(
                "Requesting main access to binding object with bo " + bo.toString() + " and hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        // AccessParams.BindingObjectAccessParams oap = new AccessParams.BindingObjectAccessParams(AccessMode.RW, bo,
        // hashCode);
        BindingObjectAccessParams oap =
            new BindingObjectAccessParams(AccessMode.R, this.dataInfoProvider, bo, hashCode);
        DataAccessId oaId = registerDataAccess(oap);

        // DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        // String wRename = wId.getRenaming();
        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId());
        }

        // Defaut access is read because the binding object is removed after accessing it
        waitForTask(oaId.getDataId(), AccessMode.R);
        if (this.taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            // Defaut access is read because the binding object is removed after accessing it
            waitForConcurrent(oaId.getDataId(), AccessMode.R);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                this.taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }
        // String lastRenaming = ((DataAccessId.RWAccessId) oaId).getReadDataInstance().getRenaming();
        // return obtainBindingObject((DataAccessId.RWAccessId)oaId);
        String bindingObjectID = obtainBindingObject((RAccessId) oaId);

        finishBindingObjectAccess(oap);

        return bindingObjectID;
    }

    private void finishBindingObjectAccess(BindingObjectAccessParams boAP) {
        if (!this.requestQueue.offer(new FinishBindingObjectAccessRequest(boAP))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing binding object access");
        }
    }

    /**
     * Notification for no more tasks.
     *
     * @param appId Application Id.
     */
    public void noMoreTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new EndOfAppRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "no more tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("All tasks finished");
    }

    /**
     * Returns whether the @{code loc} has already been accessed or not.
     *
     * @param loc Location.
     * @return {@code true} if the location has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(DataLocation loc) {
        Semaphore sem = new Semaphore(0);
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(loc, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Barrier.
     *
     * @param appId Application Id.
     */
    public void barrier(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new BarrierRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("Barrier: End of waited all tasks");
    }

    /**
     * Barrier for group.
     *
     * @param groupName Name of the task group
     * @throws COMPSsException Exception thrown by user
     */
    public void barrierGroup(Long appId, String groupName) throws COMPSsException {
        Semaphore sem = new Semaphore(0);
        BarrierGroupRequest request = new BarrierGroupRequest(appId, groupName, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }
        // Wait for response
        sem.acquireUninterruptibly();

        if (request.getException() != null) {
            LOGGER.debug("The thrown exception message is: " + request.getException().getMessage());
            throw new COMPSsException(
                "Group " + groupName + " raised a COMPSs Exception ( " + request.getException().getMessage() + ")");
        }

        LOGGER.info("Group barrier: End of tasks of group " + groupName);
    }

    /**
     * Cancellation of all tasks of an application.
     * 
     * @param appId Application Id.
     */
    public void cancelApplicationTasks(Long appId) {
        LOGGER.info("Cancelled all remaining tasks for application with id " + appId);

        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new CancelApplicationTasksRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }
        // Wait for response
        LOGGER.debug("Waiting for finishing tasks cancellation " + appId);
        sem.acquireUninterruptibly();

        LOGGER.info("Tasks cancelled for application with id " + appId);
    }

    @Override
    public void cancelTaskGroup(Long appId, String groupName) {
        LOGGER.info("Cancel remaining tasks for application " + appId + " and group " + groupName);

        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new CancelTaskGroupRequest(appId, groupName, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }
        // Wait for response
        LOGGER.debug("Waiting for cancellation of tasks in group " + groupName);
        sem.acquireUninterruptibly();

        LOGGER.info("Tasks cancelled for group " + groupName);
    }

    /**
     * Synchronism for an specific data.
     *
     * @param dataId Data Id.
     * @param mode Access mode.
     */
    private void waitForTask(int dataId, AccessMode mode) {
        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new WaitForTaskRequest(dataId, mode, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("End of waited task for data " + dataId);
    }

    /**
     * Synchronism for a concurrent task.
     *
     * @param dataId Data Id.
     * @param accessMode Access mode.
     */
    private void waitForConcurrent(int dataId, AccessMode accessMode) {
        Semaphore sem = new Semaphore(0);
        Semaphore semTasks = new Semaphore(0);
        WaitForConcurrentRequest request = new WaitForConcurrentRequest(dataId, accessMode, sem, semTasks);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for concurrent task");
        }

        // Wait for response
        sem.acquireUninterruptibly();
        int n = request.getNumWaitedTasks();
        semTasks.acquireUninterruptibly(n);
        LOGGER.info("End of waited concurrent task for data " + dataId);
    }

    /**
     * Registers a new data access.
     *
     * @param access Access parameters.
     * @return The registered access Id.
     */
    private DataAccessId registerDataAccess(AccessParams access) {
        Semaphore sem = new Semaphore(0);
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data access");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Registers a new version of file/object with the same value.
     *
     * @param rRenaming Read renaming path.
     * @param wRenaming Write renaming path.
     */
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        NewVersionSameValueRequest request = new NewVersionSameValueRequest(rRenaming, wRenaming);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new version");
        }
    }

    /**
     * Sets a new value to a specific version of a file/object.
     *
     * @param renaming Renaming version.
     * @param value New value.
     */
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new object version value");
        }
    }

    /**
     * Sets the task group to assign to all the following tasks.
     * 
     * @param groupName Name of the task group
     */
    public void setCurrentTaskGroup(String groupName, boolean implicitBarrier, Long appId) {
        OpenTaskGroupRequest request = new OpenTaskGroupRequest(groupName, implicitBarrier, appId);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new task group");
        }
    }

    /**
     * Closes the current task group.
     */
    public void closeCurrentTaskGroup(Long appId) {
        CloseTaskGroupRequest request = new CloseTaskGroupRequest(appId);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "closure of task group");
        }
    }

    /**
     * Returns the last version of a file/object with code {@code code}.
     *
     * @param code File code.
     * @return Renaming of the last version.
     */
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get last renaming");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Unblock result files.
     *
     * @param resFiles List of result files to unblock.
     */
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unblock result files");
        }
    }

    /**
     * Shutdown request.
     */
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new ShutdownRequest(sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }

        // Wait for response
        sem.acquireUninterruptibly();
    }

    /**
     * Returns a string with the description of the tasks in the graph.
     *
     * @return The description of the current tasks in the graph.
     */
    public String getCurrentTaskState() {
        Semaphore sem = new Semaphore(0);
        TasksStateRequest request = new TasksStateRequest(sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get current task state");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return (String) request.getResponse();
    }

    /**
     * Marks a location for deletion.
     *
     * @param loc Location to delete.
     */
    public void markForDeletion(DataLocation loc, boolean enableReuse) {
        LOGGER.debug("Marking data " + loc + " for deletion");
        Semaphore sem = new Semaphore(0);
        Semaphore semWait = new Semaphore(0);
        // No need to wait if data is noReuse
        if (enableReuse) {
            WaitForDataReadyToDeleteRequest request = new WaitForDataReadyToDeleteRequest(loc, sem, semWait);
            // Wait for data to be ready for deletion
            if (!this.requestQueue.offer(request)) {
                ErrorManager.error(ERROR_QUEUE_OFFER + "wait for data ready to delete");
            }

            // Wait for response
            LOGGER.debug("Waiting for ready to delete request response...");
            sem.acquireUninterruptibly();

            int nPermits = request.getNumPermits();
            if (nPermits > 0) {
                LOGGER.debug("Waiting for " + nPermits + " tasks to finish...");
                semWait.acquireUninterruptibly(nPermits);
            }
        }
        // Request to delete data
        LOGGER.debug("Sending delete request response for " + loc);
        if (!this.requestQueue.offer(new DeleteFileRequest(loc, sem, !enableReuse))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }

        LOGGER.debug("Waiting for delete request response...");
        // Wait for response
        sem.acquireUninterruptibly();
        LOGGER.debug("Data " + loc + " deleted.");
    }

    /**
     * Marks a BindingObject for its deletion.
     *
     * @param code BindingObject code.
     */
    public void markForBindingObjectDeletion(int code) {
        if (!this.requestQueue.offer(new DeleteBindingObjectRequest(code))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
    }

    /**
     * Adds a request for file raw transfer.
     *
     * @param faId Data Access Id.
     * @param location File location.
     */
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        RAccessId faRId = (RAccessId) faId;
        TransferRawFileRequest request = new TransferRawFileRequest(faRId, location, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file raw");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Raw file transferred");
    }

    /**
     * Adds a request for open file transfer.
     *
     * @param faId Data Access Id.
     * @return Location of the transferred open file.
     */
    private DataLocation transferFileOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenFileRequest request = new TransferOpenFileRequest(faId, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open file transferred");
        return request.getLocation();
    }

    /**
     * Adds a request for open file transfer.
     *
     * @param faId Data Access Id.
     * @return Location of the transferred open file.
     */
    private DataLocation transferDirectoryOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenDirectoryRequest req = new TransferOpenDirectoryRequest(faId, sem);
        if (!this.requestQueue.offer(req)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer directory open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open directory transferred");
        return req.getLocation();
    }

    /**
     * Adds a request to obtain an object from a worker to the master.
     *
     * @param oaId Data Access Id.
     * @return The synchronized object value.
     */
    private Object obtainObject(DataAccessId oaId) {
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        if (!this.requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Get response
        Object oUpdated = tor.getResponse();
        if (oUpdated == null) {
            /*
             * The Object didn't come from a WS but was transferred from a worker, we load it from its storage (file or
             * persistent)
             */
            LogicalData ld = tor.getTargetData();
            try {
                ld.loadFromStorage();
                oUpdated = ld.getValue();
            } catch (CannotLoadException e) {
                LOGGER.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
                ErrorManager.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
            }
        }

        return oUpdated;
    }

    /**
     * Adds a request to retrieve the result files from the workers to the master.
     *
     * @param appId Application Id.
     */
    public void getResultFiles(Long appId) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(appId, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get result files");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        if (!this.requestQueue.offer(urfr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unlock result files");
        }
    }

    /**
     * Unregisters the given object.
     *
     * @param o Object to unregister.
     */
    public void deregisterObject(Object o) {
        if (!this.requestQueue.offer(new DeregisterObject(o))) {

            ErrorManager.error(ERROR_QUEUE_OFFER + "deregister object");
        }
    }

    /**
     * Registers a data value as available on remote locations.
     *
     * @param code code identifying the object
     * @param dataId name of the data associated to the object
     */
    public void registerRemoteObject(int code, String dataId) {
        RegisterRemoteObjectDataRequest request = new RegisterRemoteObjectDataRequest(code, dataId);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data");
        }
    }

}
