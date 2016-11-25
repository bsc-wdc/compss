package integratedtoolkit.types.allocatableactions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceScheduler;

/**
 * Representation of a Master Execution Action
 *
 * @param <P>
 * @param <T>
 */
public class MasterExecutionAction<P extends Profile, T extends WorkerResourceDescription> extends ExecutionAction<P, T> {

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);
    
    private final int numSlaves;
    private final Semaphore waitForSlaves;
    private final List<String> slaveWorkerNodeNames;
    private final List<JobStatusListener<P, T>> slaveListeners;

    /**
     * Creates a new master action with a fixed amount of slaves
     * 
     * @param schedulingInformation
     * @param producer
     * @param task
     * @param numSlaves
     * @param forcedResource
     */
    public MasterExecutionAction(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task,
            int numSlaves, ResourceScheduler<P, T> forcedResource) {

        super(schedulingInformation, producer, task, forcedResource);
        
        this.numSlaves = numSlaves;
        this.waitForSlaves = new Semaphore(0);
        this.slaveWorkerNodeNames = new ArrayList<>();
        this.slaveListeners = new ArrayList<>();
    }
    
    /**
     * Handler for a SlaveExecutionAction to notify its readiness to the master
     * 
     * @param w
     * @param listener
     */
    protected void notifySlaveReady(Worker<T> w, JobStatusListener<P, T> listener) {
        this.waitForSlaves.release();
        this.slaveWorkerNodeNames.add(w.getNode().getName());
        this.slaveListeners.add(listener);
    }

    @Override
    protected void transferInputData(JobTransfersListener<P, T> listener) {
        TaskDescription taskDescription = task.getTaskDescription();
        for (Parameter p : taskDescription.getParameters()) {
            JOB_LOGGER.debug("    * " + p);
            if (p instanceof DependencyParameter) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case METHOD:
                        transferJobData(dp, listener);
                        break;
                    case SERVICE:
                        if (dp.getDirection() != DataDirection.INOUT) {
                            // For services we only transfer IN parameters because the only
                            // parameter that can be INOUT is the target
                            transferJobData(dp, listener);
                        }
                        break;
                }
            }
        }
    }

    // Private method that performs data transfers
    private void transferJobData(DependencyParameter param, JobTransfersListener<P, T> listener) {
        Worker<?> w = selectedResource.getResource();
        DataAccessId access = param.getDataAccessId();
        if (access instanceof DataAccessId.WAccessId) {
            String tgtName = ((DataAccessId.WAccessId) access).getWrittenDataInstance().getRenaming();
            if (debug) {
                JOB_LOGGER.debug("Setting data target job transfer: " + w.getCompleteRemotePath(param.getType(), tgtName));
            }
            param.setDataTarget(w.getCompleteRemotePath(param.getType(), tgtName).getPath());

            return;
        }

        listener.addOperation();
        if (access instanceof DataAccessId.RAccessId) {
            String srcName = ((DataAccessId.RAccessId) access).getReadDataInstance().getRenaming();
            w.getData(srcName, srcName, param, listener);
        } else {
            // Is RWAccess
            String srcName = ((DataAccessId.RWAccessId) access).getReadDataInstance().getRenaming();
            String tgtName = ((DataAccessId.RWAccessId) access).getWrittenDataInstance().getRenaming();
            w.getData(srcName, tgtName, (LogicalData) null, param, listener);
        }
    }

    @Override
    public Job<?> submitJob(int transferGroupId, JobStatusListener<P, T> listener) {
        Worker<T> w = selectedResource.getResource();
        
        // Wait for slaves
        try {
            this.waitForSlaves.acquire(this.numSlaves);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Add slave listeners to ours
        listener.addSlaveListeners(this.slaveListeners);
        
        // Create job        
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.selectedImpl, this.slaveWorkerNodeNames, listener);
        job.setTransferGroupId(transferGroupId);
        job.setHistory(Job.JobHistory.NEW);

        return job;
    }

    @Override
    protected void doOutputTransfers(Job<?> job) {
        // Job finished, update info about the generated/updated data
        Worker<T> w = selectedResource.getResource();

        for (Parameter p : job.getTaskParams().getParameters()) {
            if (p instanceof DependencyParameter) {
                // OUT or INOUT: we must tell the FTM about the
                // generated/updated datum
                DataInstanceId dId = null;
                DependencyParameter dp = (DependencyParameter) p;
                switch (p.getDirection()) {
                    case IN:
                        // FTM already knows about this datum
                        continue;
                    case OUT:
                        dId = ((DataAccessId.WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        break;
                    case INOUT:
                        dId = ((DataAccessId.RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        if (job.getType() == TaskType.SERVICE) {
                            continue;
                        }
                        break;
                }

                String name = dId.getRenaming();
                if (job.getType() == TaskType.METHOD) {
                    String targetProtocol = null;
                    switch (dp.getType()) {
                        case FILE_T:
                            targetProtocol = DataLocation.Protocol.FILE_URI.getSchema();
                            break;
                        case OBJECT_T:
                            targetProtocol = DataLocation.Protocol.OBJECT_URI.getSchema();
                            break;
                        case PSCO_T:
                        case EXTERNAL_PSCO_T:
                            targetProtocol = DataLocation.Protocol.PERSISTENT_URI.getSchema();
                            break;
                        default:
                            // Should never reach this point because only
                            // DependencyParameter types are treated
                            // Ask for any_uri just in case
                            targetProtocol = DataLocation.Protocol.ANY_URI.getSchema();
                            break;
                    }

                    DataLocation outLoc = null;
                    try {
                        SimpleURI targetURI = new SimpleURI(targetProtocol + dp.getDataTarget());
                        outLoc = DataLocation.createLocation(w, targetURI);
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
                    }
                    Comm.registerLocation(name, outLoc);
                } else {
                    // Service
                    Object value = job.getReturnValue();
                    Comm.registerValue(name, value);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "MasterExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ")";
    }

}
