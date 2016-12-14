package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.nio.commands.CommandNewTask;
import integratedtoolkit.nio.commands.CommandShutdown;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.tracing.CommandGeneratePackage;
import integratedtoolkit.nio.commands.workerFiles.CommandGenerateWorkerDebugFiles;
import integratedtoolkit.nio.dataRequest.DataRequest;
import integratedtoolkit.nio.dataRequest.MasterDataRequest;
import integratedtoolkit.nio.master.configuration.NIOConfiguration;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.operation.DataOperation.OpEndState;
import integratedtoolkit.types.data.operation.copy.Copy;
import integratedtoolkit.types.data.operation.copy.DeferredCopy;
import integratedtoolkit.types.data.operation.copy.StorageCopy;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


public class NIOWorkerNode extends COMPSsWorker {

    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);

    private NIONode node;
    private final NIOConfiguration config;
    private final NIOAdaptor commManager;
    private boolean started = false;
    private WorkerStarter workerStarter;

    @Override
    public String getName() {
        return config.getHost();
    }

    public NIOWorkerNode(String name, NIOConfiguration config, NIOAdaptor adaptor) {
        super(name, config);
        this.config = config;
        this.commManager = adaptor;
    }

    @Override
    public void start() throws InitNodeException {
        NIONode n = null;
        try {
        	workerStarter = new WorkerStarter(this);
            n = workerStarter.startWorker();
        } catch (InitNodeException e) {
            ErrorManager.warn("There was an exception when initiating worker " + getName() + ".", e);
            throw e;
        }
        this.node = n;

        if (NIOTracer.isActivated()) {
            logger.debug("Initializing NIO tracer " + this.getName());
            NIOTracer.startTracing(this.getName(), this.getUser(), this.getHost(), this.getLimitOfTasks());
        }
    }

    @Override
    public String getUser() {
        return config.getUser();
    }

    public String getHost() {
        return config.getHost();
    }

    public String getInstallDir() {
        return config.getInstallDir();
    }

    public String getBaseWorkingDir() {
        return config.getWorkingDir();
    }

    public String getWorkingDir() {
        return config.getSandboxWorkingDir();
    }

    public String getAppDir() {
        return config.getAppDir();
    }

    public String getLibPath() {
        return config.getLibraryPath();
    }

    @Override
    public String getClasspath() {
        return config.getClasspath();
    }

    @Override
    public String getPythonpath() {
        return config.getPythonpath();
    }

    public int getLimitOfTasks() {
        return config.getLimitOfTasks();
    }

    public int getTotalComputingUnits() {
        return config.getTotalComputingUnits();
    }
    
    public int getTotalGPUs() {
    	return config.getTotalGPUComputingUnits();
    }

    public NIOConfiguration getConfiguration() {
        return this.config;
    }

    @Override
    public void setInternalURI(MultiURI uri) throws UnstartedNodeException {
        if (node == null) {
            throw new UnstartedNodeException();
        }
        NIOURI nio = new NIOURI(node, uri.getPath());
        uri.setInternalURI(NIOAdaptor.ID, nio);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, 
            List<String> slaveWorkersNodeNames, JobListener listener) {
        
        return new NIOJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener);
    }

    @Override
    public void stop(ShutdownListener sl) {
    	if (started){
    		logger.debug("Shutting down " + this.getName());
    		if (node == null) {
    			sl.notifyFailure(new UnstartedNodeException());
    			logger.error("Shutdown has failed");
    		}
            Connection c = NIOAgent.tm.startConnection(node);
            commManager.shuttingDown(this, c, sl);
            CommandShutdown cmd = new CommandShutdown(null, null);
            c.sendCommand(cmd);

            c.receive();
            c.finishConnection();
        } else {
        	logger.debug("Worker " + this.getName() + " has not started. Setting this to be stopped");
        	workerStarter.setToStop();
        	sl.notifyEnd();
        }
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        if (target.getHosts().contains(Comm.getAppHost())) {
            // Request to master

            // Order petition directly
            if (tgtData != null) {
                MultiURI u = ld.alreadyAvailable(Comm.getAppHost());
                if (u != null) { // Already present at the master
                    reason.setDataTarget(u.getPath());
                    listener.notifyEnd(null);
                    return;
                }
            }

            Copy c = new DeferredCopy(ld, null, target, tgtData, reason, listener);
            Data d = new Data(ld);
            if (source != null) {
                for (MultiURI uri : source.getURIs()) {
                    try {
                        NIOURI nURI = (NIOURI) uri.getInternalURI(NIOAdaptor.ID);
                        if (nURI != null) {
                            d.getSources().add(nURI);
                        }
                    } catch (UnstartedNodeException une) {
                        // Ignore internal URI
                    }
                }
            }
            String path = target.getURIInHost(Comm.getAppHost()).getPath();
            ld.startCopy(c, c.getTargetLoc());
            DataRequest dr = new MasterDataRequest(c, reason.getType(), d, path);
            commManager.addTransferRequest(dr);
            commManager.requestTransfers();
        } else {
            // Request to any other
            orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener));
        }
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        if (logger.isDebugEnabled()) {
            logger.debug("Obtain Data " + ld.getName() + " as " + target);
        }

        // If it has a PSCO location, it is a PSCO -> Order new StorageCopy
        for (DataLocation loc : ld.getLocations()) {
            if (loc.getProtocol().equals(Protocol.PERSISTENT_URI)) {
                orderStorageCopy(new StorageCopy(ld, source, target, tgtData, reason, listener));
                return;
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Ordering deferred copy " + ld.getName() );
        }
        orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener));
    }

    private void orderStorageCopy(StorageCopy sc) {
        logger.info("Order PSCO Copy for " + sc.getSourceData().getName());
        if (logger.isDebugEnabled()) {
            logger.debug("LD Target " + sc.getTargetData());
            logger.debug("FROM: " + sc.getPreferredSource());
            logger.debug("TO: " + sc.getTargetLoc());
        }

        LogicalData source = sc.getSourceData();
        LogicalData target = sc.getTargetData();
        if (target != null) {
            if (target.getName().equals(source.getName())) {
                // The source and target are the same --> IN
                newReplica(sc);
            } else {
                // The source and target are different --> OUT
                newVersion(sc);
            }
        } else {
            // Target doesn't exist yet --> INOUT
            newVersion(sc);
        }
    }

    private void newReplica(StorageCopy sc) {
        String targetHostname = this.getName();
        LogicalData srcLD = sc.getSourceData();
        LogicalData targetLD = sc.getTargetData();

        logger.debug("Ask for new Replica of " + srcLD.getName() + " to " + targetHostname);

        // Get the PSCO to replicate
        String pscoId = srcLD.getId();

        // Get the current locations
        List<String> currentLocations = new LinkedList<>();
        try {
            currentLocations = StorageItf.getLocations(pscoId);
        } catch (StorageException se) {
            // Cannot obtain current locations from back-end
            sc.end(OpEndState.OP_FAILED, se);
            return;
        }

        if (!currentLocations.contains(targetHostname)) {
            // Perform replica
            logger.debug("Performing new replica for PSCO " + pscoId);
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.Event.STORAGE_NEWREPLICA.getId(), NIOTracer.Event.STORAGE_NEWREPLICA.getType());
            }
            try {
                // TODO: WARN New replica is NOT necessary because we can't prefetch data
                // StorageItf.newReplica(pscoId, targetHostname);
            } finally {
                if (NIOTracer.isActivated()) {
                    NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_NEWREPLICA.getType());
                }
            }
        } else {
            logger.debug("PSCO " + pscoId + " already present. Skip replica.");
        }

        // Update information
        sc.setFinalTarget(pscoId);
        if (targetLD != null) {
            targetLD.setId(pscoId);
        }
        
        // Notify successful end
        sc.end(OpEndState.OP_OK);
    }

    private void newVersion(StorageCopy sc) {
        String targetHostname = this.getName();
        LogicalData srcLD = sc.getSourceData();
        LogicalData targetLD = sc.getTargetData();

        logger.debug("Ask for new Version of " + srcLD.getName() + " with id " + srcLD.getId() + " to " + targetHostname);

        // Get the PSCOId to replicate
        String pscoId = srcLD.getId();

        // Perform version
        logger.debug("Performing new version for PSCO " + pscoId);
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.STORAGE_NEWVERSION.getId(), NIOTracer.Event.STORAGE_NEWVERSION.getType());
        }
        try {
            String newId = StorageItf.newVersion(pscoId, Comm.getAppHost().getName());
            logger.debug("Register new new version of " + pscoId + " as " + newId);
            sc.setFinalTarget(newId);
            if (targetLD != null) {
                targetLD.setId(newId);
            }
        } catch (Exception e) {
            sc.end(OpEndState.OP_FAILED, e);
            return;
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_NEWVERSION.getType());
            }
        }

        // Notify successful end
        sc.end(OpEndState.OP_OK);
    }

    private void orderCopy(DeferredCopy c) {
        logger.info("Order Copy for " + c.getSourceData());

        Resource tgtRes = c.getTargetLoc().getHosts().getFirst();
        LogicalData ld = c.getSourceData();
        String path;
        synchronized (ld) {
            if (c.getTargetData() != null) {
                MultiURI u = ld.alreadyAvailable(tgtRes);
                if (u != null) {
                    path = u.getPath();
                } else {
                	
                    path = c.getTargetLoc().getURIInHost(tgtRes).getPath();
                }
            } else {
                path = c.getTargetLoc().getURIInHost(tgtRes).getPath();
            }
            c.setProposedSource(new Data(ld));
            logger.debug("Setting final target in deferred copy " + path);
            c.setFinalTarget(path);
            // TODO: MISSING CHECK IF FILE IS ALREADY BEEN COPIED IN A SHARED LOCATION
            ld.startCopy(c, c.getTargetLoc());
            commManager.registerCopy(c);
        }
        c.end(DataOperation.OpEndState.OP_OK);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
    }

    @Override
    public void announceDestruction() {
        // No need to do nothing
    }

    @Override
    public void announceCreation() {
        // No need to do nothing
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
                path = Protocol.FILE_URI.getSchema() + config.getSandboxWorkingDir() + name;
                break;
            case OBJECT_T:
                path = Protocol.OBJECT_URI.getSchema() + name;
                break;
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                // Search for the PSCO id
                String id = Comm.getData(name).getId();
                path = Protocol.PERSISTENT_URI.getSchema() + id;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        // This is only used to clean the master
        // Nothing to do
    }

    @Override
    public boolean generatePackage() {
    	if (started){
    		logger.debug("Sending command to generated tracing package for " + this.getHost());
    		if(node == null) {
    			logger.error("ERROR: Package generation for "+ this.getHost() +" has failed.");
    			return false;
    		}else{
        
    			Connection c = NIOAgent.tm.startConnection(node);
    			CommandGeneratePackage cmd = new CommandGeneratePackage();
    			c.sendCommand(cmd);
    			c.receive();
    			c.finishConnection();
    			commManager.waitUntilTracingPackageGenerated();
    			logger.debug("Tracing Package generated");
    			return true;
    		}
    	}else{
    		logger.debug("Worker " + this.getHost()+ " not started. No tracing package generated");
    		return false;
    	}
    		
    }

    @Override
    public boolean generateWorkersDebugInfo() {
    	if (started){
    		logger.debug("Sending command to generate worker debug files for " + this.getHost());
    		if (node == null) {
    			logger.error("Worker debug files generation has failed.");
    		}
    		
    		Connection c = NIOAgent.tm.startConnection(node);
    		CommandGenerateWorkerDebugFiles cmd = new CommandGenerateWorkerDebugFiles();
    		c.sendCommand(cmd);
    		c.receive();
    		c.finishConnection();

    		commManager.waitUntilWorkersDebugInfoGenerated();
    		logger.debug("Worker debug files generated");
    		return true;
    	}else{
    		logger.debug("Worker debug files not generated because worker was not started");
    		return false;
    	}
    }

    public void submitTask(NIOJob job, LinkedList<String> obsolete) throws UnstartedNodeException {
        if (node == null) {
            throw new UnstartedNodeException();
        }
        NIOTask t = job.prepareJob();
        CommandNewTask cmd = new CommandNewTask(t, obsolete);
        Connection c = NIOAgent.tm.startConnection(node);
        c.sendCommand(cmd);
        c.finishConnection();
    }

	public void setStarted(boolean b) {
		started=b;
		
	}

}
