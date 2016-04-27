package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.api.ITExecution;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOAgent.DataRequest.MasterDataRequest;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.nio.commands.CommandShutdown;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.tracing.CommandGeneratePackage;
import integratedtoolkit.nio.commands.workerFiles.CommandGenerateWorkerDebugFiles;
import integratedtoolkit.nio.master.configuration.NIOConfiguration;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.Copy.DeferredCopy;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.operation.DataOperation.EventListener;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;

import org.apache.log4j.Logger;


public class NIOWorkerNode extends COMPSsWorker {

    protected static final Logger logger = Logger.getLogger(Loggers.COMM);

	private static final int MAX_RETRIES = 5;

    private NIONode node;
    private NIOConfiguration config;
    private NIOAdaptor commManager;
    
    @Override
    public String getName() {
        return config.getHost();
    }

    public NIOWorkerNode(String name, NIOConfiguration config, NIOAdaptor adaptor) {
        super(name, config);

        this.config = config;
        this.commManager = adaptor;

        if (tracing) {
            logger.debug("Initializing NIO tracer " + this.getName());
            NIOTracer.startTracing(this.getName(), this.getUser(), this.getHost(), this.getLimitOfTasks());
        }
    }

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
    
    public int getLimitOfTasks() {
        return config.getLimitOfTasks();
    }

    public void setNode(NIONode node) {
        this.node = node;
    }

    public NIONode getNode() {
        return this.node;
    }
    
    public NIOConfiguration getConfiguration() {
    	return this.config;
    }

    @Override
    public void setInternalURI(URI uri) {
        NIOURI nio = new NIOURI(node, uri.getPath());
        uri.setInternalURI(NIOAdaptor.ID, nio);
    }

    @Override
    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, JobListener listener) {
        return new NIOJob(taskId, taskParams, impl, res, listener);
    }

    @Override
    public void stop(ShutdownListener sl) {
        logger.info("Shutting down " + node);
        for (int retries = 0; retries < MAX_RETRIES; retries++){
        	if (tryShutdown(sl)) {
        		return;
        	} else{
        		try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
        	}
        }
        
        sl.notifyFailure(new Exception());
        logger.error("Shutdown has failed");
    }
    
    private boolean tryShutdown(ShutdownListener sl){
    	Connection c = NIOAgent.tm.startConnection(node);
        commManager.shuttingDown(this, c, sl);
        CommandShutdown cmd = new CommandShutdown(null, null);
        c.sendCommand(cmd);

        c.receive();
        c.finishConnection();

        return true;
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        if (target.getHosts().contains(Comm.appHost)) {//Si es pel master
            //Ordenar la petició directament
            if (tgtData != null) {
                URI u;
                if ((u = ld.alreadyAvailable(Comm.appHost)) != null) {//Already present at the master
                    reason.setDataTarget(u.getPath());
                    listener.notifyEnd(null);
                    return;
                }
            }

            Copy c = new DeferredCopy(ld, null, target, tgtData, reason, listener);
            Data d = new Data(ld);
            if (source != null) {
                for (URI uri : source.getURIs()) {
                    NIOURI nURI = (NIOURI) uri.getInternalURI(NIOAdaptor.ID);
                    if (nURI != null) {
                        d.getSources().add(nURI);
                    }
                }
            }
            String path = target.getPath();
            ld.startCopy(c, c.getTargetLoc());
            NIOAgent.DataRequest dr = new MasterDataRequest(c, reason.getType(), d, path);
            commManager.addTransferRequest(dr);
            commManager.requestTransfers();
        } else {
            orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener));
        }

    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener));
    }

    private void orderCopy(DeferredCopy c) {
        Resource tgtRes = c.getTargetLoc().getHosts().getFirst();
        LogicalData ld = c.getSourceData();
        String path;
        synchronized (ld) {
            URI u;

            if ((c.getTargetData() != null) && (u = ld.alreadyAvailable(tgtRes)) != null) {
                path = u.getPath();
            } else {
                path = c.getTargetLoc().getPath();
            }
            //TODO: MISSING CHECK IF FILE IS ALREADY BEEN COPIED IN A SHARED LOCATION    
            ld.startCopy(c, c.getTargetLoc());
            commManager.registerCopy(c);
        }
        c.setProposedSource(new Data(ld));
        c.setFinalTarget(path);
        c.end(DataOperation.OpEndState.OP_OK);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
    }

    @Override
    public void announceDestruction() {
        //No need to do nothing
    }

    @Override
    public void announceCreation() {
        //No need to do nothing
    }

    @Override
    public String getCompletePath(ITExecution.ParamType type, String name) {
        switch (type) {
            case FILE_T:
                return config.getSandboxWorkingDir() + name;
            case OBJECT_T:
            case SCO_T:
            case PSCO_T:            	
                return name;
            default:
                return null;
        }
    }

    @Override
    public void deleteTemporary() {
        //TODO NIOWorkerNode hauria d'eliminar " + workingDir + " a " + getName());
    }
    
    @Override
    public void generatePackage(){
        logger.debug("Sending command to generated tracing package for "+ this.getHost());
        for (int retries = 0; retries < MAX_RETRIES; retries++){
        	if (tryGeneratePackage()) {
        		commManager.waitUntilTracingPackageGenerated();
        		logger.debug("Tracing Package generated");
        		return;
        	} else {
        		try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					//Nothing to do
				}
        	}
        }
        logger.error("Package generation has failed.");
    }
    
    private boolean tryGeneratePackage(){
    	Connection c = NIOAgent.tm.startConnection(node);
		CommandGeneratePackage cmd = new CommandGeneratePackage (this.getHost(), this.getInstallDir(), this.getWorkingDir(), this.getName());
		c.sendCommand(cmd);
		c.receive();
		
		c.finishConnection();
		return true;
    }
    
    @Override
    public void generateWorkersDebugInfo() {
    	logger.debug("Sending command to generate worker debug files for "+ this.getHost());
        for (int retries = 0; retries < MAX_RETRIES; retries++){
        	if (tryGenerateDebugFiles()) {
        		commManager.waitUntilWorkersDebugInfoGenerated();
        		logger.debug("Worker debug files generated");
        		return;
        	} else {
        		try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					//Nothing to do
				}
        	}
        }
        logger.error("Worker debug files generation has failed.");
    	
    }
    
    private boolean tryGenerateDebugFiles(){
    	Connection c = NIOAgent.tm.startConnection(node);
		CommandGenerateWorkerDebugFiles cmd = new CommandGenerateWorkerDebugFiles (this.getHost(), this.getInstallDir(), this.getWorkingDir(), this.getName());
		c.sendCommand(cmd);
		
		c.receive();
		c.finishConnection();
		return true;
    }
    
}
