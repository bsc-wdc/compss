package integratedtoolkit.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import integratedtoolkit.ITConstants;
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

import java.io.File;
import java.util.HashMap;

import org.apache.log4j.Logger;


public class NIOWorkerNode extends COMPSsWorker {

    protected static final Logger logger = Logger.getLogger(Loggers.COMM);

	private static final int MAX_RETRIES = 5;

    private String user;
    private String host;
    private String installDir;
    private String baseWorkingDir;
    private String workingDir;
    private String appDir;
    private String libPath;
    private String queue;
    private int limitOfTasks;
    private NIONode node;
    private NIOAdaptor commManager;
    
    @Override
    public String getName() {
        return host;
    }

    public NIOWorkerNode(String name, HashMap<String, String> properties, NIOAdaptor adaptor) {
        super(name, properties);

        this.user = properties.get(ITConstants.USER);

        this.host = name;
        this.installDir = properties.get(ITConstants.INSTALL_DIR);
        if (this.installDir == null) {
            this.installDir = "";
        } else if (!this.installDir.endsWith(File.separator)) {
            this.installDir = this.installDir + File.separator;
        }

        this.baseWorkingDir = properties.get(ITConstants.WORKING_DIR);
        if (this.baseWorkingDir == null) {
        	// No working dir specified in the project file. Using default tmp
            this.baseWorkingDir = File.separator + "tmp" + File.separator;
        } else if (!this.baseWorkingDir.endsWith(File.separator)) {
            this.baseWorkingDir = this.baseWorkingDir + File.separator;
        }
        
        this.workingDir = this.baseWorkingDir + NIOAdaptor.DEPLOYMENT_ID + File.separator + this.host + File.separator;

        this.appDir = properties.get(ITConstants.APP_DIR);
        if (this.appDir == null) {
            this.appDir = "";
        }

        this.libPath = properties.get(ITConstants.LIB_PATH);
        if (this.libPath == null) {
            this.libPath = "";
        }
        
        this.queue = properties.get("queue");
        if (this.queue == null) {
            this.queue = "";
        }
        String value;
        if ((value = properties.get(ITConstants.LIMIT_OF_TASKS)) != null) {
            this.limitOfTasks = Integer.parseInt(value);
        }
        
        this.commManager = adaptor;
        
        if (tracing) {
            logger.debug("Initializing NIO tracer " + this.getName());
            NIOTracer.startTracing(this.getName(), this.getUser(), this.getHost(), this.getLimitOfTasks());
        }
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public String getInstallDir() {
        return installDir;
    }
    
    public String getBaseWorkingDir() {
        return baseWorkingDir;
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public String getAppDir() {
        return appDir;
    }

    public String getLibPath() {
        return libPath;
    }

    public String getQueue() {
        return queue;
    }
    
    public int getLimitOfTasks() {
        return this.limitOfTasks;
    }

    public void setNode(NIONode node) {
        this.node = node;
    }

    public NIONode getNode() {
        return this.node;
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
                return workingDir + name;
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
