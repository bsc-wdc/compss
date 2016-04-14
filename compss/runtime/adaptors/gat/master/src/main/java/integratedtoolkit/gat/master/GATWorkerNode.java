package integratedtoolkit.gat.master;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.ITExecution;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.DataOperation.EventListener;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.util.SSHManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;


public class GATWorkerNode extends COMPSsWorker {

    private static final String GAT_CLEAN_SCRIPT = "adaptors/gat/clean.sh";

    private String host = "";
    private String user = "";
    private String installDir = "";
    private String sandboxWorkingDir = "";
    private String workingDir = "";
    private String appDir = "";
    private String libPath = "";
    private String queue = "";
    private int limitOfTasks = 16;

    private org.gridlab.gat.resources.Job tracingJob;

	private GATContext context;

	private boolean usingGlobus;

	private boolean userNeeded;

    @Override
    public String getName() {
        return host;
    }

    public GATWorkerNode(String name, HashMap<String, String> properties, AdaptorDescription adaptorDescription) {
        super(name, properties);
        String adaptorName = System.getProperty(ITConstants.GAT_BROKER_ADAPTOR);
        if (adaptorDescription != null){
        	String ad = adaptorDescription.getBrokerAdaptor();
        	if (ad!=null && !ad.isEmpty()){
        		adaptorName = ad;
        	}else{
        		logger.debug("GAT Broker Adaptor not specified. Setting default value " + adaptorName);
        	}
        }else{
        	logger.debug("GAT Adaptor description not found in the resource description. Setting default value " + adaptorName);
        }
        initContext(adaptorName, System.getProperty(ITConstants.GAT_FILE_ADAPTOR));
        this.host = name;

        this.installDir = properties.remove(ITConstants.INSTALL_DIR);
        if (this.installDir == null) {
            this.installDir = "";
        } else if (!this.installDir.endsWith(File.separator)) {
            this.installDir = this.installDir + File.separator;
        }
        

        String baseWorkingDir = properties.remove(ITConstants.WORKING_DIR);
        this.sandboxWorkingDir = baseWorkingDir + System.getProperty(ITConstants.IT_DEPLOYMENT_ID);
        this.workingDir = this.sandboxWorkingDir + File.separator + this.getName();
        
        File wDir = new File(this.workingDir);
        wDir.mkdirs();
        if (!wDir.exists()) {
            logger.error("Could not create GAT working working: " + this.workingDir);
        }

        if (this.workingDir == null) {
            this.workingDir = "";
        } else if (!this.workingDir.endsWith(File.separator)) {
            this.workingDir = this.workingDir + File.separator;
        }

        if ((this.user = properties.remove(ITConstants.USER)) == null) {
            this.user = "";
        }

        if ((this.appDir = properties.remove(ITConstants.APP_DIR)) == null) {
            this.appDir = "null";
        }

        if ((this.libPath = properties.remove(ITConstants.LIB_PATH)) == null) {
            this.libPath = "null";
        }
        if ((this.queue = properties.remove("queue")) == null) {
            this.queue = "";
        }

        String value;
        if ((value = properties.get(ITConstants.LIMIT_OF_TASKS)) != null) {
            limitOfTasks = Integer.parseInt(value);
        }

        for (java.util.Map.Entry<String, String> entry : properties.entrySet()) {
            String propName = entry.getKey();
            String propValue = entry.getValue();
            if (propName.startsWith("[context=job]")) {
                propName = propName.substring(13);
                addAdaptorPreference(propName, propValue);
            }
            if (propName.startsWith("[context=file]")) {
            	addAdaptorPreference(propName.substring(14), propValue);
            	GATAdaptor.addTransferContextPreferences(propName.substring(14), propValue);
            }
        }

        if (tracing) {
            logger.debug("Starting GAT tracer " + this.getName());
            tracingJob = GATTracer.startTracing(this, limitOfTasks);
            waitForTracingReady();
        }
    }
    private void initContext(String brokerAdaptor, String fileAdaptor) {
        context = new GATContext();
        //String fileAdaptor = System.getProperty(ITConstants.GAT_FILE_ADAPTOR);
        context.addPreference("ResourceBroker.adaptor.name", brokerAdaptor);
        context.addPreference("File.adaptor.name", fileAdaptor + ", srcToLocalToDestCopy, local");
        usingGlobus = brokerAdaptor.equalsIgnoreCase("globus");
        userNeeded = brokerAdaptor.regionMatches(true, 0, "ssh", 0, 3);
    }

	public void addAdaptorPreference(String property, String value) {
		context.addPreference(property, value);
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

    public boolean isTracingReady() {
        return !tracing || GATTracer.isReady(tracingJob);
    }

    public void waitForTracingReady() {
        if (!tracing) {
            return;
        }
        GATTracer.waitForTracing(tracingJob);
    }

    @Override
    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, JobListener listener) {
        return new GATJob(taskId, taskParams, impl, res, listener, context, userNeeded, usingGlobus);
    }
    
    @Override
    public void setInternalURI(URI uri) {
        String scheme = uri.getScheme();
        String user = this.user.isEmpty() ? "" : this.user + "@";
        String host = this.host;
        String filePath = uri.getPath();

        String s = (scheme
                + user
                + host + File.separator
                + filePath);
        org.gridlab.gat.URI gat;
        try {
            gat = new org.gridlab.gat.URI(s);
            uri.setInternalURI(GATAdaptor.ID, gat);
        } catch (URISyntaxException e) {
            logger.error(URI_CREATION_ERR, e);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        try {
            delete(new File(this.sandboxWorkingDir));
        } catch (FileNotFoundException e){
            logger.warn("Could not remove Node working dir\n" + e);
        }
        sl.notifyEnd();
    }
    
    private void delete(File f) throws FileNotFoundException {
        if (f.isDirectory()) {
            for (File c : f.listFiles()){
                delete(c);
            }
        }
        if (!f.delete()){
            throw new FileNotFoundException("Failed to delete file: " + f);
        }
    }

    public void processCopy(Copy c) {
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        Copy c = new GATCopy(srcData, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        Copy c = new GATCopy(ld, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        if (tracing) {
            System.err.println("Tracing system and Cloud do not work together");
        }
    }

    @Override
    public void announceCreation() throws Exception {
        SSHManager.registerWorker(this);
        SSHManager.announceCreation(this);
    }

    @Override
    public void announceDestruction() throws Exception {
        SSHManager.removeKey(this);
        SSHManager.announceDestruction(this);
        SSHManager.removeWorker(this);
    }

    @Override
    public String getCompletePath(ITExecution.ParamType type, String name) {
        switch (type) {
            case FILE_T:
                return workingDir + name;
            case OBJECT_T:
                return workingDir + name;
            default:
                return null;
        }
    }

    @Override
    public void deleteTemporary() {
        //TODO GATWorkerNode hauria d'eliminar " + workingDir + " a " + getName());
    }

    @Override
    public void generatePackage() {
        logger.debug("Generating GAT tracing package");
        GATTracer.generatePackage(this);
    }

    @Override
    public void generateWorkersDebugInfo() {
        // This feature is only for persistent workers (NIO)
        logger.info("Worker debug files not supported on GAT Adaptor");
    }

	public GATContext getContext() {
		return context;
	}
	
	public boolean isUsingGlobus(){
		return usingGlobus;
	}
	
	public boolean isUserNeeded(){
		return userNeeded;
	}

}
