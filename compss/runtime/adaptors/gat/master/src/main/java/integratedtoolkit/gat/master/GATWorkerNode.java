package integratedtoolkit.gat.master;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.gat.master.configuration.GATConfiguration;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.DataOperation.EventListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.util.SSHManager;

import org.gridlab.gat.GATContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;


public class GATWorkerNode extends COMPSsWorker {

    private GATConfiguration config;
    private org.gridlab.gat.resources.Job tracingJob;

    @Override
    public String getName() {
        return this.config.getHost();
    }

    public GATWorkerNode(String name, GATConfiguration config) {
        super(name, config);
        this.config = config;

    }

    @Override
    public void start() throws Exception {
        if (tracing) {
            logger.debug("Starting GAT tracer " + this.getName());
            tracingJob = GATTracer.startTracing(this);
            waitForTracingReady();
        }
    }

    public void addAdaptorPreference(String property, String value) {
        this.config.addContextPreference(property, value);
    }

    @Override
    public String getUser() {
        return this.config.getUser();
    }

    public String getHost() {
        return this.config.getHost();
    }

    public String getInstallDir() {
        return this.config.getInstallDir();
    }

    public String getWorkingDir() {
        return this.config.getWorkingDir();
    }

    public String getAppDir() {
        String appDir = this.config.getAppDir();
        appDir = (appDir == null || appDir.isEmpty()) ? "null" : appDir;

        return appDir;
    }

    public String getLibPath() {
        String libPath = this.config.getLibraryPath();
        libPath = (libPath == null || libPath.isEmpty()) ? "null" : libPath;

        return libPath;
    }
    
	@Override
	public String getClasspath() {
		return this.config.getClasspath();
	}

	@Override
	public String getPythonpath() {
		return this.config.getPythonpath();
	}

    public int getTotalComputingUnits() {
        return this.config.getTotalComputingUnits();
    }

    private void waitForTracingReady() {
        if (!tracing) {
            return;
        }
        GATTracer.waitForTracing(tracingJob);
    }

    @Override
    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, JobListener listener) {
        return new GATJob(taskId, taskParams, impl, res, listener, config.getContext(), config.isUserNeeded(), config.isUsingGlobus());
    }

    @Override
    public void setInternalURI(URI uri) {
        String scheme = uri.getScheme();
        String user = this.config.getUser().isEmpty() ? "" : this.config.getUser() + "@";
        String host = this.config.getHost();
        String filePath = uri.getPath();

        String s = (scheme + user + host + File.separator + filePath);
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
            File workingDirRoot = new File(this.config.getWorkingDir());
            for (File c : workingDirRoot.listFiles()){
                delete(c);
            }
        } catch (FileNotFoundException e) {
            logger.warn("Could not remove clean node working dir\n" + e);
        }
        sl.notifyEnd();
    }

    private void delete(File f) throws FileNotFoundException {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                delete(c);
            }
        }
        if (!f.delete()) {
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
    public String getCompletePath(DataType type, String name) {
        switch (type) {
            case FILE_T:
                return this.config.getWorkingDir() + name;
            case OBJECT_T:
                return this.config.getWorkingDir() + name;
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
        return this.config.getContext();
    }

    public boolean isUsingGlobus() {
        return this.config.isUsingGlobus();
    }

    public boolean isUserNeeded() {
        return this.config.isUserNeeded();
    }

}
