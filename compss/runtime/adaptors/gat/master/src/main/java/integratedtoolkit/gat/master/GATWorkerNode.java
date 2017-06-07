package integratedtoolkit.gat.master;

import integratedtoolkit.exceptions.AnnounceException;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.gat.master.configuration.GATConfiguration;
import integratedtoolkit.gat.master.utils.GATScriptExecutor;
import integratedtoolkit.gat.master.utils.SSHManager;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.data.operation.copy.Copy;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.ExecutorShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.types.annotations.parameter.DataType;

import org.gridlab.gat.GATContext;
import org.gridlab.gat.URI;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a GAT Worker node for the Runtime
 *
 */
public class GATWorkerNode extends COMPSsWorker {

    private static final String GAT_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "adaptors" + File.separator + "gat" + File.separator;
    private static final String CLEANER_SCRIPT_NAME = "clean.sh";
    private static final String INIT_SCRIPT_NAME = "init.sh";

    private GATConfiguration config;
    private org.gridlab.gat.resources.Job tracingJob;


    /**
     * New GAT Worker Node with name @name and configuration @config
     * 
     * @param name
     * @param config
     */
    public GATWorkerNode(String name, GATConfiguration config) {
        super(name, config);
        this.config = config;
    }

    @Override
    public String getName() {
        return this.config.getHost();
    }

    @Override
    public void start() throws InitNodeException {
        initWorkingDir();
        if (GATTracer.isActivated()) {
            LOGGER.debug("Starting GAT tracer " + this.getName());
            tracingJob = GATTracer.startTracing(this);
            waitForTracingReady();
        }
    }

    private void initWorkingDir() throws InitNodeException {
        LinkedList<URI> traceScripts = new LinkedList<>();
        LinkedList<String> traceParams = new LinkedList<>();
        String host = getHost();
        String installDir = getInstallDir();
        String workingDir = getWorkingDir();

        String user = getUser();
        if (user == null || user.isEmpty()) {
            user = "";
        } else {
            user += "@";
        }

        try {
            String initScriptPath = Protocol.ANY_URI.getSchema() + user + host + File.separator + installDir + GAT_SCRIPT_PATH
                    + INIT_SCRIPT_NAME;
            traceScripts.add(new URI(initScriptPath));
        } catch (URISyntaxException e) {
            new InitNodeException("Error addind initScript");
        }

        String pars = workingDir;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        LOGGER.debug("Initializing working dir " + workingDir + "  in host " + getName());
        boolean result = new GATScriptExecutor(this).executeScript(traceScripts, traceParams, "init_" + host);
        if (!result) {
            throw new InitNodeException("Error executing init script for initializing working dir " + workingDir + " in host " + getName());
        }
    }

    private void waitForTracingReady() {
        if (!GATTracer.isActivated()) {
            return;
        }
        GATTracer.waitForTracing(tracingJob);
    }

    @Override
    public String getUser() {
        return this.config.getUser();
    }

    @Override
    public String getClasspath() {
        return this.config.getClasspath();
    }

    @Override
    public String getPythonpath() {
        return this.config.getPythonpath();
    }

    /**
     * Returns the hostname
     * 
     * @return
     */
    public String getHost() {
        return this.config.getHost();
    }

    /**
     * Returns the installation dir
     * 
     * @return
     */
    public String getInstallDir() {
        return this.config.getInstallDir();
    }

    /**
     * Returns the working dir
     * 
     * @return
     */
    public String getWorkingDir() {
        return this.config.getSandboxWorkingDir();
    }

    /**
     * Returns the application dir
     * 
     * @return
     */
    public String getAppDir() {
        String appDir = this.config.getAppDir();
        appDir = (appDir == null || appDir.isEmpty()) ? "null" : appDir;

        return appDir;
    }

    /**
     * Returns the library path
     * 
     * @return
     */
    public String getLibPath() {
        String libPath = this.config.getLibraryPath();
        libPath = (libPath == null || libPath.isEmpty()) ? "null" : libPath;

        return libPath;
    }

    /**
     * Returns the total number of computing units
     * 
     * @return
     */
    public int getTotalComputingUnits() {
        return this.config.getTotalComputingUnits();
    }

    /**
     * Returns the GAT context
     * 
     * @return
     */
    public GATContext getContext() {
        return this.config.getContext();
    }

    /**
     * Returns if globus is enabled or not
     * 
     * @return
     */
    public boolean isUsingGlobus() {
        return this.config.isUsingGlobus();
    }

    /**
     * Returns if user is needed to login the worker or not
     * 
     * @return
     */
    public boolean isUserNeeded() {
        return this.config.isUserNeeded();
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, List<String> slaveWorkersNodeNames,
            JobListener listener) {

        return new GATJob(taskId, taskParams, impl, res, listener, config.getContext(), config.isUserNeeded(), config.isUsingGlobus(),
                slaveWorkersNodeNames);
    }

    @Override
    public void setInternalURI(MultiURI uri) {
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
            LOGGER.error(URI_CREATION_ERR, e);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        try {
            String workingDir = this.config.getWorkingDir();
            if (workingDir != null && !workingDir.isEmpty()) {
                File workingDirRoot = new File(workingDir);
                File[] filesInFolder = workingDirRoot.listFiles();
                if (filesInFolder != null) {
                    for (File c : filesInFolder) {
                        delete(c);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.warn("Could not remove clean node working dir\n" + e);
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

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        Copy c = new GATCopy(srcData, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        Copy c = new GATCopy(ld, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        if (GATTracer.isActivated()) {
            LOGGER.error("Tracing system and Cloud do not work together");
        }
    }

    @Override
    public void announceCreation() throws AnnounceException {
        try {
            SSHManager.registerWorker(this);
            SSHManager.announceCreation(this);
        } catch (IOException ioe) {
            throw new AnnounceException(ioe);
        }
    }

    @Override
    public void announceDestruction() throws AnnounceException {
        try {
            SSHManager.removeKey(this);
            SSHManager.announceDestruction(this);
            SSHManager.removeWorker(this);
        } catch (IOException ioe) {
            throw new AnnounceException(ioe);
        }
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case EXTERNAL_OBJECT_T:
                path = Protocol.FILE_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            default:
                return null;
        }

        // Convert path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        LinkedList<URI> traceScripts = new LinkedList<URI>();
        LinkedList<String> traceParams = new LinkedList<String>();
        String host = getHost();
        String installDir = getInstallDir();
        String workingDir = getWorkingDir();

        String user = getUser();
        if (user == null) {
            user = "";
        } else {
            user += "@";
        }

        try {
            traceScripts.add(new URI(
                    Protocol.ANY_URI.getSchema() + user + host + File.separator + installDir + GAT_SCRIPT_PATH + CLEANER_SCRIPT_NAME));
        } catch (URISyntaxException e) {
            LOGGER.error("Error deleting working dir " + workingDir + " in host " + getName(), e);
            return;
        }
        String pars = workingDir;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        LOGGER.debug("Deleting working dir " + workingDir + "  in host " + getName());
        boolean result = new GATScriptExecutor(this).executeScript(traceScripts, traceParams, "clean_" + host);
        if (!result) {
            LOGGER.error("Error executing clean script for deleting working dir " + workingDir + " in host " + getName());
        }
    }

    @Override
    public boolean generatePackage() {
        LOGGER.debug("Generating GAT tracing package");
        GATTracer.generatePackage(this);
        return true;
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        // This feature is only for persistent workers (NIO)
        LOGGER.info("Worker debug files not supported on GAT Adaptor");
        return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // GAT has no execution managers, release listener immediately
        sl.notifyEnd();
    }
}
