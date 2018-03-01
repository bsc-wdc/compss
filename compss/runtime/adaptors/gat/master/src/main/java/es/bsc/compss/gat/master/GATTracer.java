package es.bsc.compss.gat.master;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.gat.master.utils.GATScriptExecutor;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;


public class GATTracer extends Tracer {

    public static Job startTracing(GATWorkerNode worker) {
        if (DEBUG) {
            LOGGER.debug("Starting trace for woker " + worker.getHost());
        }

        tracingLevel = 1;

        int numTasks = worker.getTotalComputingUnits();
        if (numTasks <= 0) {
            if (DEBUG) {
                LOGGER.debug("Resource " + worker.getName() + " has 0 slots, it won't appear in the trace");
            }
            return null;
        }

        int hostId = Tracer.registerHost(worker.getName(), numTasks);

        String user;
        if (worker.getUser() == null || worker.getUser().isEmpty()) {
            user = "";
        } else {
            user = worker.getUser() + "@";
        }

        SoftwareDescription sd = new SoftwareDescription();
        String uriString = Protocol.ANY_URI.getSchema() + user + worker.getHost();
        sd.addAttribute("uri", uriString);
        sd.setExecutable(worker.getInstallDir() + Tracer.TRACE_SCRIPT_PATH);
        sd.setArguments(new String[] { "init", worker.getWorkingDir(), String.valueOf(hostId), String.valueOf(numTasks) });

        if (DEBUG) {
            try {
                org.gridlab.gat.io.File outFile = GAT.createFile(worker.getContext(),
                        Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(COMPSsConstants.APP_LOG_DIR) + traceOutRelativePath);
                sd.setStdout(outFile);
                org.gridlab.gat.io.File errFile = GAT.createFile(worker.getContext(),
                        Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(COMPSsConstants.APP_LOG_DIR) + traceErrRelativePath);
                sd.setStderr(errFile);
            } catch (Exception e) {
                ErrorManager.warn("Error initializing tracing system in node " + worker.getHost(), e);
                return null;
            }
        }

        sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, File.separator + "tmp" + File.separator);
        sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
        sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

        Job job = null;
        try {
            URI brokerURI = new URI(uriString);
            ResourceBroker broker = GAT.createResourceBroker(worker.getContext(), brokerURI);
            LOGGER.debug("Starting tracer init job for worker " + uriString + " submited.");
            job = broker.submitJob(new JobDescription(sd));
        } catch (Exception e) {
            ErrorManager.warn("Error initializing tracing system in node " + worker.getHost(), e);
            return null;
        }
        return job;
    }

    public static void waitForTracing(Job job) {
        Long timeout = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < timeout) {
            if (isReady(job)) {
                if (DEBUG)
                    LOGGER.debug("Tracing ready");
                return;
            }

            try {
                Thread.sleep(50);
            } catch (Exception e) {
            }
        }
        LOGGER.error("Error initializing tracing system, " + job + " job still pending.");
    }

    private static boolean isReady(Job job) {
        if (job != null) {
            if (job.getState() == Job.JobState.STOPPED) {
                String uri = (String) ((JobDescription) job.getJobDescription()).getSoftwareDescription().getAttributes().get("uri");
                if (DEBUG) {
                    LOGGER.debug("Initialized tracing system in " + uri);
                }
                return true;
            } else if (job.getState() == Job.JobState.SUBMISSION_ERROR) {
                LOGGER.error("Error initializing tracing system, host " + job);
                return true;
            }
        }
        return false;
    }

    public static void emitEvent(long eventID, int eventType) {
        LOGGER.error("Emit event method based on Extrae JAVA API is not available for GAT tracing on workers. (Use Tracer class when instrumenting master.");
    }

    public static boolean generatePackage(GATWorkerNode node) {
        LinkedList<URI> traceScripts = new LinkedList<>();
        LinkedList<String> traceParams = new LinkedList<>();
        String host = node.getHost();
        String installDir = node.getInstallDir();
        String workingDir = node.getWorkingDir();

        String user = node.getUser();
        if (user == null || user.isEmpty()) {
            user = "";
        } else {
            user += "@";
        }

        try {
            traceScripts.add(new URI(Protocol.ANY_URI.getSchema() + user + host + File.separator + installDir + TRACE_SCRIPT_PATH));
        } catch (URISyntaxException e) {
            LOGGER.error("Error deleting tracing host", e);
            return false;
        }
        String pars = "package " + workingDir + " " + host;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        return new GATScriptExecutor(node).executeScript(traceScripts, traceParams, "trace_packaging_"+host);
        
    }

}
