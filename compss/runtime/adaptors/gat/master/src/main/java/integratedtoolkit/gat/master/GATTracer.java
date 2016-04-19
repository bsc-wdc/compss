package integratedtoolkit.gat.master;

import integratedtoolkit.ITConstants;
import integratedtoolkit.util.Tracer;
import integratedtoolkit.util.ErrorManager;

import java.io.File;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;


public class GATTracer extends Tracer {

    /*private final static GATContext context;
   
    static {
        context = new GATContext();
        String brokerAdaptor = System.getProperty(ITConstants.GAT_BROKER_ADAPTOR);
        logger.debug("Using GAT Adaptor "+ brokerAdaptor);
        context.addPreference("ResourceBroker.adaptor.name", brokerAdaptor);
    }

    public static void addPreference(String name, String value) {
        context.addPreference(name, value);
    }*/

    public static Job startTracing(GATWorkerNode worker, int numTasks) {
        if (debug){
            logger.debug("Starting trace for woker " + worker.getHost());
        }
        if (numTasks <= 0) {
            if (debug) {
                logger.debug("Resource " + worker.getName() + " has 0 slots, it won't appear in the trace");
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
        String uriString = "any://" + user + worker.getHost();
        sd.addAttribute("uri", uriString);
        sd.setExecutable(worker.getInstallDir() + File.separator + Tracer.TRACE_SCRIPT);
        sd.setArguments(new String[]{"init", worker.getWorkingDir(), String.valueOf(hostId), String.valueOf(numTasks)});

        if (debug) {
            try {
                org.gridlab.gat.io.File outFile = GAT.createFile(worker.getContext(), "any:///" + System.getProperty(ITConstants.IT_APP_LOG_DIR) + traceOutRelativePath);
                sd.setStdout(outFile);
                org.gridlab.gat.io.File errFile = GAT.createFile(worker.getContext(), "any:///" + System.getProperty(ITConstants.IT_APP_LOG_DIR) + traceErrRelativePath);
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
            logger.debug("Starting tracer init job for worker "+uriString+" submited.");
            job = broker.submitJob(new JobDescription(sd));
        } catch (Exception e) {
            ErrorManager.warn("Error initializing tracing system in node " + worker.getHost(), e);
            return null;
        }
        return job;
    }

    public static boolean isReady(Job job) {
        if (job.getState() == Job.JobState.STOPPED) {
            String uri = (String) ((JobDescription) job.getJobDescription()).getSoftwareDescription().getAttributes().get("uri");
            if (debug) {
                logger.debug("Initialized tracing system in " + uri);
            }
            return true;
        } else if (job.getState() == Job.JobState.SUBMISSION_ERROR) {
            logger.error("Error initializing tracing system, host " + job);
            return true;
        }
        return false;
    }

    public static void waitForTracing(Job job) {
        Long timeout = System.currentTimeMillis() + 60000l;
        while (System.currentTimeMillis() < timeout) {
            if (isReady(job)) {
            	if (debug) logger.debug("Tracing ready");
            	return;
            }

            try {
                Thread.sleep(50);
            } catch (Exception e) {
            }
        }
        logger.error("Error initializing tracing system, " + job + " job still pending.");
    }

    public static synchronized void emitEvent(long eventID, int eventType){
        logger.error("Emit event method is not available for GAT tracing");
    }
    
    public static void generatePackage(GATWorkerNode node) {
        LinkedList<URI> traceScripts = new LinkedList<URI>();
        LinkedList<String> traceParams = new LinkedList<String>();
        String host = node.getHost();
        String installDir = node.getInstallDir();
        String workingDir = node.getWorkingDir();

        String user = node.getUser();
        if (user == null) {
            user = "";
        } else {
            user += "@";
        }

        try {
            traceScripts.add(new URI("any://" + user + host + File.separator + installDir + File.separator + TRACE_SCRIPT));
        } catch (URISyntaxException e) {
            logger.error("Error deleting tracing host", e);
        }
        String pars = "package " + workingDir + " " + host;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        new CleanerExecutor(node).executeScript(traceScripts, traceParams);
                
    }
}
