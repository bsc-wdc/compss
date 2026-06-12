/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.gat.master;

import es.bsc.compss.gat.master.utils.GATScriptExecutor;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.wdc.tracing.Tracer;
import es.bsc.wdc.tracing.util.TraceScript;
import java.io.File;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;

public class GATTracer extends Tracer {

    private static final String tracerOutDir = File.separator + Tracer.getTracerOutputDir();
    private static final String TRACER_OUT_FILENAME = "tracer.out";
    private static final String tracerOutPath = tracerOutDir + File.separator + TRACER_OUT_FILENAME;
    private static final String TRACER_ERR_FILENAME = "tracer.err";
    private static final String tracerErrPath = tracerOutDir + File.separator + TRACER_ERR_FILENAME;


    /**
     * Returns a job to start the tracing in the worker node.
     * 
     * @param worker Worker where to start the tracing.
     * @return Tracing job.
     */
    public static Job startTracing(GATWorkerNode worker) {
        if (debug) {
            logger.debug("Starting trace for woker " + worker.getHost());
        }

        int numTasks = worker.getTotalComputingUnits();
        if (numTasks <= 0) {
            if (debug) {
                logger.debug("Resource " + worker.getName() + " has 0 slots, it won't appear in the trace");
            }
            return null;
        }

        final int hostId = Tracer.registerHost(worker.getName(), numTasks);

        String user;
        if (worker.getUser() == null || worker.getUser().isEmpty()) {
            user = "";
        } else {
            user = worker.getUser() + "@";
        }

        SoftwareDescription sd = new SoftwareDescription();
        String uriString = ProtocolType.ANY_URI.getSchema() + user + worker.getHost();
        sd.addAttribute("uri", uriString);
        sd.setExecutable(worker.getInstallDir() + TraceScript.RELATIVE_PATH);
        sd.setArguments(new String[] { "init",
            worker.getWorkingDir(),
            String.valueOf(hostId),
            String.valueOf(numTasks) });

        if (debug) {
            try {
                String outFilePath = ProtocolType.ANY_URI.getSchema() + tracerOutPath;
                org.gridlab.gat.io.File outFile = GAT.createFile(worker.getContext(), outFilePath);
                sd.setStdout(outFile);

                String errFilePath = ProtocolType.ANY_URI.getSchema() + tracerErrPath;
                org.gridlab.gat.io.File errFile = GAT.createFile(worker.getContext(), errFilePath);
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
            logger.debug("Starting tracer init job for worker " + uriString + " submited.");
            job = broker.submitJob(new JobDescription(sd));
        } catch (Exception e) {
            ErrorManager.warn("Error initializing tracing system in node " + worker.getHost(), e);
            return null;
        }
        return job;
    }

    /**
     * Waits for the tracing job to finish.
     * 
     * @param job Tracing job to wait for.
     */
    public static void waitForTracing(Job job) {
        Long timeout = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < timeout) {
            if (isReady(job)) {
                if (debug) {
                    logger.debug("Tracing ready");
                }
                return;
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.error("Error initializing tracing system, " + job + " job still pending.");
    }

    private static boolean isReady(Job job) {
        if (job != null) {
            if (job.getState() == Job.JobState.STOPPED) {
                String uri = (String) ((JobDescription) job.getJobDescription()).getSoftwareDescription()
                    .getAttributes().get("uri");
                if (debug) {
                    logger.debug("Initialized tracing system in " + uri);
                }
                return true;
            } else if (job.getState() == Job.JobState.SUBMISSION_ERROR) {
                logger.error("Error initializing tracing system, host " + job);
                return true;
            }
        }
        return false;
    }

    /**
     * Emits a tracing event.
     * 
     * @param eventID Event Id.
     * @param eventType Event Type.
     */
    public static void emitEvent(long eventID, int eventType) {
        logger.error("Emit event method based on Extrae JAVA API is not available for GAT tracing on workers."
            + " (Use the Tracer class when instrumenting master.");
    }

    /**
     * Generates the tracing package in a given worker node.
     * 
     * @param node Worker node where to generate the tracing package.
     * @return {@literal true} if the tracing package has been generated, {@literal false} otherwise.
     */
    public static Set<String> generatePackage(GATWorkerNode node) {
        final LinkedList<URI> traceScripts = new LinkedList<>();
        final LinkedList<String> traceParams = new LinkedList<>();
        final String host = node.getHost();
        final String installDir = node.getInstallDir();
        final String workingDir = node.getWorkingDir();

        String user = node.getUser();
        if (user == null || user.isEmpty()) {
            user = "";
        } else {
            user += "@";
        }

        try {
            traceScripts.add(new URI(ProtocolType.ANY_URI.getSchema() + user + host + File.separator + installDir
                + TraceScript.RELATIVE_PATH));
        } catch (URISyntaxException e) {
            logger.error("Error deleting tracing host", e);
            return null;
        }
        String mode = "package";
        String pars = mode + " " + workingDir + " " + host;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        boolean scriptResult =
            new GATScriptExecutor(node).executeScript(traceScripts, traceParams, "trace_packaging_" + host);
        if (!scriptResult) {
            logger.error("Tracing package generation scripts failed to execute properly");
            return null;
        }
        Set<String> tracingFiles = new HashSet<String>();
        tracingFiles.add(workingDir);
        return tracingFiles;

    }

}
