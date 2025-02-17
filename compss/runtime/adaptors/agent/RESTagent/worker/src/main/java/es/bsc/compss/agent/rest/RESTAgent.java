/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.rest;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.agent.Agent;
import es.bsc.compss.agent.AgentException;
import es.bsc.compss.agent.AgentInterface;
import es.bsc.compss.agent.RESTAgentConfig;
import es.bsc.compss.agent.rest.types.ApplicationParameterImpl;
import es.bsc.compss.agent.rest.types.RESTAgentRequestListener;
import es.bsc.compss.agent.rest.types.RESTResult;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.rest.types.messages.EndApplicationNotification;
import es.bsc.compss.agent.rest.types.messages.IncreaseNodeNotification;
import es.bsc.compss.agent.rest.types.messages.LostNodeNotification;
import es.bsc.compss.agent.rest.types.messages.ReduceNodeRequest;
import es.bsc.compss.agent.rest.types.messages.RemoveNodeRequest;
import es.bsc.compss.agent.rest.types.messages.StartApplicationRequest;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.agent.util.RemoteJobsRegistry;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Class providing a REST Interface for the COMPSs Agent.
 */
@Path("/COMPSs")
public class RESTAgent implements AgentInterface<RESTAgentConf> {

    // Error Messages
    private static final String UNSUPPORTED_LANGUAGE_MSG = "Unsupported language.";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.AGENT);

    private int port;
    private Server server = null;


    @Override
    public RESTAgentConf configure(final JSONObject confJSON) throws AgentException {
        RESTAgentConf conf;
        try {
            String portSTR = confJSON.getString("PORT");
            portSTR = EnvironmentLoader.loadFromEnvironment(portSTR);
            int port = Integer.parseInt(portSTR);
            if (port > 0) {
                conf = new RESTAgentConf(this, port);
            } else {
                throw new AgentException("Invalid port number for REST agent's interface.");
            }
        } catch (Exception e) {
            LOGGER.error("Error configuring agent", e);
            throw new AgentException(e);
        }
        return conf;
    }

    @Override
    public synchronized void start(RESTAgentConf args) throws AgentException {
        if (this.server != null) {
            // Server already started. Ignore start;
            return;
        }
        RESTServiceLauncher launcher = null;
        try {
            this.port = args.getPort();
            RESTAgentConfig.localAgentPort = port;
            launcher = new RESTServiceLauncher(port);
            new Thread(launcher).start();
            launcher.waitForBoot();
        } catch (Exception e) {
            throw new AgentException(e);
        }
        if (launcher.getStartError() != null) {
            throw new AgentException(launcher.getStartError());
        } else {
            this.server = launcher.getServer();
        }
    }

    @Override
    public synchronized void stop() {
        if (this.server != null) {
            new Thread() {

                @Override
                public void run() {
                    Thread.currentThread().setName("REST Agent Service Stopper");
                    try {
                        Thread.sleep(500);
                        RESTAgent.this.server.stop();
                        LOGGER.debug("REST Service Agent Interface stopped");
                    } catch (Exception ex) {
                        ErrorManager.warn("Could not stop the REST server for the Agent at port " + port, ex);
                        if (!server.isStopped()) {
                            server.destroy();
                        }
                    } finally {
                        server = null;
                    }
                }
            }.start();

        }
    }

    /**
     * Stops the agent.
     *
     * @return REST response Indicates the correct termination of the Agent.
     */
    @DELETE
    public Response powerOff() {
        Agent.stop();
        LOGGER.info("Agent was shutdown");
        return Response.ok().build();
    }

    @GET
    @Path("test/")
    public Response test() {
        System.out.println("test invoked");
        return Response.ok().build();
    }

    /**
     * Returns the currently available resources.
     *
     * @return REST response with the current resource configuration.
     */
    @GET
    @Path("resources/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getResources() {
        System.out.println("Requested current resource configuraction");
        JSONObject root = new JSONObject();
        root.put("time", System.currentTimeMillis());
        JSONArray resources = new JSONArray();
        root.put("resources", resources);
        for (Worker<?> worker : ResourceManager.getAllWorkers()) {
            JSONObject workerJSON = new JSONObject();
            resources.put(workerJSON);
            workerJSON.put("name", worker.getName());

            JSONObject descriptionJSON = new JSONObject();
            workerJSON.put("description", descriptionJSON);

            ResourceDescription description = worker.getDescription();
            if (description instanceof MethodResourceDescription) {
                MethodResourceDescription mrd = (MethodResourceDescription) description;
                JSONArray processors = new JSONArray();
                descriptionJSON.put("processors", processors);
                for (Processor processor : mrd.getProcessors()) {
                    JSONObject processorJSON = new JSONObject();
                    processors.put(processorJSON);
                    processorJSON.put("name", processor.getName());
                    processorJSON.put("architecture", processor.getArchitecture());
                    processorJSON.put("units", processor.getComputingUnits());
                }
                descriptionJSON.put("memory_size", mrd.getMemorySize());
                descriptionJSON.put("memory_type", mrd.getMemoryType());

                descriptionJSON.put("storage_size", mrd.getStorageSize());
                descriptionJSON.put("storage_type", mrd.getStorageType());
                descriptionJSON.put("storage_bandwidth", mrd.getStorageBW());
            }

            workerJSON.put("adaptor", worker.getNode().getClass().getCanonicalName());
        }

        return Response.ok(root.toString(), MediaType.TEXT_PLAIN).build();
    }

    /**
     * Prints through the agent's standard output stream the resources currently available.
     *
     * @return REST response confirming the execution of the print command
     */
    @GET
    @Path("printResources/")
    public Response printResources() {
        System.out.println(ResourceManager.getCurrentState(""));
        return Response.ok().build();
    }

    /**
     ** Adds new available resources to the runtime system.
     *
     * @param nodeRequest requested resource description.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("addResources/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response addResource(IncreaseNodeNotification nodeRequest) {
        Resource<?, ?> r = nodeRequest.getResource();
        // Updating processors
        MethodResourceDescription description = r.getDescription();
        List<Processor> procs = description.getProcessors();
        description.setProcessors(procs);

        try {
            Agent.addResources(r);
        } catch (AgentException ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(ex.getMessage()).build();
        }
        return Response.ok().build();
    }

    /**
     * Removes some resources from the pool of available resources.
     *
     * @param request request describing the resources to release.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("removeResources/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response removeResources(ReduceNodeRequest request) {
        String name = request.getWorkerName();
        MethodResourceDescription mrd = request.getResources();
        List<Processor> procs = mrd.getProcessors();
        mrd.setProcessors(procs);
        try {
            Agent.removeResources(name, mrd);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

        }
        return Response.ok().build();
    }

    /**
     * Removes all the resources from a node from the resource pool.
     *
     * @param request request describing the node to release.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("removeNode/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response removeResource(RemoveNodeRequest request) {
        String name = request.getWorkerName();
        try {
            Agent.removeNode(name);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

        }
        return Response.ok().build();
    }

    /**
     * Removes all the resources from the node and assumes that all the tasks running there fail.
     *
     * @param notification request describing the lost node.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("lostNode/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response lostResource(LostNodeNotification notification) {
        String name = notification.getWorkerName();
        try {
            Agent.lostNode(name);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

        }
        return Response.ok().build();
    }

    /**
     * Request to run a method as a task.
     *
     * @param request description of the method to execute.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("startApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startApplication(StartApplicationRequest request) {
        System.out.println("Received REST call to run a " + request.getLang() + " method");
        Response response;
        try {
            // String ceiClass = request.getCeiClass();
            response = runTask(request);
        } catch (Exception e) {
            response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return response;
    }

    private Response runTask(StartApplicationRequest request) {
        Lang lang;
        try {
            lang = Lang.valueOf(request.getLang().toUpperCase());
        } catch (java.lang.IllegalArgumentException iae) {
            return Response.status(Response.Status.NOT_ACCEPTABLE).entity(UNSUPPORTED_LANGUAGE_MSG).build();
        }
        String className;
        className = request.getClassName();
        String methodName;
        methodName = request.getMethodName();
        String ceiClass;
        ceiClass = request.getCeiClass();
        ApplicationParameterImpl[] arguments = request.getParams();
        ApplicationParameterImpl target = request.getTarget();
        ApplicationParameterImpl[] results;
        boolean hasResult = request.isHasResult();
        if (hasResult) {
            results = new ApplicationParameterImpl[1];
            results[1] = new ApplicationParameterImpl(null, Direction.IN, DataType.OBJECT_T, StdIOStream.UNSPECIFIED,
                "", "result", "", 1.0, false);
        } else {
            results = new ApplicationParameterImpl[0];
        }
        long appId;
        RESTAgentRequestListener requestListener = request.getRequestListener();
        RESTAppMonitor monitor = new RESTAppMonitor(arguments, target, results, this, requestListener);

        // COMPUTE SIGNATURES
        StringBuilder typesSB = new StringBuilder();

        LOGGER.debug("Handles parameters:");
        for (ApplicationParameter param : arguments) {
            LOGGER.debug("\t Parameter:" + param.getParamName());
            if (typesSB.length() > 0) {
                typesSB.append(",");
            }
            if (param.getType() != DataType.PSCO_T) {
                typesSB.append(param.getType().toString());
            } else {
                typesSB.append("OBJECT_T");
            }
        }

        String paramsTypes = typesSB.toString();

        String ceSignature = methodName + "(" + paramsTypes + ")";
        String implSignature = methodName + "(" + paramsTypes + ")" + className;
        String[] typeArgs = new String[] { className,
            methodName };

        MethodResourceDescription requirements = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS;
        CoreElementDefinition ced = new CoreElementDefinition();
        ced.setCeSignature(ceSignature);

        ImplementationDescription<?, ?> implDef = ImplementationDescription.defineImplementation("METHOD",
            implSignature, false, requirements, request.getProlog(), request.getEpilog(), null, typeArgs);
        ced.addImplementation(implDef);
        try {
            appId = Agent.runTask(lang, ced, ceiClass, arguments, target, results, monitor, OnFailure.FAIL);
            LOGGER.info("External job - is app " + appId);
        } catch (AgentException e) {
            LOGGER.error("ERROR IN runTask : ", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.ok(appId, MediaType.TEXT_PLAIN).build();
    }

    /**
     * Notification that a task submitted to another REST agent has finished.
     *
     * @param notification Result of the operation.
     * @return REST response containing the service reply to the request
     */
    @PUT
    @Path("endApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response endApplication(EndApplicationNotification notification) {
        try {
            String jobId = notification.getJobId();
            JobEndStatus endStatus = notification.getEndStatus();
            RESTResult[] resultsLocations = notification.getResults();
            TaskProfile profile = notification.getProfile();
            RemoteJobsRegistry.notifyJobEnd(jobId, endStatus, resultsLocations, profile);
            return Response.ok().build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e).build();
        }
    }

    /**
     * Main method of the application starting a REST service.
     *
     * @param args Arguments to configuring the REST service. Position 0: port.
     * @throws Exception Error starting the REST Agent
     */
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        RESTAgent ra = new RESTAgent();
        RESTAgentConf config = new RESTAgentConf(ra, port);
        Agent.startInterface(config);
        Agent.start();
    }

}
