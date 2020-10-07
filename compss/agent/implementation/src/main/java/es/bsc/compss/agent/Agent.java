/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.CoreElementDefinition;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import storage.StorageException;
import storage.StorageItf;


public class Agent {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.AGENT);

    private static final String AGENT_NAME;

    private static final COMPSsRuntimeImpl RUNTIME;

    private static final List<AgentInterface<?>> INTERFACES;

    private static final int PARAM_LENGTH = COMPSsRuntimeImpl.NUM_FIELDS_PER_PARAM;

    static {
        AGENT_NAME = COMPSsNode.getMasterName();
        LOGGER.info("Initializing agent with name: " + AGENT_NAME);

        String dcConfigPath = System.getProperty(AgentConstants.DATACLAY_CONFIG_PATH);
        LOGGER.debug("DataClay configuration: " + dcConfigPath);
        if (dcConfigPath != null) {
            try {
                StorageItf.init(dcConfigPath);
            } catch (StorageException se) {
                se.printStackTrace(System.err);
                System.err.println("Continuing...");
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        StorageItf.finish();
                    } catch (StorageException se) {
                        se.printStackTrace(System.err);
                        System.err.println("Continuing...");
                    }
                }
            });
        }

        RUNTIME = new COMPSsRuntimeImpl();
        RUNTIME.setObjectRegistry(new ObjectRegistry(RUNTIME));
        RUNTIME.setStreamRegistry(new StreamRegistry(RUNTIME));

        INTERFACES = new LinkedList<>();
    }


    /**
     * Start the runtime within the Agent and sets it up to allow the execution of COMPSs methods.
     */
    public static void start() {

        RUNTIME.startIT();
    }

    /**
     * Requests the execution of a method as a task.
     *
     * @param lang programming language of the method
     * @param ced Definition of the Core Element to execute
     * @param ceiClass Core Element interface to detect nested tasks in the code. If null, no nested parallelism will be
     *            detected
     * @param arguments parameter description of the task's arguments
     * @param target paramter description of the task's callee
     * @param results paramter description of the task's results
     * @param monitor monitor to notify changes on the method execution
     * @param onFailure behaviour in case of task execution failure
     * @return Identifier of the application associated to the task
     * @throws AgentException could not retrieve the value of some parameter
     */
    public static long runTask(Lang lang, CoreElementDefinition ced, String ceiClass, ApplicationParameter[] arguments,
        ApplicationParameter target, ApplicationParameter[] results, AppMonitor monitor, OnFailure onFailure)
        throws AgentException {
        LOGGER.debug("New request to run as a " + lang + " task " + ced.getCeSignature());
        LOGGER.debug("Parallelizing application according to " + ceiClass);
        LOGGER.debug("Parameters: ");
        for (ApplicationParameter param : arguments) {
            LOGGER.debug("\t* " + param);
        }

        Long appId = RUNTIME.registerApplication(ceiClass);
        monitor.setAppId(appId);

        try {
            // PREPARING PARAMETERS
            StringBuilder typesSB = new StringBuilder();

            int paramsCount = arguments.length;
            if (target != null) {
                paramsCount++;
            }
            paramsCount += results.length;

            Object[] params = new Object[PARAM_LENGTH * paramsCount];
            int position = 0;
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
                addParameterToTaskArguments(appId, param, position, params);
                position += PARAM_LENGTH;
            }

            if (target != null) {
                LOGGER.debug("\t Target:" + target.getParamName());
                addParameterToTaskArguments(appId, target, position, params);
                position += PARAM_LENGTH;
            }

            for (ApplicationParameter param : results) {
                params[position] = new Object();
                params[position + 1] = param.getType();
                params[position + 2] = param.getDirection();
                params[position + 3] = param.getStdIOStream();
                params[position + 4] = param.getPrefix();
                params[position + 5] = param.getParamName();
                params[position + 6] = param.getContentType();
                params[position + 7] = Double.toString(param.getWeight());
                params[position + 8] = new Boolean(param.isKeepRename());
                position += PARAM_LENGTH;
            }

            RUNTIME.registerCoreElement(ced);
            int numNodes = 1;
            RUNTIME.executeTask(appId, // APP ID
                monitor, // Corresponding task monitor
                lang, true, null, null, ced.getCeSignature(), // Method to call
                onFailure, // On failure behavior
                0, // Time out of the task
                false, numNodes, false, false, // Scheduling information
                target != null, null, paramsCount, // Parameter information
                params // Parameter values
            );

        } catch (Exception e) {
            LOGGER.error("Error submitting task", e);
            throw new AgentException(e);
        }
        return appId;
    }

    private static void addParameterToTaskArguments(Long appId, ApplicationParameter param, int position,
        Object[] arguments) throws AgentException, Exception {

        RemoteDataInformation remote = param.getRemoteData();
        if (param.getRemoteData() == null) {
            LOGGER.debug("\t\tUsing value passed in as parameter");
            arguments[position] = param.getValueContent();
        } else {
            Object stub;
            if (param.getType() == DataType.FILE_T) {
                stub = param.getValueContent();
            } else {
                stub = "app_" + appId + "_param" + position;
            }
            arguments[position] = stub;
            addRemoteData(remote);
            RUNTIME.registerData(appId, param.getType(), stub, remote.getRenaming());
        }
        arguments[position + 1] = param.getType();
        arguments[position + 2] = param.getDirection();
        arguments[position + 3] = param.getStdIOStream();
        arguments[position + 4] = param.getPrefix();
        arguments[position + 5] = param.getParamName();
        arguments[position + 6] = param.getContentType();
        arguments[position + 7] = Double.toString(param.getWeight());
        arguments[position + 8] = new Boolean(param.isKeepRename());

    }

    private static void addRemoteData(RemoteDataInformation remote) throws AgentException {
        int addedSources = 0;
        LogicalData ld = Comm.getData(remote.getRenaming());
        LogicalData otherNamedLocalData = null;

        LinkedList<DataLocation> locations = new LinkedList<>();
        for (RemoteDataLocation loc : remote.getSources()) {
            try {
                String path = loc.getPath();
                SimpleURI uri = new SimpleURI(path);
                Resource<?, ?> r = loc.getResource();
                String workerName = r.getName();
                Worker<? extends WorkerResourceDescription> host = ResourceManager.getWorker(workerName);
                if (host == null) {
                    MethodResourceDescription mrd = r.getDescription();
                    String adaptor = r.getAdaptor();
                    Map<String, Object> projectConf = new HashMap<>();
                    projectConf.put("Properties", r.getProjectConf());
                    Map<String, Object> resourcesConf = new HashMap<>();
                    resourcesConf.put("Properties", r.getResourceConf());
                    host = registerWorker(workerName, mrd, adaptor, projectConf, resourcesConf);
                } else {
                    if (host == Comm.getAppHost()) {
                        LogicalData localData = Comm.getData(uri.getPath());
                        if (localData != null) {
                            otherNamedLocalData = localData;
                            addedSources++;
                            continue;
                        }
                    }
                }
                DataLocation dl = DataLocation.createLocation(host, uri);
                locations.add(dl);
            } catch (AgentException | IOException e) {
                // Do nothing. Ignore location
                e.printStackTrace();
            }
        }

        if (ld == null) {
            if (otherNamedLocalData == null) {
                ld = Comm.registerData(remote.getRenaming());
            } else {
                try {
                    Comm.linkData(otherNamedLocalData.getName(), remote.getRenaming());
                } catch (CommException ce) {
                    ErrorManager
                        .error("Could not link " + remote.getRenaming() + " and " + otherNamedLocalData.getName(), ce);
                }
                addedSources++;
            }
        }
        for (DataLocation loc : locations) {
            ld.addLocation(loc);
            addedSources++;
        }
        if (addedSources == 0) {
            throw new AgentException("Could not add any source for data " + remote.getRenaming());
        }
    }

    /**
     * Adds new resources into the resource pool.
     *
     * @param r Description of the resources to add into the resource pool
     * @throws AgentException could not create a configuration to start using this resource
     */
    public static void addResources(Resource<?, ?> r) throws AgentException {
        String workerName = r.getName();
        MethodResourceDescription description = r.getDescription();

        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.increasedDynamicWorker(worker, description);
        } else {
            String adaptor = r.getAdaptor();
            Map<String, Object> projectConf = new HashMap<>();
            projectConf.put("Properties", r.getProjectConf());
            Map<String, Object> resourcesConf = new HashMap<>();
            resourcesConf.put("Properties", r.getResourceConf());
            registerWorker(workerName, description, adaptor, projectConf, resourcesConf);
        }
    }

    private static DynamicMethodWorker registerWorker(String workerName, MethodResourceDescription description,
        String adaptor, Map<String, Object> projectConf, Map<String, Object> resourcesConf) throws AgentException {
        if (description == null) {
            description = new MethodResourceDescription();
        }

        MethodConfiguration mc;
        try {
            mc = (MethodConfiguration) Comm.constructConfiguration(adaptor, projectConf, resourcesConf);
        } catch (ConstructConfigurationException e) {
            throw new AgentException(e.getMessage(), e);
        }
        int limitOfTasks = mc.getLimitOfTasks();
        int computingUnits = description.getTotalCPUComputingUnits();
        if (limitOfTasks < 0 && computingUnits < 0) {
            mc.setLimitOfTasks(0);
            mc.setTotalComputingUnits(0);
        } else {
            mc.setLimitOfTasks(Math.max(limitOfTasks, computingUnits));
            mc.setTotalComputingUnits(Math.max(limitOfTasks, computingUnits));
        }
        mc.setLimitOfGPUTasks(description.getTotalGPUComputingUnits());
        mc.setTotalGPUComputingUnits(description.getTotalGPUComputingUnits());
        mc.setLimitOfFPGATasks(description.getTotalFPGAComputingUnits());
        mc.setTotalFPGAComputingUnits(description.getTotalFPGAComputingUnits());
        mc.setLimitOfOTHERsTasks(description.getTotalOTHERComputingUnits());
        mc.setTotalOTHERComputingUnits(description.getTotalOTHERComputingUnits());

        mc.setHost(workerName);
        DynamicMethodWorker worker;
        worker = new DynamicMethodWorker(workerName, description, mc, new HashMap<>());
        ResourceManager.addDynamicWorker(worker, description);
        return worker;
    }

    /**
     * Requests the agent to stop using some resources from a node.
     *
     * @param workerName name of the worker to whom the resources belong.
     * @param reduction description of the resources to stop using.
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeResources(String workerName, MethodResourceDescription reduction) throws AgentException {
        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.requestWorkerReduction(worker, reduction);
        } else {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Request the agent to stop using all the resources from a node.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeNode(String workerName) throws AgentException {
        try {
            ResourceManager.requestWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Forces the agent to remove a node with which it has lost the connection.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void lostNode(String workerName) throws AgentException {
        try {
            ResourceManager.notifyWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Starts an agent interface.
     *
     * @param conf Agent Interface configuration parameters
     * @throws ClassNotFoundException Could not find the specify agent interface class
     * @throws InstantiationException Could not instantiate the agent interface
     * @throws IllegalAccessException Could not call the empty constructor because is private
     * @throws AgentException Error during the interface boot process
     */
    @SuppressWarnings("unchecked")
    public static final void startInterface(AgentInterfaceConfig conf)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        AgentInterface<AgentInterfaceConfig> itf = (AgentInterface<AgentInterfaceConfig>) conf.getAgentInterface();
        itf.start(conf);
        INTERFACES.add(itf);
    }

    private static AgentInterfaceConfig getInterfaceConfig(String className, JSONObject arguments)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        Class<?> agentClass = Class.forName(className);
        AgentInterface<?> itf = (AgentInterface<?>) agentClass.newInstance();
        return itf.configure(arguments);
    }

    /**
     * Main method to start a COMPSs agent. (Currently it only allows a REST agent)
     *
     * @param args Set of JSONObjects describing the AgentInterfaces to start and their configurations.
     * @throws Exception Could not create the configuration for the REST agent due to internal errors
     */
    public static final void main(String[] args) throws Exception {
        LinkedList<AgentInterfaceConfig> agents = new LinkedList<>();

        String agentConfig = System.getProperty(AgentConstants.AGENT_CONFIG_PATH);

        if (agentConfig != null && !agentConfig.isEmpty()) {
            LOGGER.info("Reading Agent config from " + agentConfig);
            File configFile = new File(agentConfig);
            if (configFile.exists()) {
                String configString = new String(Files.readAllBytes(configFile.toPath()));
                JSONArray array = new JSONArray(configString);
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jo = array.getJSONObject(i);
                    try {
                        String interfaceClass = jo.getString("AGENT_IMPL");
                        JSONObject conf = jo.getJSONObject("CONF");
                        LOGGER.info("Loading " + interfaceClass + "'s agent interface");
                        AgentInterfaceConfig aic = getInterfaceConfig(interfaceClass, conf);
                        agents.add(aic);
                    } catch (Exception e) {
                        ErrorManager.warn("Unexpected format for agent config: " + jo);
                    }
                }
            } else {
                ErrorManager.warn("Could not find the agent configuration file " + agentConfig);
            }
        }

        for (String arg : args) {
            try {
                JSONObject jo = new JSONObject(arg);
                String interfaceClass = jo.getString("AGENT_IMPL");
                JSONObject conf = jo.getJSONObject("CONF");
                LOGGER.info("Loading " + agentConfig + "'s agent interface");
                AgentInterfaceConfig aic = getInterfaceConfig(interfaceClass, conf);
                agents.add(aic);
            } catch (Exception e) {
                ErrorManager.warn("Unexpected format for agent config: " + arg);
            }
        }

        for (AgentInterfaceConfig agent : agents) {
            try {
                startInterface(agent);
            } catch (Exception e) {
                ErrorManager.warn("Could not start Agent", e);
            }
        }
        if (INTERFACES.isEmpty()) {
            ErrorManager.fatal("Could not start any interface");
        }
        start();
    }

    /**
     * Handles the notification of the end of an application.
     *
     * @param appId Identifier of the finished application
     */
    public static void finishedApplication(long appId) {
        // Remove all data bound to the application
        RUNTIME.removeApplicationData(appId);
        RUNTIME.deregisterApplication(appId);
    }
}
