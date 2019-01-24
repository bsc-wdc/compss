/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;

import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ImplementationDefinition;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.parsers.ITFParser;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import storage.StorageException;
import storage.StorageItf;


public class Agent {

    private static final COMPSsRuntimeImpl RUNTIME;
    private static final Random APP_ID_GENERATOR = new Random();

    static {
        String dcConfigPath = System.getProperty(AgentConstants.DATACLAY_CONFIG_PATH);
        System.out.println("DataClay configuration: " + dcConfigPath);
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
        RUNTIME.startIT();

        CoreElementDefinition ced = new CoreElementDefinition();
        ced.setCeSignature("load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)");
        MethodResourceDescription mrd = new MethodResourceDescription("");
        for (Processor p : mrd.getProcessors()) {
            p.setName("LocalProcessor");
        }
        ImplementationDefinition implDef = ImplementationDefinition.defineImplementation(
                "METHOD",
                "load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)es.bsc.compss.agent.loader.Loader",
                new MethodResourceDescription(""),
                "es.bsc.compss.agent.loader.Loader", "load");
        ced.addImplementation(implDef);
        RUNTIME.registerCoreElement(ced);

        String hostName = System.getProperty(AgentConstants.COMPSS_AGENT_NAME);
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostName = "localhost";
            }
        }
        System.setProperty(AgentConstants.COMPSS_AGENT_NAME, hostName);
    }

    public static long runMain(Lang lang, String ceiClass, String className, String methodName, Object[] params, AppMonitor monitor) throws AgentException {

        long appId = Math.abs(APP_ID_GENERATOR.nextLong());
        long mainAppId = Math.abs(APP_ID_GENERATOR.nextLong());
        monitor.setAppId(mainAppId);

        try {
            Class<?> cei = Class.forName(ceiClass);
            List<CoreElementDefinition> ceds = ITFParser.parseITFMethods(cei);
            for (CoreElementDefinition ced : ceds) {
                RUNTIME.registerCoreElement(ced);
            }
        } catch (ClassNotFoundException cnfe) {
            throw new AgentException("Could not find class " + ceiClass + " to detect internal methods.");
        }

        RUNTIME.executeTask(
                mainAppId,
                monitor,
                lang, "es.bsc.compss.agent.loader.Loader", "load",
                false, 1, false, false,
                false, 7,
                new Object[]{
                    RUNTIME, DataType.OBJECT_T, Direction.IN, Stream.UNSPECIFIED, "", "runtime",
                    RUNTIME, DataType.OBJECT_T, Direction.IN, Stream.UNSPECIFIED, "", "api",
                    ceiClass, DataType.STRING_T, Direction.IN, Stream.UNSPECIFIED, "", "ceiClass",
                    appId, DataType.LONG_T, Direction.IN, Stream.UNSPECIFIED, "", "appId",
                    className, DataType.STRING_T, Direction.IN, Stream.UNSPECIFIED, "", "className",
                    methodName, DataType.STRING_T, Direction.IN, Stream.UNSPECIFIED, "", "methodName",
                    params, DataType.OBJECT_T, Direction.IN, Stream.UNSPECIFIED, "", "params"
                }
        );
        return mainAppId;
    }

    public static long runTask(Lang lang, String className, String methodName, ApplicationParameter[] sarParams, ApplicationParameter target, boolean hasResult, AppMonitor monitor) throws AgentException {
        long appId = Math.abs(APP_ID_GENERATOR.nextLong());
        monitor.setAppId(appId);
        try {
            //PREPARING PARAMETERS
            StringBuilder typesSB = new StringBuilder();

            int paramsCount = sarParams.length;
            if (target != null) {
                paramsCount++;
            }
            if (hasResult) {
                paramsCount++;
            }

            Object[] params = new Object[6 * paramsCount];
            int position = 0;
            for (ApplicationParameter param : sarParams) {
                if (typesSB.length() > 0) {
                    typesSB.append(",");
                }
                if (param.getType() != DataType.PSCO_T) {
                    typesSB.append(param.getType().toString());
                } else {
                    typesSB.append("OBJECT_T");
                }
                params[position] = param.getValueContent();
                params[position + 1] = param.getType();
                params[position + 2] = param.getDirection();
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = ""; // Prefix
                params[position + 5] = ""; // Parameter Name
                position += 6;
            }

            if (target != null) {
                params[position] = target.getValueContent();
                params[position + 1] = target.getType();
                params[position + 2] = target.getDirection();
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = "";
                params[position + 5] = ""; // Parameter Name
                position += 6;
            }

            if (hasResult) {
                params[position] = null;
                params[position + 1] = DataType.OBJECT_T;
                params[position + 2] = Direction.OUT;
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = "";
                params[position + 5] = "";
                position += 6;
            }

            String paramsTypes = typesSB.toString();

            String ceSignature = methodName + "(" + paramsTypes + ")";
            String implSignature = methodName + "(" + paramsTypes + ")" + className;
            CoreElementDefinition ced = new CoreElementDefinition();
            ced.setCeSignature(ceSignature);
            ImplementationDefinition implDef = ImplementationDefinition.defineImplementation(
                    "METHOD",
                    implSignature,
                    new MethodResourceDescription(""),
                    className, methodName);
            ced.addImplementation(implDef);
            RUNTIME.registerCoreElement(ced);

            RUNTIME.executeTask(
                    appId,
                    monitor,
                    lang, className, methodName,
                    false, 1, false, false,
                    target != null, paramsCount, params
            );

        } catch (Exception e) {
            throw new AgentException(e);
        }
        return appId;
    }

    public static void addNode(String workerName, MethodResourceDescription description, String adaptor, Object projectConf, Object resourcesConf) throws AgentException {
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
        mc.setLimitOfOTHERSTasks(description.getTotalOTHERComputingUnits());
        mc.setTotalOTHERComputingUnits(description.getTotalOTHERComputingUnits());

        mc.setHost(workerName);
        DynamicMethodWorker mw = new DynamicMethodWorker(workerName, description, mc, new HashMap());
        ResourceManager.addDynamicWorker(mw, description);
    }

    public static void removeNode(String name) throws AgentException {
        try {
            ResourceManager.reduceWholeWorker(name);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + name + "was not set up for this agent. Ignoring request.");
        }
    }


    public static abstract class AppMonitor implements TaskMonitor {

        private long appId;

        public AppMonitor() {
        }

        private void setAppId(long appId) {
            this.appId = appId;
        }

        public long getAppId() {
            return this.appId;
        }

    }
}
