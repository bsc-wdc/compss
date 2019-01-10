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
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.types.ApplicationParameter;
import es.bsc.compss.types.Resource;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import java.net.InetAddress;
import java.util.Random;
import storage.StorageException;
import storage.StorageItf;


public class Agent {

    private static final COMPSsRuntimeImpl RUNTIME;
    private static final Random APP_ID_GENERATOR = new Random();
    private static final String AGENT_NAME;

    static {
        String dcConfigPath = System.getProperty(Constants.DATACLAY_CONFIG_PATH);
        System.out.println("DataClay configuration: " + dcConfigPath);
        if (dcConfigPath != null) {
            try {
                StorageItf.init(dcConfigPath);
            } catch (StorageException se) {
                se.printStackTrace(System.err);
                System.err.println("Continuing...");
            }
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

        RUNTIME = new COMPSsRuntimeImpl();
        RUNTIME.setObjectRegistry(new ObjectRegistry(RUNTIME));
        RUNTIME.startIT();
        RUNTIME.registerCoreElement(
                "load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)",
                "load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)es.bsc.compss.agent.loader.Loader",
                "",
                "METHOD",
                new String[]{"es.bsc.compss.agent.loader.Loader", "load"}
        );

        String hostName = System.getProperty(Constants.COMPSS_AGENT_NAME);
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostName = "localhost";
            }
        }
        AGENT_NAME = hostName;
    }

    public static long runMain(Lang lang, String ceiClass, String className, String methodName, Object[] params, Resource[] resources, AppMonitor monitor) throws AgentException {

        long appId = Math.abs(APP_ID_GENERATOR.nextLong());
        long mainAppId = Math.abs(APP_ID_GENERATOR.nextLong());
        monitor.setAppId(mainAppId);

        try {
            Class<?> cei = Class.forName(ceiClass);
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

    public static long runTask(Lang lang, String className, String methodName, ApplicationParameter[] sarParams, ApplicationParameter target, boolean hasResult, Resource[] resources, AppMonitor monitor) throws AgentException {
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

            Object[] params = new Object[5 * paramsCount];
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
                params[position] = param.getValue().getContent();
                params[position + 1] = param.getType();
                params[position + 2] = param.getDirection();
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = "";
                position += 5;
            }

            if (target != null) {
                params[position] = target.getValue().getContent();
                params[position + 1] = target.getType();
                params[position + 2] = target.getDirection();
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = "";
                position += 5;
            }

            if (hasResult) {
                params[position] = null;
                params[position + 1] = DataType.OBJECT_T;
                params[position + 2] = Direction.OUT;
                params[position + 3] = Stream.UNSPECIFIED;
                params[position + 4] = "";
                position += 5;
            }

            String paramsTypes = typesSB.toString();

            RUNTIME.registerCoreElement(
                    methodName + "(" + paramsTypes + ")",
                    methodName + "(" + paramsTypes + ")" + className,
                    "",
                    "METHOD",
                    new String[]{className, methodName}
            );

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
