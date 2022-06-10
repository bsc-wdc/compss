/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.ws.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;
import es.bsc.compss.worker.COMPSsException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.QName;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.endpoint.ClientCallback;
import org.apache.cxf.jaxws.endpoint.dynamic.JaxWsDynamicClientFactory;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WSJob extends Job<ServiceInstance> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private static final String SUBMIT_ERROR = "Error calling Web Service";

    // Class structures
    private static final int POOL_SIZE = 10;
    private static final String POOL_NAME = "WS";
    private static final JaxWsDynamicClientFactory DCF = JaxWsDynamicClientFactory.newInstance();
    // wsdl-port--> Client
    private static final Map<String, Client> PORT2CLIENT = new HashMap<>();

    private static RequestQueue<WSJob> callerQueue;
    private static WSCaller caller;
    // Pool of worker threads and queue of requests
    private static ThreadPool callerPool;

    private Object returnValue;


    /**
     * Initializes the WSJob structures.
     */
    public static void init() {
        // Create thread that will handle job submission requests
        if (callerQueue == null) {
            callerQueue = new RequestQueue<>();
        } else {
            callerQueue.clear();
        }
        caller = new WSCaller(callerQueue);
        callerPool = new ThreadPool(POOL_SIZE, POOL_NAME, caller);
        callerPool.startThreads();
    }

    /**
     * Stops the WSJob structures.
     */
    public static void end() {
        callerPool.stopThreads();
    }

    /**
     * Creates a new WSJob instance.
     *
     * @param taskId Associated task Id.
     * @param taskParams Associated task parameters.
     * @param impl Task implementation.
     * @param res Resource.
     * @param listener Task listener.
     */
    public WSJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, JobListener listener,
        List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, taskParams, impl, res, listener, predecessors, numSuccessors);

        this.returnValue = null;
    }

    @Override
    public TaskType getType() {
        return TaskType.SERVICE;
    }

    @Override
    public void submit() {
        callerQueue.enqueue(this);
    }

    @Override
    public void cancelJob() {

    }

    @Override
    public Object getReturnValue() {
        return returnValue;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[[Job id: ").append(getJobId()).append("]");
        buffer.append(", ").append(taskParams.toString());
        String name = "";
        COMPSsNode node = getResourceNode();
        name = node.getName();
        buffer.append(", [Target URL: ").append(name).append("]]");

        return buffer.toString();
    }

    @Override
    public String getHostName() {
        return getResourceNode().getName();
    }


    private static class WSCaller extends RequestDispatcher<WSJob> {

        public WSCaller(RequestQueue<WSJob> queue) {
            super(queue);
        }

        @Override
        public void processRequests() {
            while (true) {
                WSJob job = queue.dequeue();
                if (job == null) {
                    break;
                }
                try {
                    ArrayList<Object> input = new ArrayList<>();
                    TaskDescription taskParams = job.taskParams;
                    ServiceImplementation service = (ServiceImplementation) job.impl;
                    for (Parameter par : taskParams.getParameters()) {
                        if (par.getDirection() == Direction.IN || par.getDirection() == Direction.IN_DELETE) {
                            switch (par.getType()) {
                                case OBJECT_T:
                                case PSCO_T:
                                case EXTERNAL_PSCO_T:
                                    DependencyParameter dp = (DependencyParameter) par;
                                    Object o = getObjectValue(dp);
                                    input.add(o);
                                    break;
                                case FILE_T:
                                    LOGGER.error("Error: WS CAN'T USE BINDING FILES AS PARAMETERS!");
                                    // Skip
                                    break;
                                case STREAM_T:
                                case EXTERNAL_STREAM_T:
                                    LOGGER.error("Error: WS CAN'T USE STREAMS AS PARAMETERS!");
                                    // Skip
                                    break;
                                case BINDING_OBJECT_T:
                                    LOGGER.error("Error: WS CAN'T USE BINDING OBJECTS AS PARAMETERS!");
                                    // Skip
                                    break;
                                default:
                                    // Basic or String
                                    BasicTypeParameter btParB = (BasicTypeParameter) par;
                                    input.add(btParB.getValue());
                            }
                        }
                    }
                    ServiceInstance si = (ServiceInstance) job.getResourceNode();
                    String portName = service.getRequirements().getPort();
                    String operationName = service.getOperation();
                    if (operationName.compareTo("[unassigned]") == 0) {
                        operationName = taskParams.getName();
                    }
                    Client client = getClient(si, portName);

                    ClientCallback cb = new ClientCallback();
                    client.invoke(cb, operationName, input.toArray());
                    Object[] result = cb.get();

                    if (result.length > 0) {
                        job.returnValue = result[0];
                    }
                    job.listener.jobCompleted(job);
                } catch (Exception e) {
                    if (e instanceof COMPSsException) {
                        job.listener.jobException(job, (COMPSsException) e);
                    } else {
                        job.listener.jobFailed(job, JobEndStatus.EXECUTION_FAILED);
                    }
                    LOGGER.error(SUBMIT_ERROR, e);
                    return;
                }

            }
        }

        private Client getClient(ServiceInstance si, String portName) {
            Client c = PORT2CLIENT.get(si.getName() + "-" + portName);
            if (c == null) {
                c = addPort(si, portName);
                PORT2CLIENT.put(si.getName() + "-" + portName, c);
            }
            return c;
        }

        public synchronized Client addPort(ServiceInstance si, String portName) {
            Client client = PORT2CLIENT.get(portName);
            if (client != null) {
                return client;
            }

            QName serviceQName = new QName(si.getNamespace(), si.getServiceName());
            QName portQName = new QName(si.getNamespace(), portName);
            try {
                client = DCF.createClient(si.getWsdl(), serviceQName, portQName);
            } catch (Exception e) {
                LOGGER.error("Exception", e);
                return null;
            }

            HTTPConduit http = (HTTPConduit) client.getConduit();
            HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
            httpClientPolicy.setConnectionTimeout(0);
            httpClientPolicy.setReceiveTimeout(0);
            http.setClient(httpClientPolicy);

            PORT2CLIENT.put(portName, client);
            return client;
        }

        private Object getObjectValue(DependencyParameter dp) throws CannotLoadException {
            String renaming = ((RAccessId) dp.getDataAccessId()).getReadDataInstance().getRenaming();
            LogicalData ld = Comm.getData(renaming);
            if (!ld.isInMemory()) {
                ld.loadFromStorage();
            }

            return ld.getValue();
        }
    }

}
