package es.bsc.compss.ws.master;

import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.resources.Resource;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.namespace.QName;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.endpoint.ClientCallback;
import org.apache.cxf.jaxws.endpoint.dynamic.JaxWsDynamicClientFactory;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WSJob extends Job<ServiceInstance> {

    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    protected static final boolean debug = logger.isDebugEnabled();

    private static RequestQueue<WSJob> callerQueue;
    private static WSCaller caller;
    private static final JaxWsDynamicClientFactory dcf = JaxWsDynamicClientFactory.newInstance();
    // wsdl-port--> Client
    private static final HashMap<String, Client> portToClient = new HashMap<>();

    // Pool of worker threads and queue of requests
    private static ThreadPool callerPool;

    private static final int POOL_SIZE = 10;
    private static final String POOL_NAME = "WS";
    private static final String THREAD_POOL_ERR = "Error starting pool of threads";
    private static final String SUBMIT_ERROR = "Error calling Web Service";

    private Object returnValue;


    public static void init() throws Exception {
        // Create thread that will handle job submission requests
        if (callerQueue == null) {
            callerQueue = new RequestQueue<>();
        } else {
            callerQueue.clear();
        }
        caller = new WSCaller(callerQueue);
        callerPool = new ThreadPool(POOL_SIZE, POOL_NAME, caller);
        try {
            callerPool.startThreads();
        } catch (Exception e) {
            logger.error(THREAD_POOL_ERR, e);
            throw e;
        }
    }

    public static void end() {
        try {
            callerPool.stopThreads();
        } catch (Exception e) {
        }
    }

    public WSJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, JobListener listener) {
        super(taskId, taskParams, impl, res, listener);
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
    public void stop() {

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
                    Parameter[] parameters = taskParams.getParameters();
                    for (int i = 0; i < taskParams.getParameters().length; i++) {
                        if (parameters[i].getDirection() == Direction.IN) {
                            switch (parameters[i].getType()) {
                                case OBJECT_T:
                                case PSCO_T:
                                case EXTERNAL_OBJECT_T:
                                    DependencyParameter dp = (DependencyParameter) parameters[i];
                                    Object o = getObjectValue(dp);
                                    input.add(o);
                                    break;
                                case FILE_T:
                                    // CAN'T USE A FILE AS A PARAMETER
                                    // SKIP!
                                    break;
                                default:
                                    // Basic or String
                                    BasicTypeParameter btParB = (BasicTypeParameter) parameters[i];
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
                    job.listener.jobFailed(job, JobEndStatus.EXECUTION_FAILED);
                    logger.error(SUBMIT_ERROR, e);
                    return;
                }

            }
        }

        private Client getClient(ServiceInstance si, String portName) {
            Client c = portToClient.get(si.getName() + "-" + portName);
            if (c == null) {
                c = addPort(si, portName);
                portToClient.put(si.getName() + "-" + portName, c);
            }
            return c;
        }

        public synchronized Client addPort(ServiceInstance si, String portName) {
            Client client = portToClient.get(portName);
            if (client != null) {
                return client;
            }

            QName serviceQName = new QName(si.getNamespace(), si.getServiceName());
            QName portQName = new QName(si.getNamespace(), portName);
            try {
                client = dcf.createClient(si.getWsdl(), serviceQName, portQName);
            } catch (Exception e) {
                logger.error("Exception", e);
                return null;
            }

            HTTPConduit http = (HTTPConduit) client.getConduit();
            HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
            httpClientPolicy.setConnectionTimeout(0);
            httpClientPolicy.setReceiveTimeout(0);
            http.setClient(httpClientPolicy);

            portToClient.put(portName, client);
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
