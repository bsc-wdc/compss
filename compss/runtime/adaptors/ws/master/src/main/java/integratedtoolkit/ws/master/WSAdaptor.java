package integratedtoolkit.ws.master;

import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.DataOperation;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class WSAdaptor implements CommAdaptor {

    //Logging
    public static final Logger logger = Logger.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    // Tracing
    protected static boolean tracing;

    @Override
    public void init() {
        try {
            WSJob.init();
        } catch (Exception e) {
            logger.error("Can not initialize WS Adaptor");
        }
    }

    @Override
    public ServiceInstance initWorker(String workerName, HashMap<String, String> properties, TreeMap<String, AdaptorDescription> adaptorsDesc) {
        return new ServiceInstance(workerName, properties);
    }

    @Override
    public void stop() {
        WSJob.end();
    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return null;
    }

    @Override
    public void stopSubmittedJobs() {

    }

    @Override
    public void completeMasterURI(URI u) {
        //No need to do nothing
    }
}
