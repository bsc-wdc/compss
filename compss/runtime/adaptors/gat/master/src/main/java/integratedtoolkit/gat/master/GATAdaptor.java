package integratedtoolkit.gat.master;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;

import java.util.LinkedList;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.comm.Dispatcher;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.util.ThreadPool;

import java.io.File;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class GATAdaptor implements CommAdaptor {

    public static final String ID = GATAdaptor.class.getCanonicalName();

    private static final int GAT_POOL_SIZE = 5;
    protected static final String POOL_NAME = "FTM";
    protected static final String SAFE_POOL_NAME = "SAFE_FTM";
    protected static final String THREAD_POOL_ERR = "Error starting pool of threads";
    protected static final String POOL_ERR = "Error deleting pool of threads";
    protected static final int SAFE_POOL_SIZE = 1;

    // Copy request queues
    // copyQueue is for ordinary copies
    // safeQueue is for priority copies
    public static RequestQueue<DataOperation> copyQueue;
    public static RequestQueue<DataOperation> safeQueue;

    protected static ThreadPool pool;
    protected static ThreadPool safePool;

    private static String masterUser = System.getProperty("user.name");
    // GAT context
    private static GATContext transferContext;

    //LOGGING
    private static final Logger logger = Logger.getLogger(Loggers.COMM);
    private static final boolean debug = logger.isDebugEnabled();

    public GATAdaptor() {

    }

    public void init() {
        // Create request queues
        copyQueue = new RequestQueue<DataOperation>();
        safeQueue = new RequestQueue<DataOperation>();

        String adaptor = System.getProperty(ITConstants.GAT_FILE_ADAPTOR);
        
        if (debug) {
        	logger.debug("Initializing GAT");
        }
        //GATJob.init();

        pool = new ThreadPool(GAT_POOL_SIZE, POOL_NAME, new Dispatcher(copyQueue));
        try {
            pool.startThreads();
        } catch (Exception e) {
        	ErrorManager.error(THREAD_POOL_ERR, e);
        }

        safePool = new ThreadPool(SAFE_POOL_SIZE, SAFE_POOL_NAME, new Dispatcher(safeQueue));
        try {
            safePool.startThreads();
        } catch (Exception e) {
        	ErrorManager.error(THREAD_POOL_ERR, e);
        }

        // GAT adaptor path
        if (debug) {
        	logger.debug("Initializing GAT Tranfer Context");
        }
        transferContext = new GATContext();

        /* We need to try the local adaptor when both source and target hosts
         * are local, because ssh file adaptor cannot perform local operations
         */
        transferContext.addPreference("File.adaptor.name", adaptor + ", srcToLocalToDestCopy, local");

    }

    // GAT adaptor initializes the worker each time it sends a new job
    @Override
    public GATWorkerNode initWorker(String name, HashMap<String, String> properties, TreeMap<String, AdaptorDescription> adaptorsDesc) {
    	logger.debug("There are "+ adaptorsDesc.size()+ "descriptions.\n"+adaptorsDesc.toString());
    	GATWorkerNode node = new GATWorkerNode(name, properties, adaptorsDesc.get(AdaptorDescription.GATAdaptor));
        return node;
    }

    public static void addTransferContextPreferences(String name, String value) {
        transferContext.addPreference(name, value);
    }

    public LinkedList<DataOperation> getPending() {
        LinkedList<DataOperation> l = new LinkedList<DataOperation>();

        for (DataOperation c : copyQueue.getQueue()) {
            l.add(c);
        }
        for (DataOperation c : safeQueue.getQueue()) {
            l.add(c);
        }
        return l;
    }

    @Override
    public void stop() {
        // Make pool threads finish
        try {
            pool.stopThreads();
            safePool.stopThreads();
        } catch (Exception e) {
            logger.error(POOL_ERR, e);
        }

        GAT.end();
    }

    @Override
    public void stopSubmittedJobs() {
    	GATJob.stopAll();
    }

    @Override
    public void completeMasterURI(integratedtoolkit.types.data.location.URI uri) {
        String scheme = uri.getScheme();
        String user = masterUser + "@";
        String host = uri.getHost().getName();
        String path = uri.getPath();
        if (!path.contains(File.separator)) {
            return;
        }

        String s = (scheme
                + user
                + host + File.separator
                + path);
        try {
            uri.setInternalURI(ID, new org.gridlab.gat.URI(s));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void enqueueCopy(Copy c) {
        copyQueue.enqueue(c);
    }
    
    public static GATContext getTransferContext(){
    	return transferContext;
    }
}
