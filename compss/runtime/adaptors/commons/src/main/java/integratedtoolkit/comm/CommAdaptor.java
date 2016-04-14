package integratedtoolkit.comm;

import integratedtoolkit.types.data.location.URI;
import java.util.LinkedList;

import integratedtoolkit.types.AdaptorDescription;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.data.operation.DataOperation;

import java.util.HashMap;
import java.util.TreeMap;

public interface CommAdaptor {

    public void init();

    public COMPSsWorker initWorker(String workerName, HashMap<String, String> properties, TreeMap<String, AdaptorDescription> adaptorsDesc) throws Exception;

    public void stop();

    public LinkedList<DataOperation> getPending();

    public void completeMasterURI(URI u);

    public void stopSubmittedJobs();

}
