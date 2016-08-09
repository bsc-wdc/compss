package integratedtoolkit.comm;

import java.util.LinkedList;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.uri.MultiURI;


public interface CommAdaptor {

    public void init();
    
    public Configuration constructConfiguration(Object project_properties, Object resources_properties) throws Exception;

    public COMPSsWorker initWorker(String workerName, Configuration config);

    public void stop();

    public LinkedList<DataOperation> getPending();

    public void completeMasterURI(MultiURI u);

    public void stopSubmittedJobs();

}
