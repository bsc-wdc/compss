package es.bsc.compss.test.dummyAdaptor;

import java.util.LinkedList;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;


/**
 * Dummy Adaptor for testing purposes. Defined in main package because it is used in integration tests
 *
 */
public class DummyAdaptor implements CommAdaptor {

    private static final String ID = DummyAdaptor.class.getCanonicalName();


    /**
     * Instantiates a new Dummy Adaptor
     */
    public DummyAdaptor() {
        // Nothing to do since there are no attributes to initialize
    }

    @Override
    public void init() {
    }

    @Override
    public MethodConfiguration constructConfiguration(Object project_properties, Object resources_properties)
            throws ConstructConfigurationException {

        MethodConfiguration config = new MethodConfiguration(ID);
        return config;
    }

    @Override
    public DummyWorkerNode initWorker(String name, Configuration config) {
        return new DummyWorkerNode(name, (MethodConfiguration) config);
    }

    @Override
    public void stop() {
    }

    @Override
    public void stopSubmittedJobs() {
    }

    @Override
    public void completeMasterURI(es.bsc.compss.types.uri.MultiURI uri) {
    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return null;
    }

}
