package integratedtoolkit.test.dummyAdaptor;

import java.util.LinkedList;

import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class DummyAdaptor implements CommAdaptor {

	public static final String ID = DummyAdaptor.class.getCanonicalName();


	public DummyAdaptor() {

	}

	@Override
	public void init() {
	}

	@Override
	public MethodConfiguration constructConfiguration(Object project_properties, Object resources_properties) throws Exception {
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
	public void completeMasterURI(integratedtoolkit.types.uri.MultiURI uri) {
	}

	@Override
	public LinkedList<DataOperation> getPending() {
		return null;
	}

}
