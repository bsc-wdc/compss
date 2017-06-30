/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package integratedtoolkit.comm.fake;

import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.exceptions.ConstructConfigurationException;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.fake.FakeNode;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.uri.MultiURI;
import java.util.LinkedList;

/**
 *
 * @author flordan
 */
public class FakeMethodAdaptor implements CommAdaptor {

    @Override
    public void init() {

    }

    @Override
    public Configuration constructConfiguration(Object project_properties, Object resources_properties) throws ConstructConfigurationException {
        return new MethodConfiguration(this.getClass().getName());
    }

    @Override
    public COMPSsWorker initWorker(String workerName, Configuration config) {
        return new FakeNode(workerName);

    }

    @Override
    public void stop() {

    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return new LinkedList<>();
    }

    @Override
    public void completeMasterURI(MultiURI u) {

    }

    @Override
    public void stopSubmittedJobs() {

    }

}
