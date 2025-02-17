/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.resources;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.uri.MultiURI;

import java.util.HashMap;
import java.util.Map;


public class MasterResourceImpl extends DynamicMethodWorker implements MasterResource {

    /**
     * Creates a new Master Resource.
     */
    public MasterResourceImpl() {
        super(COMPSsMaster.getMasterName(), // Master name
            new MethodResourceDescription(), // Master resource description
            new COMPSsMaster(null), // Master COMPSs node
            0, // limit of tasks
            0, // Limit of GPU tasks
            0, // Limit of FPGA tasks
            0, // Limit of OTHER tasks
            new HashMap<>()// Shared disks
        );
    }

    @Override
    public String getWorkingDirectory() {
        return ((COMPSsMaster) this.getNode()).getWorkingDirectory();
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public ResourceType getType() {
        return ResourceType.MASTER;
    }

    @Override
    public void retrieveUniqueDataValues() {
        // All data is kept within the master process. No need to bring to the node any data since it is already there.
    }

    @Override
    public int compareTo(Resource t) {
        if (t.getType() == ResourceType.MASTER) {
            return getName().compareTo(t.getName());
        } else {
            return 1;
        }
    }

    @Override
    public void updateDisks(Map<String, String> sharedDisks) {
        super.sharedDisksSetup = sharedDisks;
    }

    /**
     * Configures the necessary parameters so tasks executed in the worker are able to detect nested tasks.
     *
     * @param runtimeAPI runtimeAPI implementation handling the task execution
     * @param loader loaderAPI implementation to detect data accesses
     */
    public void setupNestedSupport(COMPSsRuntime runtimeAPI, LoaderAPI loader) {
        boolean enableNested = Boolean.parseBoolean(System.getProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION));
        if (enableNested) {
            ((COMPSsMaster) this.getNode()).setRuntimeApi(runtimeAPI);
            ((COMPSsMaster) this.getNode()).setLoaderApi(loader);
        }
    }
}
