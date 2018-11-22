/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


public class MasterResourceImpl extends DynamicMethodWorker implements MasterResource {

    private static final String MASTER_NAME_PROPERTY = System.getProperty(COMPSsConstants.MASTER_NAME);
    private static final String UNDEFINED_MASTER_NAME = "master";

    public static final String MASTER_NAME;

    static {
        // Initializing host attributes
        String hostName = "";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals("")) && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            hostName = MASTER_NAME_PROPERTY;
        } else {
            // The MASTER_NAME_PROPERTY has not been defined, try load from machine
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                hostName = localHost.getCanonicalHostName();
            } catch (UnknownHostException e) {
                // Sets a default hsotName value
                ErrorManager.warn("ERROR_UNKNOWN_HOST: " + e.getLocalizedMessage());
                hostName = UNDEFINED_MASTER_NAME;
            }
        }
        MASTER_NAME = hostName;
    }

    public MasterResourceImpl() {
        super(MASTER_NAME, new MethodResourceDescription(), new COMPSsMaster(MASTER_NAME), 0, 0, 0, 0, new HashMap<String, String>());
    }

    public String getCOMPSsLogBaseDirPath() {
        return ((COMPSsMaster) this.getNode()).getCOMPSsLogBaseDirPath();
    }

    @Override
    public String getWorkingDirectory() {
        return ((COMPSsMaster) this.getNode()).getWorkingDirectory();
    }

    public String getUserExecutionDirPath() {
        return ((COMPSsMaster) this.getNode()).getUserExecutionDirPath();
    }

    @Override
    public String getAppLogDirPath() {
        return ((COMPSsMaster) this.getNode()).getAppLogDirPath();
    }

    @Override
    public String getTempDirPath() {
        return ((COMPSsMaster) this.getNode()).getTempDirPath();
    }

    @Override
    public String getJobsDirPath() {
        return ((COMPSsMaster) this.getNode()).getJobsDirPath();
    }

    @Override
    public String getWorkersDirPath() {
        return ((COMPSsMaster) this.getNode()).getWorkersDirPath();
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public Type getType() {
        return Type.MASTER;
    }

    @Override
    public int compareTo(Resource t) {
        if (t.getType() == Type.MASTER) {
            return getName().compareTo(t.getName());
        } else {
            return 1;
        }
    }

    @Override
    public void updateResource(MethodResourceDescription mrd, Map<String, String> sharedDisks) {
        this.description.mimic(mrd);
        this.available.mimic(mrd);
        this.setMaxCPUTaskCount(mrd.getTotalCPUComputingUnits());
        this.setMaxGPUTaskCount(mrd.getTotalGPUComputingUnits());
        this.setMaxFPGATaskCount(mrd.getTotalFPGAComputingUnits());
        this.setMaxOthersTaskCount(mrd.getTotalOTHERComputingUnits());
        ((COMPSsMaster) this.getNode()).setUpExecutionCapabilities(mrd, mrd.getTotalCPUComputingUnits());
        super.sharedDisks = sharedDisks;
    }

}
