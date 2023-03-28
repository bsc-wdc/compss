/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.ExternalPropertyException;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessparams.DataParams.ExternalStreamData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.util.ExternalStreamHandler;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.requests.AddStreamWriterRequest;


public class ExternalStreamAccessParams extends StreamAccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new StreamAccessParams instance for the given object.
     *
     * @param app Id of the application accessing the stream.
     * @param mode Access mode.
     * @param location Location of the external stream.
     */
    public ExternalStreamAccessParams(Application app, AccessMode mode, DataLocation location) {
        super(new ExternalStreamData(app, location.hashCode()), mode, location, location.hashCode());
    }

    @Override
    public void registeredAsFirstVersionForData(DataInfo dInfo) {
        DataInstanceId lastDID = dInfo.getCurrentDataVersion().getDataInstanceId();
        String renaming = lastDID.getRenaming();
        Comm.registerLocation(renaming, (DataLocation) this.getValue());
    }

    @Override
    public void externalRegister() {
        DataLocation location = (DataLocation) this.getValue();
        // Inform the StreamClient
        if (mode != AccessMode.R) {
            String filePath = location.getURIInHost(Comm.getAppHost()).getPath();

            try {
                String pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
                if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
                    pythonInterpreter = COMPSsDefaults.PYTHON_INTERPRETER;
                }
                String streamId = ExternalStreamHandler.getExternalStreamProperty(pythonInterpreter, filePath, "id");
                if (DEBUG) {
                    LOGGER.debug("Registering writer for stream " + streamId);
                }
                AddStreamWriterRequest req = new AddStreamWriterRequest(streamId);
                // Registering the writer asynchronously (no check completion nor error)
                DistroStreamClient.request(req);
            } catch (ExternalPropertyException e) {
                LOGGER.error("ERROR: Cannot retrieve external property. Not adding stream writer", e);
            }
        }
    }
}
