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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.ExternalPropertyException;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.ExternalStreamData;
import es.bsc.compss.util.ExternalStreamHandler;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.requests.AddStreamWriterRequest;


public class ExternalStreamAccessParams extends StreamAccessParams<DataLocation, ExternalStreamData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new ExternalStreamAccessParams instance for the given object.
     *
     * @param app Id of the application accessing the stream.
     * @param dir Access mode.
     * @param location Location of the external stream.
     * @return new ExternalStreamAccessParams instance
     */
    public static final ExternalStreamAccessParams constructESAP(Application app, Direction dir,
        DataLocation location) {
        return new ExternalStreamAccessParams(app, dir, location);
    }

    private ExternalStreamAccessParams(Application app, Direction dir, DataLocation location) {
        super(app, new ExternalStreamData(location.hashCode()), dir, location);
    }

    @Override
    protected void registerValueForVersion(DataVersion dv) {
        EngineDataInstanceId lastDID = dv.getDataInstanceId();
        String renaming = lastDID.getRenaming();
        Comm.registerLocation(renaming, this.getValue());
    }

    @Override
    protected void externalRegister() {
        DataLocation location = this.getValue();
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
