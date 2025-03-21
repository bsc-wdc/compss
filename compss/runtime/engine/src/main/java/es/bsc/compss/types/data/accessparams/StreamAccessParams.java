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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.params.StreamData;
import es.bsc.distrostreamlib.api.DistroStream;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.requests.AddStreamWriterRequest;


public class StreamAccessParams<T extends Object, D extends StreamData> extends ObjectAccessParams<T, D> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new StreamAccessParams instance for the given object.
     * 
     * @param app Id of the application accessing the stream.
     * @param dir operation performed.
     * @param value Associated object.
     * @param code Hashcode of the associated object.
     * @return new StreamAccessParams instance
     */
    public static final <T extends Object> StreamAccessParams<T, StreamData> constructStreamAP(Application app,
        Direction dir, T value, int code) {
        return new StreamAccessParams(app, new StreamData(code), dir, value);
    }

    protected StreamAccessParams(Application app, D data, Direction dir, T value) {
        super(app, data, dir, value);
    }

    @Override
    protected void registerValueForVersion(DataVersion dv) {
        EngineDataInstanceId lastDID = dv.getDataInstanceId();
        String renaming = lastDID.getRenaming();
        Comm.registerValue(renaming, this.getValue());
    }

    @Override
    protected void externalRegister() {
        // Inform the StreamClient
        if (mode != AccessMode.R) {
            DistroStream<?> ds = (DistroStream<?>) this.getValue();
            String streamId = ds.getId();
            if (DEBUG) {
                LOGGER.debug("Registering writer for stream " + streamId);
            }
            AddStreamWriterRequest req = new AddStreamWriterRequest(streamId);
            // Registering the writer asynchronously (no check completion nor error)
            DistroStreamClient.request(req);
        }
    }
}
