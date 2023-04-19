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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataParams.ExternalPSCObjectData;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessparams.ExternalPSCObjectAccessParams;


/**
 * Handling of an access from the main code to an external PSC object.
 */
public class ExternalPSCObjectMainAccess
    extends ObjectMainAccess<String, ExternalPSCObjectData, ExternalPSCObjectAccessParams> {

    /**
     * Creates a new ExternalPSCObjectAccessParams instance.
     *
     * @param app Id of the application accessing the External PSCO.
     * @param dir operation performed.
     * @param pscoId Id of the accessed PSCO.
     * @param hashCode Hashcode of the associated External PSCO
     * @return new ExternalPSCObjectAccessParams instance
     */
    public static final ExternalPSCObjectMainAccess constructEPOMA(Application app, Direction dir, String pscoId,
        int hashCode) {
        ExternalPSCObjectAccessParams epoap = ExternalPSCObjectAccessParams.constructEPOAP(app, dir, pscoId, hashCode);
        return new ExternalPSCObjectMainAccess(epoap);
    }

    protected ExternalPSCObjectMainAccess(ExternalPSCObjectAccessParams params) {
        super(params);
    }

    /**
     * Fetches the last version of the object.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open file.
     */
    @Override
    public String fetch(DataAccessId daId) {
        // TODO: Check if the object was already piggybacked in the task notification
        String lastRenaming = ((RWAccessId) daId).getReadDataInstance().getRenaming();
        return Comm.getData(lastRenaming).getPscoId();
    }

    @Override
    public boolean isAccessFinishedOnRegistration() {
        return false;
    }
}
