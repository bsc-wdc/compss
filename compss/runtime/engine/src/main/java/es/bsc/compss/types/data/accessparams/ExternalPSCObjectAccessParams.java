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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessparams.DataParams.ExternalPSCObjectData;


public class ExternalPSCObjectAccessParams extends ObjectAccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new BindingObjectAccessParams instance.
     * 
     * @param app Id of the application accessing the External PSCO.
     * @param mode Access mode.
     * @param pscoId Id of the accessed PSCO.
     * @param hashCode Hashcode of the associated External PSCO
     */
    public ExternalPSCObjectAccessParams(Application app, AccessMode mode, String pscoId, int hashCode) {
        super(new ExternalPSCObjectData(app, hashCode), mode, pscoId, hashCode);
    }

    /**
     * Returns the id of associated PSCO.
     * 
     * @return Id of the associated PSCO.
     */
    public String getPSCOId() {
        return (String) this.getValue();
    }

    @Override
    protected void registeredAsFirstVersionForData(DataInfo dInfo) {
        if (mode != AccessMode.W) {
            DataInstanceId lastDID = dInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerExternalPSCO(renaming, this.getPSCOId());
        }
    }

}
