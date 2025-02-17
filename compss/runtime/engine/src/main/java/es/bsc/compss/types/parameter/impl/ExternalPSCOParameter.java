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
package es.bsc.compss.types.parameter.impl;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.ExternalPSCObjectAccessParams;
import es.bsc.compss.types.data.params.ExternalPSCObjectData;


public class ExternalPSCOParameter
    extends ObjectParameter<String, ExternalPSCObjectAccessParams, ExternalPSCObjectData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private String pscoId;


    /**
     * Creates a new ExternalPSCO Parameter.
     *
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param weight Parameter weight.
     * @param pscoId Parameter PSCO Id.
     * @param hashCode Parameter object hashcode.
     * @param monitor object to notify to changes on the parameter
     * @return new ExternalPSCO Parameter.
     */
    public static final ExternalPSCOParameter newEPOP(Application app, Direction direction, StdIOStream stream,
        String prefix, String name, double weight, String pscoId, int hashCode, ParameterMonitor monitor) {
        ExternalPSCObjectAccessParams epoap;
        epoap = ExternalPSCObjectAccessParams.constructEPOAP(app, direction, pscoId, hashCode);
        return new ExternalPSCOParameter(epoap, direction, stream, prefix, name, weight, pscoId, hashCode, monitor);
    }

    private ExternalPSCOParameter(ExternalPSCObjectAccessParams epoap, Direction direction, StdIOStream stream,
        String prefix, String name, double weight, String pscoId, int hashCode, ParameterMonitor monitor) {

        super(epoap, DataType.EXTERNAL_PSCO_T, direction, stream, prefix, name, "null", weight, monitor);
        this.pscoId = pscoId;
        this.hashCode = hashCode;
    }

    public String getId() {
        return this.pscoId;
    }

    public void setId(String pscoId) {
        this.pscoId = pscoId;
    }

    @Override
    public String toString() {
        return "ExternalObjectParameter with Id " + this.pscoId + " and HashCode " + this.hashCode;
    }

    @Override
    public String generateDataTargetName(String tgtName) {
        return getId();
    }
}
