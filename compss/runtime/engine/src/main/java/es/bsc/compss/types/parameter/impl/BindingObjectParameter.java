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
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.BindingObjectAccessParams;
import es.bsc.compss.types.data.params.BindingObjectData;


public class BindingObjectParameter
    extends ObjectParameter<BindingObject, BindingObjectAccessParams, BindingObjectData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new BindingObjectParameter Parameter.
     * 
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param weight Parameter weight.
     * @param bo Parameter binding object.
     * @param hashCode Parameter object hashcode.
     * @param monitor object to notify to changes on the parameter
     * @return new BindingObjectParameter instance
     */
    public static final BindingObjectParameter newBOP(Application app, Direction direction, StdIOStream stream,
        String prefix, String name, String contentType, double weight, BindingObject bo, int hashCode,
        ParameterMonitor monitor) {
        BindingObjectAccessParams boap = BindingObjectAccessParams.constructBOAP(app, direction, bo, hashCode);
        return new BindingObjectParameter(boap, direction, stream, prefix, name, contentType, weight, monitor);
    }

    private BindingObjectParameter(BindingObjectAccessParams boap, Direction direction, StdIOStream stream,
        String prefix, String name, String contentType, double weight, ParameterMonitor monitor) {

        super(boap, DataType.BINDING_OBJECT_T, direction, stream, prefix, name, contentType, weight, monitor);
    }

    public String getId() {
        return this.getAccess().getBindingObject().toString();
    }

    @Override
    public String getOriginalName() {
        return this.getAccess().getBindingObject().getId();
    }

    @Override
    public String toString() {
        BindingObject bo = this.getBindingObject();
        return "BindingObjectParameter with Id " + bo.getId() + ", type " + bo.getType() + ", elements "
            + bo.getElements() + " and HashCode " + this.getCode();
    }

    @Override
    public String getDataTarget() {
        BindingObject bo = this.getBindingObject();
        String dataTarget = super.getDataTarget();
        if (dataTarget != null) {
            if (dataTarget.contains("#")) {
                return dataTarget;
            } else {
                return dataTarget + "#" + bo.getType() + "#" + bo.getElements();
            }
        } else {
            return "null#" + bo.getType() + "#" + bo.getElements();
        }
    }

    public BindingObject getBindingObject() {
        return this.getAccess().getBindingObject();
    }

    @Override
    public String generateDataTargetName(String tgtName) {
        BindingObject bo = this.getBindingObject();
        if (!tgtName.contains("#")) {
            tgtName = tgtName + "#" + bo.getType() + "#" + bo.getElements();
        }
        return tgtName;
    }
}
