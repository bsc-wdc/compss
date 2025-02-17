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
package es.bsc.compss.agent.rest.types;

import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;


/**
 * This class contains all the necessary information to transfer a parameter through the REST Agent interface.
 */
public class ApplicationParameterImpl implements ApplicationParameter {

    private int paramId;
    private ApplicationParameterValue value;
    private Direction direction;
    private DataType type;
    private StdIOStream stdIOStream;
    private String prefix;
    private String paramName;
    private String contentType;
    private double weight;
    private boolean keepRename;


    public ApplicationParameterImpl() {
        weight = 1.0D;
    }

    /**
     * Constructs an ApplicationParameterImpl setting up all its characteristics.
     *
     * @param val Actual value of the parameter
     * @param dir directionality of the parameter
     * @param type type of data of the parameter
     * @param stream stream to redirect to the parameter
     * @param prefix prefix to attach to the parameter
     * @param paramName name of the parameter
     */
    public ApplicationParameterImpl(Object val, Direction dir, DataType type, StdIOStream stream, String prefix,
        String paramName, String contentType, double weight, boolean keepRename) {
        this.value = ApplicationParameterValue.createParameterValue(val);
        this.direction = dir;
        this.stdIOStream = stream;
        this.type = type;
        this.prefix = prefix;
        this.paramName = paramName;
        this.contentType = contentType;
        this.weight = weight;
        this.keepRename = keepRename;
    }

    @XmlAttribute
    public int getParamId() {
        return paramId;
    }

    public void setParamId(int paramId) {
        this.paramId = paramId;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    @Override
    public StdIOStream getStdIOStream() {
        return stdIOStream;
    }

    public void setStdIOStream(StdIOStream stream) {
        this.stdIOStream = stream;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getPrefix() {
        return this.prefix;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    @Override
    public String getParamName() {
        return this.paramName;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String getContentType() {
        return this.contentType;
    }

    @XmlElements({ @XmlElement(name = "array", type = ApplicationParameterValue.ArrayParameter.class, required = false),
        @XmlElement(name = "element", type = ApplicationParameterValue.ElementParameter.class, required = false), })
    public ApplicationParameterValue getValue() {
        return value;
    }

    public void setValue(ApplicationParameterValue value) {
        this.value = value;
    }

    @Override
    public Object getValueContent() throws Exception {
        return value.getContent();
    }

    @Override
    public RemoteDataInformation getRemoteData() {
        // Cannot be obtained from a remote location
        return null;
    }

    @Override
    public String getDataMgmtId() {
        return null;
    }

    @Override
    public boolean isKeepRename() {
        return this.keepRename;
    }

    @Override
    public double getWeight() {
        return this.weight;
    }

}
