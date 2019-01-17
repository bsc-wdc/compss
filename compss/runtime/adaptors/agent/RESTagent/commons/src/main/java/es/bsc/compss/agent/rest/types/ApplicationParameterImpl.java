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
package es.bsc.compss.agent.rest.types;

import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;


public class ApplicationParameterImpl implements ApplicationParameter {

    private int paramId;
    private ApplicationParameterValue value;
    private Direction direction;
    private DataType type;
    private Stream stream;

    public ApplicationParameterImpl() {

    }

    public ApplicationParameterImpl(Object val, Direction dir, DataType type, Stream stream) {
        this.value = ApplicationParameterValue.createParameterValue(val);
        this.direction = dir;
        this.stream = stream;
        this.type = type;
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

    public Stream getStream() {
        return stream;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    @XmlElements({
        @XmlElement(name = "array", type = ApplicationParameterValue.ArrayParameter.class, required = false),
        @XmlElement(name = "element", type = ApplicationParameterValue.ElementParameter.class, required = false),})
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
}
