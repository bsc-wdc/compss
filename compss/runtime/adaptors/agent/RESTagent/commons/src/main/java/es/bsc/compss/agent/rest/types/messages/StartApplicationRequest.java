/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.rest.types.messages;

import es.bsc.compss.agent.rest.types.ApplicationParameterImpl;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ArrayParameter;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ElementParameter;
import es.bsc.compss.agent.rest.types.Orchestrator;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.parameter.Parameter;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * This class contains all the information required to start an execution on a remote agent through the REST Agent
 * interface.
 */
@XmlRootElement(name = "startApplication")
public class StartApplicationRequest implements Serializable {

    /**
     * OBjects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private String serviceInstanceId;
    private String ceiClass;
    private String className;
    private String methodName;
    private ApplicationParameterImpl[] params = new ApplicationParameterImpl[0];
    private ApplicationParameterImpl target;
    private boolean hasResult;
    private Orchestrator orchestrator;

    public StartApplicationRequest() {

    }

    public void setServiceInstanceId(String serviceInstanceId) {
        this.serviceInstanceId = serviceInstanceId;
    }

    public String getServiceInstanceId() {
        return serviceInstanceId;
    }

    public String getCeiClass() {
        return ceiClass;
    }

    public void setCeiClass(String ceiClass) {
        this.ceiClass = ceiClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ApplicationParameterImpl getTarget() {
        return target;
    }

    public void setTarget(ApplicationParameterImpl target) {
        this.target = target;
    }

    public void addParameter(String name, String prefix, boolean value) {
        addParameter(value, Direction.IN, DataType.BOOLEAN_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, byte value) {
        addParameter(value, Direction.IN, DataType.BYTE_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, char value) {
        addParameter(value, Direction.IN, DataType.CHAR_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, short value) {
        addParameter(value, Direction.IN, DataType.SHORT_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, int value) {
        addParameter(value, Direction.IN, DataType.INT_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, long value) {
        addParameter(value, Direction.IN, DataType.LONG_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, float value) {
        addParameter(value, Direction.IN, DataType.FLOAT_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String name, String prefix, double value) {
        addParameter(value, Direction.IN, DataType.DOUBLE_T, StdIOStream.UNSPECIFIED, prefix, name);
    }

    public void addParameter(String value) {
        addParameter(value, Direction.IN, DataType.STRING_T, StdIOStream.UNSPECIFIED, "", "");
    }

    public void addParameter(Object value) {
        addParameter(Direction.IN, value);
    }

    public void addParameter(Direction direction, Object value) {
        addParameter(value, Direction.IN, DataType.OBJECT_T, StdIOStream.UNSPECIFIED, "", "");
    }

    public void addParameter(Parameter p, Object value) {
        addParameter(value, p.getDirection(), p.getType(), p.getStream(), p.getPrefix(), p.getName());
    }

    private ApplicationParameterImpl addParameter(Object value, Direction direction, DataType type, StdIOStream stream,
            String prefix, String name) {
        ApplicationParameterImpl p;
        p = new ApplicationParameterImpl(value, direction, type, stream, prefix, name);
        p.setParamId(params.length);

        ApplicationParameterImpl[] oldParams = params;
        params = new ApplicationParameterImpl[oldParams.length + 1];
        if (oldParams.length > 0) {
            System.arraycopy(oldParams, 0, params, 0, oldParams.length);
        }
        params[oldParams.length] = p;
        return p;
    }

    public void addPersistedParameter(String id) {
        addPersistedParameter(Direction.IN, id);
    }

    public void addPersistedParameter(Direction direction, String id) {
        ApplicationParameterImpl p = addParameter(id, direction, DataType.PSCO_T, StdIOStream.UNSPECIFIED, "", "");
        ((ElementParameter) p.getValue()).setClassName("storage.StubItf");
    }

    @XmlElementWrapper(name = "parameters")
    public ApplicationParameterImpl[] getParams() {
        return params;
    }

    public void setParams(ApplicationParameterImpl[] params) {
        this.params = params;
    }

    public void setHasResult(boolean hasResult) {
        this.hasResult = hasResult;
    }

    public boolean isHasResult() {
        return hasResult;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StartApplication ").append(className).append(".").append(methodName).append("(");

        int count = 0;
        for (ApplicationParameterImpl param : this.params) {
            if (count > 0) {
                sb.append(", ");
            }
            count++;
            if (param.getValue() instanceof ArrayParameter) {
                sb.append(param.getType());
            } else {
                sb.append(param.getType());
            }
        }
        sb.append(") defined in CEI ").append(ceiClass);
        return sb.toString();
    }

    public void setOrchestrator(String host, Orchestrator.HttpMethod method, String operation) {
        this.orchestrator = new Orchestrator(host, method, operation);
    }

    public void setOrchestrator(Orchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public Orchestrator getOrchestrator() {
        return orchestrator;
    }

}
