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
package es.bsc.compss.agent.rest.types.messages;

import es.bsc.compss.agent.rest.types.ActionTrigger;
import es.bsc.compss.agent.rest.types.ApplicationParameterImpl;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ArrayParameter;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ElementParameter;
import es.bsc.compss.agent.rest.types.OrchestratorNotification;
import es.bsc.compss.agent.rest.types.RESTAgentRequestListener;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.implementations.ExecType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;


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
    private String lang;
    private String ceiClass;
    private String className;
    private String methodName;
    private ApplicationParameterImpl[] params = new ApplicationParameterImpl[0];
    private ApplicationParameterImpl target;
    private boolean hasResult;
    private RESTAgentRequestListener requestListener;
    private ExecType prolog;
    private ExecType epilog;


    public StartApplicationRequest() {
        // Nothing to do
    }

    public void setServiceInstanceId(String serviceInstanceId) {
        this.serviceInstanceId = serviceInstanceId;
    }

    public String getServiceInstanceId() {
        return this.serviceInstanceId;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    /**
     * Getter for the language of the request. If the language is not definied by default assigns Java.
     *
     * @return language of the request
     */
    public String getLang() {
        String returnLang = lang;
        if (returnLang == null) {
            returnLang = "JAVA";
        }
        return returnLang;
    }

    public String getCeiClass() {
        return this.ceiClass;
    }

    public void setCeiClass(String ceiClass) {
        this.ceiClass = ceiClass;
    }

    public String getClassName() {
        return this.className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return this.methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ApplicationParameterImpl getTarget() {
        return this.target;
    }

    public void setTarget(ApplicationParameterImpl target) {
        this.target = target;
    }

    public ExecType getProlog() {
        return prolog;
    }

    public void setProlog(ExecType prolog) {
        this.prolog = prolog;
    }

    public ExecType getEpilog() {
        return epilog;
    }

    public void setEpilog(ExecType epilog) {
        this.epilog = epilog;
    }

    public void addParameter(String name, String prefix, boolean value) {
        addParameter(value, Direction.IN, DataType.BOOLEAN_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, byte value) {
        addParameter(value, Direction.IN, DataType.BYTE_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, char value) {
        addParameter(value, Direction.IN, DataType.CHAR_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, short value) {
        addParameter(value, Direction.IN, DataType.SHORT_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, int value) {
        addParameter(value, Direction.IN, DataType.INT_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, long value) {
        addParameter(value, Direction.IN, DataType.LONG_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, float value) {
        addParameter(value, Direction.IN, DataType.FLOAT_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String name, String prefix, double value) {
        addParameter(value, Direction.IN, DataType.DOUBLE_T, StdIOStream.UNSPECIFIED, prefix, name, "", 1.0, false);
    }

    public void addParameter(String value) {
        addParameter(value, Direction.IN, DataType.STRING_T, StdIOStream.UNSPECIFIED, "", "", "", 1.0, false);
    }

    public void addParameter(Object value) {
        addParameter(Direction.IN, value);
    }

    public void addParameter(Direction direction, Object value) {
        addParameter(value, Direction.IN, DataType.OBJECT_T, StdIOStream.UNSPECIFIED, "", "", "", 1.0, false);
    }

    /**
     * Constructs a new Parameter for the request and appends it to the current existing ones.
     * 
     * @param value value of the parameter
     * @param direction directionality of the parameter
     * @param type type of the data of the parameter
     * @param stream stream redirection
     * @param prefix prefix to add to the parameter on execution
     * @param name name of the parameter
     * @param contentType type of element within the parameter value
     * @param weight scheduling weight
     * @param keepRename should keep rename on execution
     * @return constructed parameter
     */
    public ApplicationParameterImpl addParameter(Object value, Direction direction, DataType type, StdIOStream stream,
        String prefix, String name, String contentType, double weight, boolean keepRename) {

        ApplicationParameterImpl p;
        p = new ApplicationParameterImpl(value, direction, type, stream, prefix, name, contentType, weight, keepRename);
        p.setParamId(this.params.length);

        ApplicationParameterImpl[] oldParams = this.params;
        this.params = new ApplicationParameterImpl[oldParams.length + 1];
        if (oldParams.length > 0) {
            System.arraycopy(oldParams, 0, this.params, 0, oldParams.length);
        }
        this.params[oldParams.length] = p;
        return p;
    }

    public void addPersistedParameter(String id) {
        addPersistedParameter(Direction.IN, id);
    }

    /**
     * Add a Persistent parameter.
     *
     * @param direction parameter direction
     * @param id parameter identifier
     */
    public void addPersistedParameter(Direction direction, String id) {
        ApplicationParameterImpl p =
            addParameter(id, direction, DataType.PSCO_T, StdIOStream.UNSPECIFIED, "", "", "", 1.0, false);
        ((ElementParameter) p.getValue()).setClassName("storage.StubItf");
    }

    @XmlElementWrapper(name = "parameters")
    public ApplicationParameterImpl[] getParams() {
        return this.params;
    }

    public void setParams(ApplicationParameterImpl[] params) {
        this.params = params;
    }

    public void setHasResult(boolean hasResult) {
        this.hasResult = hasResult;
    }

    public boolean isHasResult() {
        return this.hasResult;
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

    public void setOrchestratorNotification(String host, OrchestratorNotification.HttpMethod method, String operation) {
        this.requestListener = new OrchestratorNotification(host, method, operation);
    }

    @XmlElements({ @XmlElement(name = "action", type = ActionTrigger.class, required = false),
        @XmlElement(name = "orchestrator", type = OrchestratorNotification.class, required = false), })
    public RESTAgentRequestListener getRequestListener() {
        return this.requestListener;
    }

    public void setRequestListener(RESTAgentRequestListener requestListener) {
        this.requestListener = requestListener;
    }

}
