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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class HTTPDefinition implements ImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 8;

    private String serviceName;
    private String resource;
    private String request;
    private String payload;
    private String payloadType;
    private String produces;
    private String updates;

    private String defReturn;


    public HTTPDefinition() {
        // For serialization
    }

    /**
     * Creates a new HTTPDefinition to create an HTTP core element implementation.
     *
     * @param request HTTP request type.
     * @param resource HTTP resource in the URL .
     */
    public HTTPDefinition(String serviceName, String resource, String request, String payload, String payloadType,
        String produces, String updates, String defReturn) {
        this.serviceName = serviceName;
        this.resource = resource;
        this.request = request;
        this.payload = payload;
        this.payloadType = payloadType;
        this.produces = produces;
        this.updates = updates;
        this.defReturn = defReturn;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public HTTPDefinition(String[] implTypeArgs, int offset) {
        this.serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.resource = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.request = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.payload = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.payloadType = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.produces = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 5]);
        this.updates = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 6]);
        this.defReturn = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 7]);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getRequest() {
        return request;
    }

    public String getResource() {
        return resource;
    }

    public String getPayload() {
        return payload;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public String getProduces() {
        return produces;
    }

    public String getUpdates() {
        return updates;
    }

    public String getDefReturn() {
        return defReturn;
    }

    @Override
    public String toString() {
        return "HTTP Definition \n" + "\t ServiceName: " + serviceName + "\n" + "\t HTTP resource: " + resource + "\n";
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }

    @Override
    public String toJSON() {
        return "{\"service_name\":\"" + serviceName + "\"," + "\"resource\":\"" + this.resource + "\","
            + "\"request\":\"" + this.request + "\"" + "}";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.request = (String) in.readObject();
        this.resource = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.request);
        out.writeObject(this.resource);
    }

    @Override
    public String toShortFormat() {
        return " HTTP method type: " + this.request + " with base URL: " + this.resource;
    }
}
