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

package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Class containing all the necessary information to generate a service implementation of a CE.
 */
public class ServiceDefinition implements ImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 4;

    private String namespace;
    private String serviceName;
    private String operation;
    private String port;


    public ServiceDefinition() {
        // For serialization
    }

    /**
     * Creates a new ServiceDefinition to create a service core element implementation.
     * 
     * @param namespace Service namespace.
     * @param serviceName Service name.
     * @param port Service port.
     * @param operation Service operation.
     */
    public ServiceDefinition(String namespace, String serviceName, String operation, String port) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.operation = operation;
        this.port = port;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public ServiceDefinition(String[] implTypeArgs, int offset) {
        this.namespace = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.operation = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.port = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SERVICE Definition \n");
        sb.append("\t Namespace: ").append(namespace).append("\n");
        sb.append("\t Service name: ").append(serviceName).append("\n");
        sb.append("\t Operation: ").append(operation).append("\n");
        sb.append("\t Port: ").append(port).append("\n");
        return sb.toString();
    }

    public TaskType getTaskType() {
        return TaskType.SERVICE;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.namespace = (String) in.readObject();
        this.serviceName = (String) in.readObject();
        this.operation = (String) in.readObject();
        this.port = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.namespace);
        out.writeObject(this.serviceName);
        out.writeObject(this.operation);
        out.writeObject(this.port);
    }

    @Override
    public String toShortFormat() {
        return " Service in namespace " + this.namespace + " with name " + this.serviceName + " on port " + this.port
            + "and operation " + this.operation;
    }

    public String getOperation() {
        return operation;
    }

}
