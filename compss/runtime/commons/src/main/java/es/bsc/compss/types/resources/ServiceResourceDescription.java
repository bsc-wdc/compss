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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class ServiceResourceDescription extends WorkerResourceDescription {

    private final String serviceName;
    private final String namespace;
    private final String port;

    private int connections;


    /**
     * Creates a new ServiceResourceDescription instance with the given parameters.
     * 
     * @param serviceName Service name.
     * @param namespace Service namespace.
     * @param port Service port.
     * @param connections Service connections.
     */
    public ServiceResourceDescription(String serviceName, String namespace, String port, int connections) {
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.port = port;
        this.connections = connections;
    }

    /**
     * Returns the service name.
     * 
     * @return The service name.
     */
    public String getServiceName() {
        return this.serviceName;
    }

    /**
     * Returns the service namespace.
     * 
     * @return The service namespace.
     */
    public String getNamespace() {
        return this.namespace;
    }

    /**
     * Returns the service port.
     * 
     * @return The service port.
     */
    public String getPort() {
        return this.port;
    }

    @Override
    public boolean canHost(Implementation impl) {
        if (impl.getTaskType() == TaskType.SERVICE) {
            ServiceResourceDescription s = (ServiceResourceDescription) impl.getRequirements();
            return s.serviceName.compareTo(this.serviceName) == 0 && s.namespace.compareTo(this.namespace) == 0
                && s.port.compareTo(this.port) == 0 && s.connections < this.connections;
        }
        return false;
    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        ServiceResourceDescription srd = (ServiceResourceDescription) impl.getRequirements();
        return srd.connections <= this.connections;
    }

    @Override
    public void mimic(ResourceDescription rd) {
        // Do nothing
    }

    @Override
    public void increase(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void reduce(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections -= srd.connections;
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections -= srd.connections;
        return new ServiceResourceDescription("", "", "", srd.connections);
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        ServiceResourceDescription sConstraints = (ServiceResourceDescription) constraints;
        int conCommons = Math.min(sConstraints.connections, this.connections);
        return new ServiceResourceDescription("", "", "", conCommons);
    }

    @Override
    public boolean isDynamicUseless() {
        return connections == 0;
    }

    @Override
    public boolean isDynamicConsuming() {
        return connections > 0;
    }

    @Override
    public String toString() {
        return "[SERVICE " + "NAMESPACE=" + this.namespace + " " + "SERVICE_NAME=" + this.getServiceName() + " "
            + "PORT=" + this.port + " " + "CONNECTIONS=" + this.connections + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to serialize since it is never used
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to serialize since it is never used
    }

    @Override
    public ServiceResourceDescription copy() {
        return new ServiceResourceDescription(serviceName, namespace, port, connections);
    }

    @Override
    public String getDynamicDescription() {
        return "Connections:" + this.connections;
    }

    @Override
    public boolean usesCPUs() {
        return false;
    }

    @Override
    public void scaleUpBy(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("ERROR: Trying to scale by 0 or negative");
        } else if (n > 1) {
            this.connections = this.connections * n;
        }

    }

    @Override
    public void scaleDownBy(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("ERROR: Trying to scale by 0 or negative");
        } else if (n > 1) {
            this.connections = this.connections / n;
        }

    }
}
