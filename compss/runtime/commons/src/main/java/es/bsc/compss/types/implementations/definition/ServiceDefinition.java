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

package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.resources.ServiceResourceDescription;


/**
 * Class containing all the necessary information to generate a service implementation of a CE.
 */
public class ServiceDefinition extends ImplementationDefinition<ServiceResourceDescription> {

    private final String namespace;
    private final String serviceName;
    private final String operation;
    private final String port;

    protected ServiceDefinition(String signature, String namespace, String serviceName, String operation,
            String port) {
        super(signature, null);
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.operation = operation;
        this.port = port;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new ServiceImplementation(coreId, namespace, serviceName, port, operation);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SERVICE Implementation \n");
        sb.append("\t Signature: ").append(this.getSignature()).append("\n");
        sb.append("\t Namespace: ").append(namespace).append("\n");
        sb.append("\t Service name: ").append(serviceName).append("\n");
        sb.append("\t Operation: ").append(operation).append("\n");
        sb.append("\t Port: ").append(port).append("\n");
        return sb.toString();
    }
}
