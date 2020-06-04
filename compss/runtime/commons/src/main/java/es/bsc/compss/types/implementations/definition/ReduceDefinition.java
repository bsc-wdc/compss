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
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.ReduceImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;


/**
 * Class containing all the necessary information to generate a reduce implementation of a CE.
 */
public class ReduceDefinition extends ImplementationDefinition<MethodResourceDescription> {

    private final String reduceClass;
    private final String reduceName;


    protected ReduceDefinition(String signature, String reduceClass, String reduceName,
        MethodResourceDescription implConstraints) {
        super(signature, implConstraints);
        this.reduceClass = reduceClass;
        this.reduceName = reduceName;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new ReduceImplementation(reduceClass, reduceName, coreId, implId, this.getSignature(),
            this.getConstraints());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reduce Implementation \n");
        sb.append("\t Signature: ").append(this.getSignature()).append("\n");
        sb.append("\t IO: ").append(!this.getConstraints().usesCPUs()).append("\n");
        sb.append("\t Class: ").append(reduceClass).append("\n");
        sb.append("\t Name: ").append(reduceName).append("\n");
        sb.append("\t Constraints: ").append(this.getConstraints());
        return sb.toString();
    }
}
