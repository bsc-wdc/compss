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
package es.bsc.compss.types;

import java.util.LinkedList;


public class CoreElementDefinition {

    private String ceSignature;
    private final LinkedList<ImplementationDefinition> implementations = new LinkedList<>();


    /**
     * Creates a new CoreElementDefinition instance.
     */
    public CoreElementDefinition() {
    }

    /**
     * Sets a new signature.
     * 
     * @param ceSignature CoreElement signature.
     */
    public void setCeSignature(String ceSignature) {
        this.ceSignature = ceSignature;
    }

    /**
     * Returns the CoreElement signature.
     * 
     * @return The CoreElement signature.
     */
    public String getCeSignature() {
        return ceSignature;
    }

    /**
     * Adds a new implementation to the current CoreElement definition.
     * 
     * @param impl The new CoreElement implementation.
     */
    public void addImplementation(ImplementationDefinition impl) {
        implementations.add(impl);
    }

    /**
     * Returns the registered implementations for the CoreElement.
     * 
     * @return The registered implementations for the CoreElement.
     */
    public LinkedList<ImplementationDefinition> getImplementations() {
        return implementations;
    }

}
