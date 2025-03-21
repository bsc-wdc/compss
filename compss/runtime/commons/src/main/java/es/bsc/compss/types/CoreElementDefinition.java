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
package es.bsc.compss.types;

import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.resources.components.Processor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class CoreElementDefinition implements Comparable<CoreElementDefinition> {

    private String ceSignature;
    private final List<ImplementationDescription<?, ?>> implementations;


    /**
     * Creates a new CoreElementDefinition instance.
     */
    public CoreElementDefinition() {
        this.implementations = new LinkedList<>();
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
        return this.ceSignature;
    }

    /**
     * Adds a new implementation to the current CoreElement definition.
     * 
     * @param impl The new CoreElement implementation.
     */
    public void addImplementation(ImplementationDescription<?, ?> impl) {
        this.implementations.add(impl);
    }

    /**
     * Returns the registered implementations for the CoreElement.
     * 
     * @return The registered implementations for the CoreElement.
     */
    public List<ImplementationDescription<?, ?>> getImplementations() {
        return this.implementations;
    }

    @Override
    public int compareTo(CoreElementDefinition ced) {
        return this.ceSignature.compareTo(ced.ceSignature);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        if (this.ceSignature == null) {
            sb.append("\"ce_signature\":null,");
        } else {
            sb.append("\"ce_signature\":\"").append(this.ceSignature).append("\",");
        }
        sb.append("\"implementations\":[");
        Iterator<ImplementationDescription<?, ?>> implsItr = this.implementations.iterator();
        ImplementationDescription<?, ?> impl;
        if (implsItr.hasNext()) {
            impl = implsItr.next();
            sb.append(impl.toJSON());
        }
        while (implsItr.hasNext()) {
            sb.append(",");
            impl = implsItr.next();
            sb.append(impl.toJSON());
        }
        sb.append("]}");
        return sb.toString();
    }
}
