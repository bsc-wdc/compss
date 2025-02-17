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

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ImplementationDescription;

import java.util.ArrayList;
import java.util.List;


/**
 * Class describing a Core Element.
 */
public class CoreElement {

    private final int coreId;
    private final String signature;
    private final List<Implementation> implementations;


    /**
     * Constructs a new Core Element with coreId {@code coreId}.
     *
     * @param coreId Id of the Core Element
     * @param signature Signature of the Core Element
     */
    public CoreElement(int coreId, String signature) {
        this.coreId = coreId;
        this.signature = signature;
        this.implementations = new ArrayList<>();
    }

    /**
     * Returns the Id of the core element.
     *
     * @return Id of the core element.
     */
    public int getCoreId() {
        return this.coreId;
    }

    /**
     * Returns the signature of the core element.
     * 
     * @return The signature of the core element.
     */
    public String getSignature() {
        return this.signature;
    }

    /**
     * Registers the implementation {@code impl} for the core element.
     *
     * @param implDef Implementation to relate to the CoreElement
     * @return {@literal true} if the implementation has been registered; false, otherwise.
     */
    public boolean addImplementation(ImplementationDescription<?, ?> implDef) {
        boolean alreadyExisting = false;
        String implSignature = implDef.getSignature();

        for (Implementation impl : this.implementations) {
            String registeredImplSign = impl.getSignature();
            if (implSignature.compareTo(registeredImplSign) == 0) {
                alreadyExisting = true;
                break;
            }
        }

        if (!alreadyExisting) {
            // Register Implementation
            int implId = this.implementations.size();
            Implementation impl = implDef.getImpl(this.coreId, implId);
            this.implementations.add(impl);
        }

        return alreadyExisting;
    }

    /**
     * Returns the list of implementations related to the core element.
     *
     * @return list of implementations related to the core element.
     */
    public List<Implementation> getImplementations() {
        return this.implementations;
    }

    /**
     * Returns the implementation of the core element with id {@code implId}.
     *
     * @param implId Id of the implementation to return
     * @return the implementation of the core element with id {@code implId}.
     */
    public Implementation getImplementation(int implId) {
        return this.implementations.get(implId);
    }

    /**
     * Returns the number of implementations associated to the Core Element.
     *
     * @return the number of implementations associated to the Core Element.
     */
    public int getImplementationsCount() {
        return this.implementations.size();
    }

    /**
     * Registers a set of implementations and their respective signatures.
     *
     * @param impls Implementations to register
     */
    public void registerImplementations(List<Implementation> impls) {
        this.implementations.addAll(impls);
    }

    /**
     * Returns the signature of the core element implementation with id {@code implId}.
     *
     * @param implId Id of the implementation
     * @return the signature of the core element implementation with id {@code implId}.
     */
    public String getImplementationSignature(int implId) {
        // 1-offset for the non-implementation signature
        return this.implementations.get(implId).getSignature();
    }

}
