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
package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.resources.ResourceDescription;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CoreManager {

    // LOGGER
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TD_COMP);

    // Constants definition
    private static final String ERROR_INVALID_SIGNATURE = "Invalid signature. Skipping addition";
    private static final String ERROR_UNREGISTERED_CORE_ELEMENT = "Unregistered CoreElement. Skipping addition of ";
    private static final String WARN_UNREGISTERED_IMPL = "Unregistered implementation. Skipping addition";

    // List of core elements
    private static final List<CoreElement> CORE_ELEMENTS = new ArrayList<>();

    // Map for impSignatures to coreElement (several signatures may point to the same coreId since there is versioning)
    private static final Map<String, CoreElement> SIGNATURE_TO_CORE = new LinkedHashMap<>();

    // Structure counters
    private static int coreCount = 0;


    /**
     * Private constructor to avoid instantiation.
     */
    private CoreManager() {
        throw new NonInstantiableException("CoreManager");
    }

    /**
     * Returns the number of registered CoreElements.
     *
     * @return The number of registered CoreElements.
     */
    public static int getCoreCount() {
        return coreCount;
    }

    /**
     * checks whether the core element passed in as parameter is already registered or not.
     * 
     * @param ced Core Element Definition with its implementations.
     * @return {@literal true}, if the core element is already registered with all the implementations; {@literal false}
     *         otherwise.
     */
    public static boolean isRegisteredCoreElement(CoreElementDefinition ced) {
        String ceSignature = ced.getCeSignature();
        // Check that the signature is valid
        if (ceSignature == null || ceSignature.isEmpty()) {
            return false;
        }
        CoreElement coreElement = SIGNATURE_TO_CORE.get(ceSignature);
        if (coreElement == null) {
            return false;
        }
        boolean done = false;
        while (!done) {
            try {
                for (ImplementationDescription<?, ?> implDef : ced.getImplementations()) {
                    String newSignature = implDef.getSignature();
                    boolean exists = false;
                    for (Implementation impl : coreElement.getImplementations()) {
                        String registeredSignature = impl.getSignature();
                        if (registeredSignature.compareTo(newSignature) == 0) {
                            exists = true;
                            break;
                        }
                    }

                    if (!exists) {
                        return false;
                    }
                }
                done = true;
            } catch (ConcurrentModificationException cme) {
                // Do nothing. It will retry
            }
        }
        return true;
    }

    /**
     * Registers or updates a Core Element according to the description passed in as a {@code ced} parameter.
     *
     * @param ced Core Element Definition with its implementations.
     * @return Core Element of the registered definition.
     */
    public static CoreElement registerNewCoreElement(CoreElementDefinition ced) {
        String ceSignature = ced.getCeSignature();
        // Check that the signature is valid
        if (ceSignature == null || ceSignature.isEmpty()) {
            LOGGER.warn(ERROR_INVALID_SIGNATURE + (ceSignature));
            return null;
        }
        LOGGER.debug("Registering New CoreElement " + ced);

        // Check that the signature does not exist
        CoreElement coreElement = SIGNATURE_TO_CORE.get(ceSignature);

        if (coreElement == null) {
            coreElement = insertCoreElement(ceSignature);
        }
        for (ImplementationDescription<?, ?> implDef : ced.getImplementations()) {
            String implSignature = implDef.getSignature();
            if (implSignature != null && !implSignature.isEmpty()) {
                boolean alreadyExisting = coreElement.addImplementation(implDef);
                if (!alreadyExisting) {
                    SIGNATURE_TO_CORE.put(implSignature, coreElement);
                }
            }
        }
        LOGGER.debug("Registered CoreElement " + coreElement.getCoreId());
        return coreElement;
    }

    private static CoreElement insertCoreElement(String signature) {
        // Insert new core element
        Integer methodId = coreCount;
        CoreElement ce = new CoreElement(methodId, signature);

        // Register the cross-reference
        SIGNATURE_TO_CORE.put(signature, ce);

        // Register the coreElement
        CORE_ELEMENTS.add(ce);

        // Increase the coreCount counter
        ++coreCount;

        return ce;
    }

    /**
     * Returns a list with all the registered Core Elements.
     *
     * @return list containing all the registered Core Elements
     */
    public static List<CoreElement> getAllCores() {
        return CORE_ELEMENTS;
    }

    /**
     * Returns the Core associated to a registered signature. If the requested signature has not been previously
     * registered raises an ErrorManager ERROR and stops the execution.
     *
     * @param signature Method signature.
     * @return Core Element associated to the signature.
     */
    public static CoreElement getCore(String signature) {
        // Check that the signature is valid
        if (signature == null || signature.isEmpty()) {
            ErrorManager.error(ERROR_INVALID_SIGNATURE);
            return null;
        }

        // Check that the signature does not exist
        CoreElement ce = SIGNATURE_TO_CORE.get(signature);
        if (ce == null) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT + signature);
            return null;
        }

        return ce;
    }

    /**
     * Returns the Core with coreId @{code coreId}. If the requested coreId is not related to any Core element, it
     * registered raises an ErrorManager ERROR and stops the execution.
     *
     * @param coreId Id of the core to retrieve.
     * @return Core Element with id {@code coreId}.
     */
    public static CoreElement getCore(int coreId) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT + coreId);
            return null;
        }

        return CORE_ELEMENTS.get(coreId);
    }

    /**
     * Returns the signature of a given implementationId of a give coreElementId. If there is no {@code coreId} -
     * {@code implId} registered, raises an ErrorManager WARN.
     *
     * @param coreId Core Id.
     * @param implId Implementation Id.
     * @return The signature of the requested CoreElement.
     */
    public static String getSignature(int coreId, int implId) {
        if (coreId < 0 || coreId >= coreCount) {
            LOGGER.warn(ERROR_UNREGISTERED_CORE_ELEMENT);
            return null;
        }

        CoreElement ce = CORE_ELEMENTS.get(coreId);
        if (implId < 0 || implId >= ce.getImplementationsCount()) {
            LOGGER.warn(WARN_UNREGISTERED_IMPL);
            return null;
        }
        return ce.getImplementationSignature(implId);
    }

    /**
     * Gets the map of registered signatures their corresponding core elements.
     *
     * @return The map of registered signatures and the corresponding core elements.
     */
    public static Map<String, CoreElement> getSignaturesToCores() {
        return SIGNATURE_TO_CORE;
    }

    /**
     * Gets the map of registered signatures and coreIds.
     *
     * @return The map of registered signatures and coreIds.
     */
    public static Map<String, Integer> getSignaturesToCeAndImpls() {
        HashMap<String, Integer> result = new HashMap<>();
        for (Entry<String, CoreElement> ceEntry : SIGNATURE_TO_CORE.entrySet()) {
            result.put(ceEntry.getKey(), ceEntry.getValue().getCoreId());
        }
        return result;
    }

    /**
     * Returns a map from the CE signature to the CE id.
     *
     * @return The map of registered signatures and coreIds.
     */
    public static Map<String, Integer> getSignaturesToCEIds() {
        HashMap<String, Integer> result = new HashMap<>();
        for (CoreElement ce : CORE_ELEMENTS) {
            result.put(ce.getSignature(), ce.getCoreId());
        }
        return result;
    }

    /**
     * Clears the internal structures.
     */
    public static void clear() {
        CORE_ELEMENTS.clear();
        SIGNATURE_TO_CORE.clear();
        coreCount = 0;
    }

    /*
     * *****************************************************************************************************
     * *****************************************************************************************************
     * ************** QUERY OPERATIONS *********************************************************************
     * *****************************************************************************************************
     * *****************************************************************************************************
     */
    /**
     * Returns all the implementations of a CoreElement.
     *
     * @param coreId Core Id.
     * @return The implementations for a CoreElement.
     */
    public static List<Implementation> getCoreImplementations(int coreId) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT);
            return null;
        }

        return CORE_ELEMENTS.get(coreId).getImplementations();
    }

    /**
     * Returns the number of implementations of a CoreElement.
     *
     * @param coreId Core Id.
     * @return the number of implementations of a CoreElement.
     */
    public static int getNumberCoreImplementations(int coreId) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT);
            return -1;
        }

        return CORE_ELEMENTS.get(coreId).getImplementationsCount();
    }

    /**
     * Looks for all the cores in the annotated Interface whose constraints are fulfilled by the resource description
     * {@code rd}.
     *
     * @param rd ResourceDescription to find cores compatible with.
     * @return List of cores whose constraints are compatible with the given resource.
     */
    public static List<CoreElement> findExecutableCores(ResourceDescription rd) {
        List<CoreElement> executableList = new LinkedList<>();

        for (CoreElement ce : CORE_ELEMENTS) {
            for (Implementation impl : ce.getImplementations()) {
                LOGGER.debug("******* Executing Can Host ");
                if (rd.canHost(impl)) {
                    // Add core to executable list
                    executableList.add(ce);
                    // Break the search to next core
                    break;
                }
            }
        }

        return executableList;
    }

    /*
     * *****************************************************************************************************
     * *****************************************************************************************************
     * ************** DEBUG OPERATIONS *********************************************************************
     * *****************************************************************************************************
     * *****************************************************************************************************
     */
    /**
     * Returns a debug string describing the currently registered cores.
     *
     * @return A string containing all the information about the registered cores.
     */
    public static String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Core Count: ").append(coreCount).append("\n");
        for (CoreElement ce : CORE_ELEMENTS) {
            sb.append("\tCore ").append(ce.getCoreId()).append(":\n");
            for (Implementation impl : ce.getImplementations()) {
                sb.append("\t\t -").append(impl.toString()).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Returns a debug string describing the signatures of the currently registered cores.
     *
     * @return A string containing all the signatures of the registered cores.
     */
    public static String debugSignaturesString() {
        StringBuilder sb = new StringBuilder();
        sb.append("REGISTERED SIGNATURES: \n");
        for (Entry<String, CoreElement> entry : SIGNATURE_TO_CORE.entrySet()) {
            sb.append("Signature: ").append(entry.getKey());
            sb.append(" with MethodId ").append(entry.getValue().getCoreId());
            sb.append("\n");
        }

        return sb.toString();
    }

}
