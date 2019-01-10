/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ImplementationDefinition;
import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.Iterator;

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
    private static final String ERROR_INVALID_IMPLS_SIGNS = "Attempting to register a different number of implementations and signatures. Skipping addition";
    private static final String WARN_REGISTERED_CORE_ELEMENT = "Already registered CoreElement. Skipping addition of ";
    private static final String WARN_UNREGISTERED_IMPL = "Unregistered implementation. Skipping addition";

    // List of implementations of each coreElement
    private static final List<List<Implementation>> IMPLEMENTATIONS = new LinkedList<>();
    // List of signatures of each implementation of each coreElement
    // The first signature is always the signature of the coreElement
    private static final List<List<String>> SIGNATURES = new LinkedList<>();
    // Map for signatures to coreElementId (several signatures may point to the same coreId since there is versioning)
    private static final Map<String, Integer> SIGNATURE_TO_CORE_ID = new LinkedHashMap<>();

    // Structure counters
    private static int coreCount = 0;

    /**
     * Private constructor to avoid instantiation
     *
     */
    private CoreManager() {
        throw new NonInstantiableException("CoreManager");
    }

    /**
     * Returns the number of registered CoreElements
     *
     * @return
     */
    public static int getCoreCount() {
        return coreCount;
    }

    /**
     * Registers or updates a Core Element according to the description passed in as a {@code ced} parameter.
     *
     * @param ced Core Element Definition with its implementations
     *
     * @return coreId assigned to the registered Core Element
     */
    public static Integer registerNewCoreElement(CoreElementDefinition ced) {
        String ceSignature = ced.getCeSignature();

        // Check that the signature is valid
        if (ceSignature == null || ceSignature.isEmpty()) {
            LOGGER.warn(ERROR_INVALID_SIGNATURE);
            return null;
        }

        // Check that the signature does not exist
        Integer coreId = SIGNATURE_TO_CORE_ID.get(ceSignature);
        if (coreId == null) {
            coreId = insertCoreElement(ceSignature);
        }

        List<Implementation> coreImplementations = IMPLEMENTATIONS.get(coreId);
        List<String> coreSignatures = SIGNATURES.get(coreId);

        for (ImplementationDefinition implDef : ced.getImplementations()) {

            String implSignature = implDef.getSignature();
            if (implSignature != null && !implSignature.isEmpty()) {

                //Check whether the implementation is already registered
                boolean alreadyExisting = false;
                Iterator<String> it = coreSignatures.iterator();
                it.next();//Skip method signature with no implementation
                while (it.hasNext()) {
                    String registeredImplSign = it.next();
                    if (implSignature.compareTo(registeredImplSign) == 0) {
                        alreadyExisting = true;
                        break;
                    }
                }

                if (!alreadyExisting) {
                    //Register Implementation
                    int implId = coreImplementations.size();
                    Implementation impl = implDef.getImpl(coreId, implId);
                    coreImplementations.add(impl);
                    coreSignatures.add(implSignature);
                    SIGNATURE_TO_CORE_ID.put(implSignature, coreId);
                }
            }
        }
        return coreId;
    }

    /**
     * Registers a new Method as Core Element if it doesn't exist
     *
     * @param signature
     *
     * @return the methodId assigned to the new Core Element
     */
    public static Integer registerNewCoreElement(String signature) {
        // Check that the signature is valid
        if (signature == null || signature.isEmpty()) {
            LOGGER.warn(ERROR_INVALID_SIGNATURE);
            return null;
        }

        // Check that the signature does not exist
        Integer methodId = SIGNATURE_TO_CORE_ID.get(signature);
        if (methodId != null) {
            LOGGER.warn(WARN_REGISTERED_CORE_ELEMENT + signature);
            return null;
        }

        // Insert new core element
        methodId = insertCoreElement(signature);

        return methodId;
    }

    private static Integer insertCoreElement(String signature) {
        // Insert new core element
        Integer methodId = coreCount;
        // Increase the coreCount counter
        ++coreCount;
        // Register the coreElement
        List<Implementation> impls = new LinkedList<>();
        IMPLEMENTATIONS.add(impls);
        // Register the signature
        List<String> signs = new LinkedList<>();
        signs.add(signature);
        SIGNATURES.add(signs);
        // Register the cross-reference
        SIGNATURE_TO_CORE_ID.put(signature, methodId);

        return methodId;
    }

    /**
     * Registers a new Implementation for a given CoreElement The coreElement MUST have been previously registered The
     * impls and signs must have the same size and are sorted
     *
     * @param coreId
     * @param impls
     * @param signs
     */
    public static void registerNewImplementations(int coreId, List<Implementation> impls, List<String> signs) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT + coreId);
            return;
        }
        if (impls.size() != signs.size()) {
            ErrorManager.error(ERROR_INVALID_IMPLS_SIGNS);
            return;
        }
        for (String signature : signs) {
            if (signature == null || signature.isEmpty()) {
                ErrorManager.error(ERROR_INVALID_SIGNATURE);
                return;
            }
        }

        // Register all the new implementations
        List<Implementation> coreImplementations = IMPLEMENTATIONS.get(coreId);
        coreImplementations.addAll(impls);
        IMPLEMENTATIONS.set(coreId, coreImplementations);

        // Register all the new signatures
        List<String> coreSignatures = SIGNATURES.get(coreId);
        coreSignatures.addAll(signs);
        SIGNATURES.set(coreId, coreSignatures);

        // Update the cross-reference
        for (String signature : signs) {
            SIGNATURE_TO_CORE_ID.put(signature, coreId);
        }
    }

    /**
     * Returns the CoreId associated to a registered signature The coreId MUST have been previously registered
     *
     * @param signature
     * @return
     */
    public static Integer getCoreId(String signature) {
        // Check that the signature is valid
        if (signature == null || signature.isEmpty()) {
            ErrorManager.error(ERROR_INVALID_SIGNATURE);
            return null;
        }

        // Check that the signature does not exist
        Integer methodId = SIGNATURE_TO_CORE_ID.get(signature);
        if (methodId == null) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT + signature);
            return null;
        }

        return methodId;
    }

    /**
     * Returns the signature of a given implementationId of a give coreElementId The coreId MUST have been previously
     * registered The implId MUST have been previously registered
     *
     * @param coreId
     * @param implId
     * @return
     */
    public static String getSignature(int coreId, int implId) {
        if (coreId < 0 || coreId >= coreCount) {
            LOGGER.warn(ERROR_UNREGISTERED_CORE_ELEMENT);
            return null;
        }
        List<String> coreSignatures = SIGNATURES.get(coreId);
        int implSignaturePosition = implId + 1;
        if (implSignaturePosition < 0 || implSignaturePosition >= coreSignatures.size()) {
            LOGGER.warn(WARN_UNREGISTERED_IMPL);
            return null;
        }

        return coreSignatures.get(implSignaturePosition);
    }

    /**
     * Gets the map of registered signatures and coreIds
     *
     * @return the map of registered signatures and coreIds
     */
    public static Map<String, Integer> getSignaturesToId() {
        return SIGNATURE_TO_CORE_ID;
    }

    /**
     * Clears the internal structures
     */
    public static void clear() {
        IMPLEMENTATIONS.clear();
        SIGNATURES.clear();
        SIGNATURE_TO_CORE_ID.clear();
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
     * Returns all the implementations of a core Element
     *
     * @param coreId
     * @return the implementations for a Core Element
     */
    public static List<Implementation> getCoreImplementations(int coreId) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT);
            return null;
        }

        return IMPLEMENTATIONS.get(coreId);
    }

    /**
     * Returns the number of implementations of a Core Element
     *
     * @param coreId
     * @return the number of implementations of a Core Element
     */
    public static int getNumberCoreImplementations(int coreId) {
        if (coreId < 0 || coreId >= coreCount) {
            ErrorManager.error(ERROR_UNREGISTERED_CORE_ELEMENT);
            return -1;
        }

        return IMPLEMENTATIONS.get(coreId).size();
    }

    /**
     * Looks for all the cores from in the annotated Interface which constraint are fullfilled by the resource
     * description passed as a parameter
     *
     * @param rd ResourceDescription to find cores compatible to
     *
     * @return the list of cores which constraints are fulfilled by th described resource
     */
    public static List<Integer> findExecutableCores(ResourceDescription rd) {
        List<Integer> executableList = new LinkedList<>();

        for (int coreId = 0; coreId < coreCount; ++coreId) {
            for (Implementation impl : IMPLEMENTATIONS.get(coreId)) {
                if (rd.canHost(impl)) {
                    // Add core to executable list
                    executableList.add(coreId);
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
    public static String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Core Count: ").append(coreCount).append("\n");
        for (int coreId = 0; coreId < coreCount; coreId++) {
            sb.append("\tCore ").append(coreId).append(":\n");
            for (Implementation impl : IMPLEMENTATIONS.get(coreId)) {
                sb.append("\t\t -").append(impl.toString()).append("\n");
            }
        }
        return sb.toString();
    }

    public static String debugSignaturesString() {
        StringBuilder sb = new StringBuilder();
        sb.append("REGISTERED SIGNATURES: \n");
        for (Entry<String, Integer> entry : SIGNATURE_TO_CORE_ID.entrySet()) {
            sb.append("Signature: ").append(entry.getKey());
            sb.append(" with MethodId ").append(entry.getValue());
            sb.append("\n");
        }

        return sb.toString();
    }

}
