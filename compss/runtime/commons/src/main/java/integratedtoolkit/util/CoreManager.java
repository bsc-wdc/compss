package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.types.exceptions.NonInstantiableException;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.resources.ResourceDescription;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;


public class CoreManager {

    // Constants definition
    private static final Lang LANG;

    public static final LinkedHashMap<String, Integer> SIGNATURE_TO_ID = new LinkedHashMap<String, Integer>();
    private static int coreCount = 0;
    private static int nextId = 0;

    private static Implementation<?>[][] implementations;

    static {
        // Compute language
        Lang l = Lang.JAVA;
        
        String langProperty = System.getProperty(ITConstants.IT_LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase("python")) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase("c")) {
                l = Lang.C;
            }
        }
        
        LANG = l;
    }


    private CoreManager() {
        throw new NonInstantiableException("CoreManager");
    }

    public static void increaseCoreCount() {
        coreCount++;
    }

    public static void setCoreCount(int newCoreCount) {
        coreCount = newCoreCount;
    }

    public static int getCoreCount() {
        return coreCount;
    }

    public static void resizeStructures(int newCoreCount) {
        if (implementations != null) {
            Implementation<?>[][] oldImplementations = implementations;
            implementations = new Implementation[newCoreCount][];
            System.arraycopy(oldImplementations, 0, implementations, 0, oldImplementations.length);
        } else {
            implementations = new Implementation[newCoreCount][];
        }
        coreCount = newCoreCount;
    }

    public static void registerImplementations(int coreId, Implementation<?>[] impls) {
        implementations[coreId] = impls;
    }

    /**
     * Get coreId to create Method Task Description
     * 
     * @param declaringClass
     * @param methodName
     * @param hasTarget
     * @param hasReturn
     * @param parameters
     * @return
     */
    public static Integer getCoreId(String declaringClass, String methodName, boolean hasTarget, boolean hasReturn,
            integratedtoolkit.types.parameter.Parameter[] parameters) {
        
        Integer methodId = null;
        String signature = MethodImplementation.getSignature(declaringClass, methodName, hasTarget, hasReturn, parameters);
        
        methodId = SIGNATURE_TO_ID.get(signature);
        
        if (methodId == null) {
            methodId = nextId++;
            SIGNATURE_TO_ID.put(signature, methodId);
            switch(LANG) {
                case JAVA:
                    // Declaring classes are already computed by versioning
                    break;
                case PYTHON:
                    ((MethodImplementation) implementations[methodId][0]).setDeclaringClass(declaringClass);
                    break;
                case C:
                    // Declaring classes are already computed by versioning
                    break;
            }
        }
        
        return methodId;
    }

    /**
     * Get coreId to create Service Task Description
     * 
     * @param namespace
     * @param serviceName
     * @param portName
     * @param operation
     * @param hasTarget
     * @param hasReturn
     * @param parameters
     * @return
     */
    public static Integer getCoreId(String namespace, String serviceName, String portName, String operation, boolean hasTarget,
            boolean hasReturn, integratedtoolkit.types.parameter.Parameter[] parameters) {
        
        Integer methodId = null;
        String signature = ServiceImplementation.getSignature(namespace, serviceName, portName, operation, hasTarget, hasReturn,
                parameters);
        methodId = SIGNATURE_TO_ID.get(signature);
        
        if (methodId == null) {
            methodId = nextId++;
            SIGNATURE_TO_ID.put(signature, methodId);
        }
        
        return methodId;
    }

    /**
     * Registers a new coreElement if it hasn't been registered previously
     * 
     * @param signature
     * @return the coreId
     */
    public static Integer registerCoreId(String signature) {
        Integer methodId = SIGNATURE_TO_ID.get(signature);

        if (methodId == null) {
            methodId = nextId++;
        }
        
        SIGNATURE_TO_ID.put(signature, methodId);

        return methodId;
    }
    
    /**
     * Register a signature to a given coreId (for versioning)
     * 
     * @param methodId
     * @param signature
     * @return if the operation has been performed or not
     */
    public static boolean registerSignatureToCoreId(Integer methodId, String signature) {
        // Check that the methodId is valid
        if (!SIGNATURE_TO_ID.containsValue(methodId)) {
            // Unregistered methodId
            return false;
        }
        
        // Registered method, add versioning signature
        SIGNATURE_TO_ID.put(signature, methodId);
        return true;
    }

    /**
     * ********************************************* 
     * ********************************************* 
     * ************** QUERY OPERATIONS ************* 
     * *********************************************
     * *********************************************
     */
    /**
     * Returns all the implementations of a core Element
     *
     * @return the implementations for a Core Element
     */
    public static Implementation<?>[] getCoreImplementations(int coreId) {
        return implementations[coreId];
    }

    /**
     * Looks for all the cores from in the annotated Interface which constraint are fullfilled by the resource
     * description passed as a parameter
     *
     * @param rd
     *            ResourceDescription to find cores compatible to
     *
     * @return the list of cores which constraints are fulfilled by th described resource
     */
    public static List<Integer> findExecutableCores(ResourceDescription rd) {
        List<Integer> executableList = new LinkedList<Integer>();
        for (int methodId = 0; methodId < CoreManager.coreCount; methodId++) {
            boolean executable = false;
            for (int implementationId = 0; !executable && implementationId < implementations[methodId].length; implementationId++) {
                if (rd.canHost(implementations[methodId][implementationId])) {
                    executableList.add(methodId);
                }
            }
        }
        return executableList;
    }

    public static String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Core Count: ").append(coreCount).append("\n");
        for (int coreId = 0; coreId < coreCount; coreId++) {
            Implementation<?>[] impls = implementations[coreId];
            sb.append("\tCore ").append(coreId).append(":\n");
            for (Implementation<?> impl : impls) {
                sb.append("\t\t -").append(impl.toString()).append("\n");
            }
        }
        return sb.toString();
    }
    
    public static String debugSignaturesString() {
        StringBuilder sb = new StringBuilder();
        sb.append("REGISTERED SIGNATURES: \n");
        for (Entry<String, Integer> entry : SIGNATURE_TO_ID.entrySet()) {
            sb.append("Signature: ").append(entry.getKey());
            sb.append(" with MethodId ").append(entry.getValue());
            sb.append("\n");
        }
        
        return sb.toString();
    }

    public static void clear() {
        implementations = null;
        coreCount = 0;
        nextId = 0;
    }

}
