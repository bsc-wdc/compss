package constraintsTest;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.ServiceImplementation;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.MultiConstraints;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.ServiceResourceDescription;
import integratedtoolkit.types.resources.ServiceWorker;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.CloudManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class ConstraintsTest {

    //Interface data
    static int coreCountItf;
    static String[][] declaringClassesItf;
    static Constraints[][] constraintsItf;
    static Constraints[] generalConstraintsItf;

    //CoreManagerData
    static String[] coreToName;
    static java.util.Map<String, Integer> signatureToId;
    static LinkedList<String>[] idToSignatures;

    /* ***************************************
     *    MAIN IMPLEMENTATION 
     * *************************************** */
    public static void main(String[] args) {
        try {
            Thread.sleep(7000);
        } catch (Exception e) {
        }
        //Run Constraint Manager Test
        System.out.println("[LOG] 1- Running ConstraintManager Test");
        constraintManagerTest();

        //Run Resources Manager Test
        System.out.println();
        System.out.println("[LOG] 2- Running ResourceManager Test");
        resourceManagerTest();

        //Run Available Resources Test
        System.out.println();
        System.out.println("[LOG] 3- Running Available Resources Test");
        availableResourcesTest();

        //Run Cloud Manager Test
        System.out.println();
        System.out.println("[LOG] 4- Running CloudManager Test");
        cloudManagerTest();
    }

    /* ***************************************
     * CONSTRAINT MANAGER TEST IMPLEMENTATION 
     * *************************************** */
    @SuppressWarnings("unchecked")
    private static void constraintManagerTest() {
        //Check if the number of tasks from itf and core is the same
        java.lang.reflect.Method[] declaredMethodsItf = ConstraintsTestItf.class.getDeclaredMethods();
        coreCountItf = declaredMethodsItf.length;
        if (CoreManager.getCoreCount() != coreCountItf) {
            System.out.println("[ERROR]" + CoreManager.getCoreCount() + " CE registered in the runtime and  " + coreCountItf + " declared in the CEI");
            System.exit(-1);
        }

        //Loading data from Cores
        signatureToId = CoreManager.SIGNATURE_TO_ID;
        idToSignatures = new LinkedList[coreCountItf];
        for (int coreId = 0; coreId < coreCountItf; coreId++) {
            idToSignatures[coreId] = new LinkedList<String>();
        }
        for (java.util.Map.Entry<String, Integer> entry : signatureToId.entrySet()) {
            String signature = entry.getKey();
            Integer coreId = entry.getValue();
            idToSignatures[coreId].add(signature);
        }
        //loading Information from the interface
        declaringClassesItf = new String[coreCountItf][];
        constraintsItf = new Constraints[coreCountItf][];
        generalConstraintsItf = new Constraints[coreCountItf];
        coreToName = new String[coreCountItf];
        for (int i = 0; i < coreCountItf; i++) {
            int cutValue1 = idToSignatures[i].getFirst().indexOf("(");
            int cutValue2 = idToSignatures[i].getFirst().indexOf(")");
            coreToName[i] = idToSignatures[i].getFirst().substring(0, cutValue1);
            String params = idToSignatures[i].getFirst().substring(cutValue1, cutValue2);
            java.lang.reflect.Method m = null;
            try {
                if (params.equals("(")) {
                    m = ConstraintsTestItf.class.getDeclaredMethod(coreToName[i], new Class[]{});
                } else {
                    m = ConstraintsTestItf.class.getDeclaredMethod(coreToName[i], new Class[]{java.lang.String.class});
                }
            } catch (NoSuchMethodException nsme) {
                System.out.println("[ERROR] Method " + coreToName[i] + "not found.");
                System.exit(-1);
            }

            if (m.getAnnotation(integratedtoolkit.types.annotations.Service.class) == null) {
                declaringClassesItf[i] = m.getAnnotation(integratedtoolkit.types.annotations.Method.class).declaringClass();
                if (m.isAnnotationPresent(MultiConstraints.class)) {
                    constraintsItf[i] = m.getAnnotation(MultiConstraints.class).value();
                } else {
                    constraintsItf[i] = new Constraints[declaringClassesItf[i].length];
                }
                if (m.isAnnotationPresent(Constraints.class)) {
                    generalConstraintsItf[i] = m.getAnnotation(Constraints.class);
                }
            }
        }

        //Check all cores
        for (int i = 0; i < coreCountItf; i++) {
            if (declaringClassesItf[i] != null) {
                checkCoreConstraints(i);
            }
        }
    }

    private static void checkCoreConstraints(int coreId) {
        System.out.println("[LOG] Checking " + coreToName[coreId]);
        System.out.println("[LOG] \t Has " + declaringClassesItf[coreId].length + " declaring classes in the CEI");
        System.out.println("[LOG] \t Has " + idToSignatures[coreId].size() + " signatures registered");

        if (declaringClassesItf[coreId].length != idToSignatures[coreId].size()) {
            System.out.println(coreToName[coreId] + " has " + idToSignatures[coreId].size() + " registered signatures and there are " + declaringClassesItf[coreId].length + " declaringClasses in the CEI");
            System.exit(-1);
        }

        Implementation<?>[] implementations = CoreManager.getCoreImplementations(coreId);
        System.out.println("[LOG] \t Has " + implementations.length + " implementations registered");
        if (declaringClassesItf[coreId].length != implementations.length) {
            System.out.println(coreToName[coreId] + " has " + implementations.length + " registered implementations and there are " + declaringClassesItf[coreId].length + " declaringClasses in the CEI");
            System.exit(-1);
        }

        //Check all constraints
        for (int impl = 0; impl < declaringClassesItf[coreId].length; impl++) {
            integratedtoolkit.types.MethodImplementation m = ((integratedtoolkit.types.MethodImplementation) implementations[impl]);
            System.out.println("[LOG] \t" + declaringClassesItf[coreId][impl]);
            if (declaringClassesItf[coreId][impl].compareTo(m.getDeclaringClass()) != 0) {
                System.out.println(coreToName[coreId] + "'s declaringClass " + declaringClassesItf[coreId][impl] + " is not included registered in the system");
                System.exit(-1);
            }
            String constraint = checkConstraints(generalConstraintsItf[coreId], constraintsItf[coreId][impl], m.getRequirements());
            if (constraint != null) {
                System.out.println("Constraints for " + coreToName[coreId] + "'s declaringClass " + declaringClassesItf[coreId][impl] + " does not meet the annotations (" + constraint + ")");
                System.exit(-1);
            }
        }
    }

    private static String checkConstraints(Constraints general, Constraints specific, MethodResourceDescription registered) {

        boolean ret = true;

        //Architecture
        if (general == null || general.processorArchitecture().compareTo("[unassigned]") == 0) {
            if (specific == null || specific.processorArchitecture().compareTo("[unassigned]") == 0) {
                //valor per defecte
                ret = registered.getProcessorArchitecture().compareTo("[unassigned]") == 0;
            } else {
                //valor de specific
                ret = registered.getProcessorArchitecture().compareTo(specific.processorArchitecture()) == 0;
            }
        } else {
            if (specific == null || specific.processorArchitecture().compareTo("[unassigned]") == 0) {
                //valor de general
                ret = registered.getProcessorArchitecture().compareTo(general.processorArchitecture()) == 0;
            } else {
                //valor de conjunt
                ret = registered.getProcessorArchitecture().compareTo(specific.processorArchitecture()) == 0;
            }
        }
        if (!ret) {
            return "processorArchitecture";
        }

        //CPUCount
        if (general == null || general.processorCPUCount() == 0) {
            if (specific == null || specific.processorCPUCount() == 0) {
                //valor per defecte
                ret = registered.getProcessorCPUCount() == 0;
            } else {
                //valor de specific
                ret = registered.getProcessorCPUCount() == specific.processorCPUCount();
            }
        } else {
            if (specific == null || specific.processorCPUCount() == 0) {
                //valor de general
                ret = registered.getProcessorCPUCount() == general.processorCPUCount();
            } else {
                //valor de conjunt
                ret = registered.getProcessorCPUCount() == specific.processorCPUCount();
            }
        }
        if (!ret) {
            return "processorCPUCount";
        }

        //CoreCount
        if (specific != null && specific.processorCoreCount() != 0) {
            ret = registered.getProcessorCoreCount() == specific.processorCoreCount();
        } else {
            if (general != null && general.processorCoreCount() != 0) {
                ret = registered.getProcessorCoreCount() == general.processorCoreCount();
            } else {
                ret = registered.getProcessorCoreCount() == 1;
            }
        }
        if (!ret) {
            return "processorCoreCount";
        }

        // processorSpeed;
        if (general == null || general.processorSpeed() == 0) {
            if (specific == null || specific.processorSpeed() == 0) {
                //valor per defecte
                ret = registered.getProcessorSpeed() == 0;
            } else {
                //valor de specific
                ret = registered.getProcessorSpeed() == specific.processorSpeed();
            }
        } else {
            if (specific == null || specific.processorSpeed() == 0) {
                //valor de general
                ret = registered.getProcessorSpeed() == general.processorSpeed();
            } else {
                //valor de conjunt
                ret = registered.getProcessorSpeed() == specific.processorSpeed();
            }
        }
        if (!ret) {
            return "processorSpeed";
        }

        // memoryPhysicalSize;
        if (general == null || general.memoryPhysicalSize() == 0) {
            if (specific == null || specific.memoryPhysicalSize() == 0) {
                //valor per defecte
                ret = registered.getMemoryPhysicalSize() == 0;
            } else {
                //valor de specific
                ret = registered.getMemoryPhysicalSize() == specific.memoryPhysicalSize();
            }
        } else {
            if (specific == null || specific.memoryPhysicalSize() == 0) {
                //valor de general
                ret = registered.getMemoryPhysicalSize() == general.memoryPhysicalSize();
            } else {
                //valor de conjunt
                ret = registered.getMemoryPhysicalSize() == specific.memoryPhysicalSize();
            }
        }
        if (!ret) {
            return "memoryPhysicalSize";
        }

        // memoryVirtualSize;
        if (general == null || general.memoryVirtualSize() == 0) {
            if (specific == null || specific.memoryVirtualSize() == 0) {
                //valor per defecte
                ret = registered.getMemoryVirtualSize() == 0;
            } else {
                //valor de specific
                ret = registered.getMemoryVirtualSize() == specific.memoryVirtualSize();
            }
        } else {
            if (specific == null || specific.memoryVirtualSize() == 0) {
                //valor de general
                ret = registered.getMemoryVirtualSize() == general.memoryVirtualSize();
            } else {
                //valor de conjunt
                ret = registered.getMemoryVirtualSize() == specific.memoryVirtualSize();
            }
        }
        if (!ret) {
            return "memoryVirtualSize";
        }

        // memoryAccessTime;
        if (general == null || general.memoryAccessTime() == 0) {
            if (specific == null || specific.memoryAccessTime() == 0) {
                //valor per defecte
                ret = registered.getMemoryAccessTime() == 0;
            } else {
                //valor de specific
                ret = registered.getMemoryAccessTime() == specific.memoryAccessTime();
            }
        } else {
            if (specific == null || specific.memoryAccessTime() == 0) {
                //valor de general
                ret = registered.getMemoryAccessTime() == general.memoryAccessTime();
            } else {
                //valor de conjunt
                ret = registered.getMemoryAccessTime() == specific.memoryAccessTime();
            }
        }
        if (!ret) {
            return "memoryAccessTime";
        }

        //memorySTR;
        if (general == null || general.memorySTR() == 0) {
            if (specific == null || specific.memorySTR() == 0) {
                //valor per defecte
                ret = registered.getMemorySTR() == 0;
            } else {
                //valor de specific
                ret = registered.getMemorySTR() == specific.memorySTR();
            }
        } else {
            if (specific == null || specific.memorySTR() == 0) {
                //valor de general
                ret = registered.getMemorySTR() == general.memorySTR();
            } else {
                //valor de conjunt
                ret = registered.getMemorySTR() == specific.memorySTR();
            }
        }
        if (!ret) {
            return "memorySTR";
        }

        // storageElemSize;
        if (general == null || general.storageElemSize() == 0) {
            if (specific == null || specific.storageElemSize() == 0) {
                //valor per defecte
                ret = registered.getStorageElemSize() == 0;
            } else {
                //valor de specific
                ret = registered.getStorageElemSize() == specific.storageElemSize();
            }
        } else {
            if (specific == null || specific.storageElemSize() == 0) {
                //valor de general
                ret = registered.getStorageElemSize() == general.storageElemSize();
            } else {
                //valor de conjunt
                ret = registered.getStorageElemSize() == specific.storageElemSize();
            }
        }
        if (!ret) {
            return "storageElemSize";
        }

        // storageElemAccessTime;
        if (general == null || general.storageElemAccessTime() == 0) {
            if (specific == null || specific.storageElemAccessTime() == 0) {
                //valor per defecte
                ret = registered.getStorageElemAccessTime() == 0;
            } else {
                //valor de specific
                ret = registered.getStorageElemAccessTime() == specific.storageElemAccessTime();
            }
        } else {
            if (specific == null || specific.storageElemAccessTime() == 0) {
                //valor de general
                ret = registered.getStorageElemAccessTime() == general.storageElemAccessTime();
            } else {
                //valor de conjunt
                ret = registered.getStorageElemAccessTime() == specific.storageElemAccessTime();
            }
        }
        if (!ret) {
            return "storageElemAccessTime";
        }

        // storageElemSTR;
        if (general == null || general.storageElemSTR() == 0) {
            if (specific == null || specific.storageElemSTR() == 0) {
                //valor per defecte
                ret = registered.getStorageElemSTR() == 0;
            } else {
                //valor de specific
                ret = registered.getStorageElemSTR() == specific.storageElemSTR();
            }
        } else {
            if (specific == null || specific.storageElemSTR() == 0) {
                //valor de general
                ret = registered.getStorageElemSTR() == general.storageElemSTR();
            } else {
                //valor de conjunt
                ret = registered.getStorageElemSTR() == specific.storageElemSTR();
            }
        }
        if (!ret) {
            return "storageElemSTR";
        }

        // operatingSystemType;
        if (general == null || general.operatingSystemType().compareTo("[unassigned]") == 0) {
            if (specific == null || specific.operatingSystemType().compareTo("[unassigned]") == 0) {
                //valor per defecte
                ret = registered.getOperatingSystemType().compareTo("[unassigned]") == 0;
            } else {
                //valor de specific
                ret = registered.getOperatingSystemType().compareTo(specific.operatingSystemType()) == 0;
            }
        } else {
            if (specific == null || specific.operatingSystemType().compareTo("[unassigned]") == 0) {
                //valor de general
                ret = registered.getOperatingSystemType().compareTo(general.operatingSystemType()) == 0;
            } else {
                //valor de conjunt
                ret = registered.getOperatingSystemType().compareTo(specific.operatingSystemType()) == 0;
            }
        }
        if (!ret) {
            return "operatingSystemType";
        }

        // hostQueue;
        if (general == null || general.hostQueue().compareTo("[unassigned]") == 0) {
            if (specific == null || specific.hostQueue().compareTo("[unassigned]") == 0) {
                //valor per defecte
                ret = registered.getHostQueue().isEmpty();
            } else {
                //valor de specific
                HashSet<String> values = new HashSet<String>();
                for (String s : specific.hostQueue().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getHostQueue().containsAll(values);
            }
        } else {
            if (specific == null || specific.hostQueue().compareTo("[unassigned]") == 0) {
                //valor de general
                HashSet<String> values = new HashSet<String>();
                for (String s : general.hostQueue().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getHostQueue().containsAll(values);
            } else {
                //valor de conjunt
                HashSet<String> values = new HashSet<String>();
                for (String s : general.hostQueue().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                for (String s : specific.hostQueue().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getHostQueue().containsAll(values);
            }
        }
        if (!ret) {
            return "hostQueue";
        }

        // appSoftware;
        if (general == null || general.appSoftware().compareTo("[unassigned]") == 0) {
            if (specific == null || specific.appSoftware().compareTo("[unassigned]") == 0) {
                //valor per defecte
                ret = registered.getAppSoftware().isEmpty();
            } else {
                //valor de specific
                HashSet<String> values = new HashSet<String>();
                for (String s : specific.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getAppSoftware().containsAll(values);
            }
        } else {
            if (specific == null || specific.appSoftware().compareTo("[unassigned]") == 0) {
                //valor de general
                HashSet<String> values = new HashSet<String>();
                for (String s : general.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getAppSoftware().containsAll(values);
            } else {
                //valor de conjunt
                HashSet<String> values = new HashSet<String>();
                for (String s : general.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                for (String s : specific.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getAppSoftware().containsAll(values);
            }
        }
        if (!ret) {
            System.out.println("Registered:" + registered.getAppSoftware());
            if (general != null) {
                System.out.println("General:" + general.appSoftware());
            }
            if (specific != null) {
                System.out.println("specific:" + specific.appSoftware());
            }
            return "appSoftware";
        }

        //ALL CONSTRAINTS OK VALUE
        return null;
    }

    /* **************************************
     * RESOURCE MANAGER TEST IMPLEMENTATION 
     * ************************************** */
    private static void resourceManagerTest() {
        //Check for each implementation the correctness of its resources
        System.out.println("[LOG] Number of cores = " + coreCountItf);
        for (int i = 0; i < coreCountItf; i++) {
            System.out.println("[LOG] Checking Core" + i);
            HashMap<Worker<?>, LinkedList<Implementation<?>>> m = ResourceManager.findAvailableWorkers(ResourceManager.findCompatibleWorkers(i), i);
            checkCoreResources(i, m);
        }

    }

    private static void checkCoreResources(int coreId, HashMap<Worker<?>, LinkedList<Implementation<?>>> hm) {
        //Revert Map
        HashMap<Implementation<?>, LinkedList<Worker<?>>> hm_reverted = new HashMap<Implementation<?>, LinkedList<Worker<?>>>();
        for (java.util.Map.Entry<Worker<?>, LinkedList<Implementation<?>>> entry_hm : hm.entrySet()) {
            for (Implementation<?> impl : entry_hm.getValue()) {
                LinkedList<Worker<?>> aux = hm_reverted.get(impl);
                if (aux == null) {
                    aux = new LinkedList<Worker<?>>();
                }
                aux.add(entry_hm.getKey());
                hm_reverted.put(impl, aux);
            }
        }

        //Check Resources assigned to each implementation
        System.out.println("[LOG] ** Number of Implementations = " + hm_reverted.size());
        for (java.util.Map.Entry<Implementation<?>, LinkedList<Worker<?>>> entry : hm_reverted.entrySet()) {
            System.out.println("[LOG] ** Checking Implementation " + entry.getKey());
            System.out.println("[LOG] **** Number of resources = " + entry.getValue().size());
            for (Worker<?> resource : entry.getValue()) {
                System.out.println("[LOG] **** Checking Resource " + resource.getName());
                String res = checkResourcesAssignedToImpl(entry.getKey(), resource);
                if (res != null) {
                    String error = "Implementation: Core = " + coreId + " Impl = " + entry.getKey().getImplementationId() + ". ";
                    error = error.concat("Implementation and resource not matching on: " + res);
                    System.out.println(error);
                    System.exit(-1);
                }
            }
        }
    }

    @SuppressWarnings("static-access")
    private static String checkResourcesAssignedToImpl(Implementation<?> impl, Worker<?> resource) {
        if ((impl.getType().equals(impl.getType().METHOD) && resource.getType().equals(resource.getType().SERVICE))
                || (impl.getType().equals(impl.getType().SERVICE) && resource.getType().equals(resource.getType().WORKER))) {
            return "types";
        }

        if (resource.getType() == Worker.Type.WORKER) {
            MethodImplementation mImpl = (MethodImplementation) impl;
            MethodResourceDescription iDescription = mImpl.getRequirements();
            MethodWorker worker = (MethodWorker) resource;
            MethodResourceDescription wDescription = worker.getDescription();
            //Check Processor Constraints
            if ((!iDescription.getProcessorArchitecture().equals("[unassigned]"))
                    && (!wDescription.getProcessorArchitecture().equals("[unassigned]"))
                    && !wDescription.getProcessorArchitecture().equals(iDescription.getProcessorArchitecture())) {
                return "processorArchitecture";
            }
            if ((iDescription.getProcessorCoreCount() != 1)
                    && (wDescription.getProcessorCoreCount() != 1)
                    && (wDescription.getProcessorCoreCount() < iDescription.getProcessorCoreCount())) {
                return "processorCoreCount";
            }
            if ((iDescription.getProcessorCPUCount() != 0)
                    && (wDescription.getProcessorCPUCount() != 0)
                    && (wDescription.getProcessorCPUCount() < iDescription.getProcessorCPUCount())) {
                return "processorCPUCount";
            }
            if ((iDescription.getProcessorSpeed() != 0)
                    && (wDescription.getProcessorSpeed() != 0)
                    && (wDescription.getProcessorSpeed() < iDescription.getProcessorSpeed())) {
                return "processorSpeed";
            }

            //Check Memory Constraints
            if ((iDescription.getMemoryAccessTime() != 0)
                    && (wDescription.getMemoryAccessTime() != 0)
                    && (wDescription.getMemoryAccessTime() > iDescription.getMemoryAccessTime())) {
                return "MemoryAccesTime";
            }
            if ((iDescription.getMemoryPhysicalSize() != 0)
                    && (wDescription.getMemoryPhysicalSize() != 0)
                    && (wDescription.getMemoryPhysicalSize() < iDescription.getMemoryPhysicalSize())) {
                return "MemoryPhysicalSize";
            }
            if ((iDescription.getMemorySTR() != 0)
                    && (wDescription.getMemorySTR() != 0)
                    && (wDescription.getMemorySTR() < iDescription.getMemorySTR())) {
                return "MemorySTR";
            }
            if ((iDescription.getMemoryVirtualSize() != 0)
                    && (wDescription.getMemoryVirtualSize() != 0)
                    && (wDescription.getMemoryVirtualSize() < iDescription.getMemoryVirtualSize())) {
                return "MemoryVirtualSize";
            }

            //Check Memory Constraints
            if ((iDescription.getStorageElemAccessTime() != 0)
                    && (wDescription.getStorageElemAccessTime() != 0)
                    && (wDescription.getStorageElemAccessTime() > iDescription.getStorageElemAccessTime())) {
                return "storageElemAccessTime";
            }
            if ((iDescription.getStorageElemSize() != 0)
                    && (wDescription.getStorageElemSize() != 0)
                    && (wDescription.getStorageElemSize() < iDescription.getStorageElemSize())) {
                return "storageElemSize";
            }
            if ((iDescription.getStorageElemSTR() != 0)
                    && (wDescription.getStorageElemSTR() != 0)
                    && (wDescription.getStorageElemSTR() < iDescription.getStorageElemSTR())) {
                return "storageElemSTR";
            }

            //Check appSoftware
            if (!(iDescription.getAppSoftware().isEmpty())
                    && !(wDescription.getAppSoftware().containsAll(iDescription.getAppSoftware()))) {
                return "appSoftware";
            }

            //Check Host Queue
            if (!(iDescription.getHostQueue().equals("[unassigned]"))) {
                LinkedList<String> aux = new LinkedList<String>();
                for (String elem : iDescription.getHostQueue()) {
                    if (elem.compareTo("[unassigned]") != 0) {
                        aux.add(elem);
                    }
                }
                if (!(wDescription.getHostQueue().containsAll(aux))) {
                    return "hostQueue";
                }
            }

            //Check Operating System Type
            if ((!iDescription.getOperatingSystemType().equals("[unassigned]"))
                    && (!wDescription.getOperatingSystemType().equals("[unassigned]"))
                    && !wDescription.getOperatingSystemType().equals(iDescription.getOperatingSystemType())) {
                System.out.println(iDescription.getOperatingSystemType());
                System.out.println(wDescription.getOperatingSystemType());
                return "operatingSystemType";
            }
        } else if (resource.getType() == Worker.Type.SERVICE) {
            ServiceImplementation mImpl = (ServiceImplementation) impl;
            ServiceResourceDescription iDescription = mImpl.getRequirements();
            ServiceWorker worker = (ServiceWorker) resource;
            ServiceResourceDescription wDescription = worker.getDescription();

            if (!wDescription.getServiceName().equals(iDescription.getServiceName())) {
                return "ServiceName";
            }
            if (!wDescription.getNamespace().equals(iDescription.getNamespace())) {
                return "Namespace";
            }
            if (!wDescription.getPort().equals(iDescription.getPort())) {
                return "Port";
            }
        } else {
            return "Unknown resource type";
        }

        //ALL CONSTRAINTS VALUE OK
        return null;
    }

    /* ***************************************
     * AVAILABLE RESOURCES TEST IMPLEMENTATION 
     * *************************************** */
    private static void availableResourcesTest() {
        //Find core numbers to execute
        boolean found_ce1 = false;
        boolean found_ce2 = false;
        int ce1 = 0;
        int ce2 = 0;
        while ((!found_ce1 || !found_ce2) && (ce1 < coreToName.length) && (ce2 < coreToName.length)) {
            if (coreToName[ce1].equals("coreElementAR1")) {
                found_ce1 = true;
            }
            if (coreToName[ce2].equals("coreElementAR2")) {
                found_ce2 = true;
            }
            if (!found_ce1) {
                ce1 = ce1 + 1;
            }
            if (!found_ce2) {
                ce2 = ce2 + 1;
            }
        }

        //Check results
        if (!found_ce1) {
            System.out.println("[ERROR] coreElementAR1 not found.");
            System.exit(-1);
        }
        if (!found_ce2) {
            System.out.println("[ERROR] coreElementAR2 not found.");
            System.exit(-1);
        }

        //Reserve/free for CORE Test
        Worker dynamicWorker = ResourceManager.getWorker("WorkerDynamic");
        System.out.println("DynWorker object is "+dynamicWorker+ "and  CoreImplementations requirements object is "+ CoreManager.getCoreImplementations(ce1)[0]);
        dynamicWorker.runTask(CoreManager.getCoreImplementations(ce1)[0].getRequirements());
        dynamicWorker.runTask(CoreManager.getCoreImplementations(ce1)[0].getRequirements());
        if (ResourceManager.findAvailableWorkers(ResourceManager.findCompatibleWorkers(ce1), ce1).containsKey(dynamicWorker)) {
            System.out.println("[ERROR] Available resources for CORE reserve is not working");
            System.exit(-1);
        }

        dynamicWorker.endTask(CoreManager.getCoreImplementations(ce1)[0].getRequirements());
        if (!ResourceManager.findAvailableWorkers(ResourceManager.findCompatibleWorkers(ce1), ce1).containsKey(dynamicWorker)) {
            System.out.println("[ERROR] Available resources for CORE free is not working");
            System.exit(-1);
        }
        dynamicWorker.endTask(CoreManager.getCoreImplementations(ce1)[0].getRequirements());

        //Reserve/free for MEMORY Test
        dynamicWorker.runTask(CoreManager.getCoreImplementations(ce2)[0].getRequirements());
        dynamicWorker.runTask(CoreManager.getCoreImplementations(ce2)[0].getRequirements());
        if (ResourceManager.findAvailableWorkers(ResourceManager.findCompatibleWorkers(ce2), ce2).containsKey(dynamicWorker)) {
            System.out.println("[ERROR] Available resources for MEMORY reserve is not working");
            System.exit(-1);
        }
        dynamicWorker.endTask(CoreManager.getCoreImplementations(ce2)[0].getRequirements());
        if (!ResourceManager.findAvailableWorkers(ResourceManager.findCompatibleWorkers(ce2), ce2).containsKey(dynamicWorker)) {
            System.out.println("[ERROR] Available resources for MEMORY free is not working");
            System.exit(-1);
        }
        dynamicWorker.endTask(CoreManager.getCoreImplementations(ce2)[0].getRequirements());
        System.out.println("[LOG] * Available Resources test passed");
    }

    /* ***********************************
     * CLOUD MANAGER TEST IMPLEMENTATION 
     * *********************************** */
    @SuppressWarnings("static-access")
    private static void cloudManagerTest() {
        //Print Out CloudManager static structures
        System.out.println("[LOG] CloudManager Static Structures definition");
//        System.out.println(CloudManager.getStaticInformation(""));

        //Check for each implementation the correctness of its resources
        System.out.println("[LOG] Number of cores = " + coreCountItf);
        for (int coreId = 0; coreId < coreCountItf; coreId++) {
            System.out.println("[LOG] Checking Core" + coreId);
            for (Implementation<?> impl : CoreManager.getCoreImplementations(coreId)) {
                if (impl.getType().equals(impl.getType().METHOD)) {
                    System.out.println("[LOG]\t Checking Implementation: " + impl.getImplementationId());
                    System.out.println("\t\t Checking obtained compatible cloud images");

                    MethodImplementation mImpl = (MethodImplementation) impl;

                    for (CloudImageDescription cid_gci : CloudManager.getProvider("BSC").getCloudImageManager().getCompatibleImages(mImpl.getRequirements())) {
                        System.out.println("\t\t\t Checking compatible Image: " + cid_gci.getName());
                        String res = checkImplementationAssignedToCloudImage(mImpl.getRequirements(), cid_gci);
                        if (res != null) {
                            String error = "[ERROR] Implementation: Core = " + coreId + " Impl = " + impl.getImplementationId() + ". ";
                            error = error.concat("Implementation and cloud image not matching on: " + res);
                            System.out.println(error);
                            System.exit(-1);
                        }
                    }
                    System.out.println("\t\t Checking obtained compatible cloud types");
                    for (CloudMethodResourceDescription gct : CloudManager.getProvider("BSC").getCloudTypeManager().getCompatibleTypes(new CloudMethodResourceDescription(mImpl.getRequirements()))) {
                        if (gct.canHostSimultaneously(mImpl.getRequirements()) < 1) {
                            continue;
                        }
                        System.out.println("\t\t\t Checking compatible Type: " + gct.getType());
                        String res = checkImplementationAssignedToType(mImpl.getRequirements(), gct);
                        if (res != null) {
                            String error = "[ERROR] Implementation: Core = " + coreId + " Impl = " + impl.getImplementationId() + ". ";
                            error = error.concat("Implementation and type not matching on: " + res);
                            System.out.println(error);
                            System.exit(-1);
                        }
                    }
                }
            }
        }

        //Return success value
        System.out.println("[LOG] * CloudManager test passed");
    }

    private static String checkImplementationAssignedToCloudImage(MethodResourceDescription rd, CloudImageDescription cid) {
        //Check Processor Architecture
        if ((!cid.getArch().equals("[unassigned]"))
                && (!rd.getProcessorArchitecture().equals("[unassigned]"))
                && (!cid.getArch().equals(rd.getProcessorArchitecture()))) {
            return "processorArchitecture";
        }

        //Check Operative System
        if ((!cid.getOperativeSystem().equals("[unassigned]"))
                && (!rd.getOperatingSystemType().equals("[unassigned]"))
                && (!cid.getOperativeSystem().equals(rd.getOperatingSystemType()))) {
            return "operatingSystemType";
        }

        //Check Software Apps
        if ((!cid.getSoftwareApps().isEmpty())
                && (!rd.getAppSoftware().isEmpty())
                && (!cid.getSoftwareApps().containsAll(rd.getAppSoftware()))) {
            return "softwareApps";
        }

        return null;
    }

    private static String checkImplementationAssignedToType(MethodResourceDescription rdImpl, CloudMethodResourceDescription rdType) {
        //Check Processor constraints
        if ((!rdImpl.getProcessorArchitecture().equals("[unassigned]"))
                && (!rdType.getProcessorArchitecture().equals("[unassigned]"))
                && (!rdType.getProcessorArchitecture().equals(rdImpl.getProcessorArchitecture()))) {
            return "processorArchitecture";
        }

        if ((rdImpl.getProcessorCoreCount() != 1)
                && (rdType.getProcessorCoreCount() != 1)
                && (rdType.getProcessorCoreCount() < rdImpl.getProcessorCoreCount())) {
            return "processorCoreCount";
        }

        if ((rdImpl.getProcessorCPUCount() != 0)
                && (rdType.getProcessorCPUCount() != 0)
                && (rdType.getProcessorCPUCount() < rdImpl.getProcessorCPUCount())) {
            return "processorCPUCount: Type has " + rdType.getProcessorCPUCount() + " > Impl has " + rdImpl.getProcessorCPUCount();
        }

        if ((rdImpl.getProcessorSpeed() != 0)
                && (rdType.getProcessorSpeed() != 0)
                && (rdType.getProcessorSpeed() < rdImpl.getProcessorSpeed())) {
            return "processorSpeed";
        }

        //Check Memory constraints
        if ((rdImpl.getMemoryAccessTime() != 0)
                && (rdType.getMemoryAccessTime() != 0)
                && (rdType.getMemoryAccessTime() < rdImpl.getMemoryAccessTime())) {
            return "MemoryAccessTime";
        }

        if ((rdImpl.getMemoryPhysicalSize() != 0)
                && (rdType.getMemoryPhysicalSize() != 0)
                && (rdType.getMemoryPhysicalSize() < rdImpl.getMemoryPhysicalSize())) {
            return "MemoryPhysicalSize";
        }

        if ((rdImpl.getMemoryVirtualSize() != 0)
                && (rdType.getMemoryVirtualSize() != 0)
                && (rdType.getMemoryVirtualSize() < rdImpl.getMemoryVirtualSize())) {
            return "getMemoryVirtualSize";
        }

        if ((rdImpl.getMemorySTR() != 0)
                && (rdType.getMemorySTR() != 0)
                && (rdType.getMemorySTR() < rdImpl.getMemorySTR())) {
            return "MemorySTR";
        }

        //Check Storage Element constraints
        if ((rdImpl.getStorageElemAccessTime() != 0)
                && (rdType.getStorageElemAccessTime() != 0)
                && (rdType.getStorageElemAccessTime() < rdImpl.getStorageElemAccessTime())) {
            return "StorageElemAccessTime";
        }

        if ((rdImpl.getStorageElemSize() != 0)
                && (rdType.getStorageElemSize() != 0)
                && (rdType.getStorageElemSize() < rdImpl.getStorageElemSize())) {
            return "StorageElemSize";
        }

        if ((rdImpl.getStorageElemSTR() != 0)
                && (rdType.getStorageElemSTR() != 0)
                && (rdType.getStorageElemSTR() < rdImpl.getStorageElemSTR())) {
            return "StorageElemSTR";
        }

        //Check Application Software constraints
        if ((!rdImpl.getAppSoftware().isEmpty())
                && (!rdType.getAppSoftware().isEmpty())
                && (!rdType.getAppSoftware().containsAll(rdImpl.getAppSoftware()))) {
            return "appSoftware";
        }

        //Check Host Queue constraints
        if ((!rdImpl.getHostQueue().isEmpty())
                && (!rdType.getHostQueue().isEmpty())
                && (!rdType.getHostQueue().containsAll(rdImpl.getHostQueue()))) {
            return "appSoftware";
        }

        //Check Operating System constraints
        if ((!rdImpl.getOperatingSystemType().equals("[unassigned]"))
                && (!rdType.getOperatingSystemType().equals("[unassigned]"))
                && (!rdType.getOperatingSystemType().equals(rdImpl.getOperatingSystemType()))) {
            return "OperatingSystemType";
        }

        return null;
    }

}
