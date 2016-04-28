package constraintManager;

import java.util.LinkedList;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.MultiConstraints;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.CoreManager;


/*
 * Checks the Constraint Manager Integrity: if the constraints parsing is correct or not
 */
public class ConstraintManager {

    //Interface data
    static int coreCountItf;
    static String[][] declaringClassesItf;
    static Constraints[][] constraintsItf;
    static Constraints[] generalConstraintsItf;

    // CoreManagerData
    static String[] coreToName;
    static java.util.Map<String, Integer> signatureToId;
    static LinkedList<String>[] idToSignatures;

    
    /* ***************************************
     *    MAIN IMPLEMENTATION 
     * *************************************** */
    public static void main(String[] args) {
    	// Wait for Runtime to be loaded
    	System.out.println("[LOG] Waiting for Runtime to be loaded");
        try {
            Thread.sleep(7_000);
        } catch (Exception e) {
        	// No need to handle such exceptions
        }
        
        //Run Constraint Manager Test
        System.out.println("[LOG] Running ConstraintManager Test");
        //constraintManagerTest();
    }

    /* ***************************************
     * CONSTRAINT MANAGER TEST IMPLEMENTATION 
     * *************************************** */
    /*private static void constraintManagerTest() {
        // Check if the number of tasks from itf and core is the same
        java.lang.reflect.Method[] declaredMethodsItf = ConstraintManagerItf.class.getDeclaredMethods();
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
                    m = ConstraintManagerItf.class.getDeclaredMethod(coreToName[i], new Class[]{});
                } else {
                    m = ConstraintManagerItf.class.getDeclaredMethod(coreToName[i], new Class[]{java.lang.String.class});
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

        // Check all constraints
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
    }*/

    /*private static String checkConstraints(Constraints general, Constraints specific, MethodResourceDescription registered) {
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

        //CPUCount  -- Runtime CPU default value is 1 (not 0)
        if (general == null || general.processorCPUCount() <= 1) {
            if (specific == null || specific.processorCPUCount() <= 1) {
                //valor per defecte
                ret = registered.getProcessorCPUCount() <= 1;
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
        return null;*/
    //}

}
