package constraintManager;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import commons.ConstantValues;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.MultiConstraints;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.CoreManager;


/*
 * Checks if the Constraints are inserted correctly inside the runtime.
 * Only parses the ITF information and compares it with the information stored inside the runtime
 * NO need to have a resources.xml / project.xml information
 */
public class Test {

    // Interface data
    private static int coreCountItf;
    private static String[][] declaringClassesItf;
    private static Constraints[][] constraintsItf;
    private static Constraints[] generalConstraintsItf;

    // CoreManagerData
    private static String[] coreToName;
    private static java.util.Map<String, Integer> signatureToId;
    private static LinkedList<String>[] idToSignatures;


    /*
     * *************************************** MAIN IMPLEMENTATION ***************************************
     */
    public static void main(String[] args) {
        // Wait for Runtime to be loaded
        System.out.println("[LOG] Waiting for Runtime to be loaded");
        try {
            Thread.sleep(ConstantValues.WAIT_FOR_RUNTIME_TIME);
        } catch (Exception e) {
            // No need to handle such exceptions
        }

        // Run Constraint Manager Test
        System.out.println("[LOG] Running ConstraintManager Test");
        constraintManagerTest();
    }

    /*
     * *************************************** 
     * CONSTRAINT MANAGER TEST IMPLEMENTATION
     * ***************************************
     */
    @SuppressWarnings("unchecked")
    private static void constraintManagerTest() {
        // Check if the number of tasks from ITF and core is the same
        java.lang.reflect.Method[] declaredMethodsItf = TestItf.class.getDeclaredMethods();
        coreCountItf = declaredMethodsItf.length;
        if (CoreManager.getCoreCount() != coreCountItf) {
            System.out.println(
                    "[ERROR]" + CoreManager.getCoreCount() + " CE registered in the runtime and  " + coreCountItf + " declared in the CEI");
            System.exit(-1);
        }

        // Loading data from Cores
        signatureToId = CoreManager.SIGNATURE_TO_ID;
        idToSignatures = new LinkedList[coreCountItf];
        for (int coreId = 0; coreId < coreCountItf; coreId++) {
            idToSignatures[coreId] = new LinkedList<String>();
        }
        for (Entry<String, Integer> entry : signatureToId.entrySet()) {
            String signature = entry.getKey();
            Integer coreId = entry.getValue();
            idToSignatures[coreId].add(signature);
        }

        // Loading Information from the interface
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
                    m = TestItf.class.getDeclaredMethod(coreToName[i], new Class[] {});
                } else {
                    m = TestItf.class.getDeclaredMethod(coreToName[i], new Class[] { String.class });
                }
            } catch (NoSuchMethodException nsme) {
                System.out.println("[ERROR] Method " + coreToName[i] + "not found.");
                System.exit(-1);
            }

            declaringClassesItf[i] = m.getAnnotation(Method.class).declaringClass();
            if (m.isAnnotationPresent(MultiConstraints.class)) {
                constraintsItf[i] = m.getAnnotation(MultiConstraints.class).value();
            } else {
                constraintsItf[i] = new Constraints[declaringClassesItf[i].length];
            }
            if (m.isAnnotationPresent(Constraints.class)) {
                generalConstraintsItf[i] = m.getAnnotation(Constraints.class);
            }
        }

        // Check all cores
        for (int i = 0; i < coreCountItf; i++) {
            if (declaringClassesItf[i] != null) {
                checkCoreElementConstraints(i);
            }
        }
    }

    private static void checkCoreElementConstraints(int coreId) {
        System.out.println("[LOG] Checking " + coreToName[coreId]);
        System.out.println("[LOG] \t Has " + declaringClassesItf[coreId].length + " declaring classes in the CEI");
        System.out.println("[LOG] \t Has " + idToSignatures[coreId].size() + " signatures registered");

        if (declaringClassesItf[coreId].length != idToSignatures[coreId].size()) {
            System.out.println(coreToName[coreId] + " has " + idToSignatures[coreId].size() + " registered signatures and there are "
                    + declaringClassesItf[coreId].length + " declaringClasses in the CEI");
            System.exit(-1);
        }

        Implementation<?>[] implementations = CoreManager.getCoreImplementations(coreId);
        System.out.println("[LOG] \t Has " + implementations.length + " implementations registered");
        if (declaringClassesItf[coreId].length != implementations.length) {
            System.out.println(coreToName[coreId] + " has " + implementations.length + " registered implementations and there are "
                    + declaringClassesItf[coreId].length + " declaringClasses in the CEI");
            System.exit(-1);
        }

        // Check all constraints
        for (int impl = 0; impl < declaringClassesItf[coreId].length; impl++) {
            MethodImplementation m = ((MethodImplementation) implementations[impl]);
            System.out.println("[LOG] \t" + declaringClassesItf[coreId][impl]);
            if (declaringClassesItf[coreId][impl].compareTo(m.getDeclaringClass()) != 0) {
                System.out.println(coreToName[coreId] + "'s declaringClass " + declaringClassesItf[coreId][impl]
                        + " is not included registered in the system");
                System.exit(-1);
            }
            String constraint = checkConstraints(generalConstraintsItf[coreId], constraintsItf[coreId][impl], m.getRequirements());
            if (constraint != null) {
                System.out.println("Constraints for " + coreToName[coreId] + "'s declaringClass " + declaringClassesItf[coreId][impl]
                        + " does not meet the annotations (" + constraint + ")");
                System.exit(-1);
            }
        }
    }

    private static String checkConstraints(Constraints general, Constraints specific, MethodResourceDescription registered) {
        boolean ret = true;

        /*
         * ***************************************** 
         * ComputingUnits
         *****************************************/
        if (general == null || general.computingUnits() == Constants.UNASSIGNED_INT) {
            if (specific == null || specific.computingUnits() == Constants.UNASSIGNED_INT) {
                // Default value
                ret = (registered.getTotalComputingUnits() == MethodResourceDescription.ONE_INT);
            } else {
                // Specific constraint value
                ret = (registered.getTotalComputingUnits() == specific.computingUnits());
            }
        } else {
            if (specific == null || specific.computingUnits() == Constants.UNASSIGNED_INT) {
                // General constraint value
                ret = (registered.getTotalComputingUnits() == general.computingUnits());
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getTotalComputingUnits() == specific.computingUnits());
            }
        }
        if (!ret) {
            if (general != null) {
                System.out.println("GEN: " + general.computingUnits());
            }
            if (specific != null) {
                System.out.println("SPC: " + specific.computingUnits());
            }
            System.out.println("REG: " + registered.getTotalComputingUnits());
            return "computingUnits";
        }

        /*
         * ***************************************** 
         * Processor
         *****************************************/
        // !!! When checking constraints the limits are always on Processor 0
        if (general == null || general.processorName().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.processorName().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getProcessors().get(0).getName().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getProcessors().get(0).getName().equals(specific.processorName()));
            }
        } else {
            if (specific == null || specific.processorName().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getProcessors().get(0).getName().equals(general.processorName()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getProcessors().get(0).getName().equals(specific.processorName()));
            }
        }
        if (!ret) {
            return "processorName";
        }

        if (general == null || general.processorSpeed() == Constants.UNASSIGNED_FLOAT) {
            if (specific == null || specific.processorSpeed() == Constants.UNASSIGNED_FLOAT) {
                // Default value
                ret = (registered.getProcessors().get(0).getSpeed() == MethodResourceDescription.UNASSIGNED_FLOAT);
            } else {
                // Specific constraint value
                ret = (registered.getProcessors().get(0).getSpeed() == specific.processorSpeed());
            }
        } else {
            if (specific == null || specific.processorSpeed() == Constants.UNASSIGNED_FLOAT) {
                // General constraint value
                ret = (registered.getProcessors().get(0).getSpeed() == general.processorSpeed());
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getProcessors().get(0).getSpeed() == specific.processorSpeed());
            }
        }
        if (!ret) {
            return "processorSpeed";
        }

        if (general == null || general.processorArchitecture().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.processorArchitecture().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getProcessors().get(0).getArchitecture().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getProcessors().get(0).getArchitecture().equals(specific.processorArchitecture()));
            }
        } else {
            if (specific == null || specific.processorArchitecture().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getProcessors().get(0).getArchitecture().equals(general.processorArchitecture()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getProcessors().get(0).getArchitecture().equals(specific.processorArchitecture()));
            }
        }
        if (!ret) {
            return "processorArchitecture";
        }

        if (general == null || general.processorPropertyName().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.processorPropertyName().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getProcessors().get(0).getPropName().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getProcessors().get(0).getPropName().equals(specific.processorPropertyName()));
            }
        } else {
            if (specific == null || specific.processorPropertyName().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getProcessors().get(0).getPropName().equals(general.processorPropertyName()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getProcessors().get(0).getPropName().equals(specific.processorPropertyName()));
            }
        }
        if (!ret) {
            return "processorPropertyName";
        }

        if (general == null || general.processorPropertyValue().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.processorPropertyValue().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getProcessors().get(0).getPropValue().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getProcessors().get(0).getPropValue().equals(specific.processorPropertyValue()));
            }
        } else {
            if (specific == null || specific.processorPropertyValue().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getProcessors().get(0).getPropValue().equals(general.processorPropertyValue()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getProcessors().get(0).getPropValue().equals(specific.processorPropertyValue()));
            }
        }
        if (!ret) {
            return "processorPropertyValue";
        }

        /*
         * ***************************************** 
         * Memory
         *****************************************/
        if (general == null || general.memorySize() == Constants.UNASSIGNED_FLOAT) {
            if (specific == null || specific.memorySize() == Constants.UNASSIGNED_FLOAT) {
                // Default value
                ret = (registered.getMemorySize() == MethodResourceDescription.UNASSIGNED_FLOAT);
            } else {
                // Specific constraint value
                ret = (registered.getMemorySize() == specific.memorySize());
            }
        } else {
            if (specific == null || specific.memorySize() == Constants.UNASSIGNED_FLOAT) {
                // General constraint value
                ret = (registered.getMemorySize() == general.memorySize());
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getMemorySize() == specific.memorySize());
            }
        }
        if (!ret) {
            return "memorySize";
        }

        if (general == null || general.memoryType().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.memoryType().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getMemoryType().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getMemoryType().equals(specific.memoryType()));
            }
        } else {
            if (specific == null || specific.memoryType().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getMemoryType().equals(general.memoryType()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getMemoryType().equals(specific.memoryType()));
            }
        }
        if (!ret) {
            return "memoryType";
        }

        /*
         * ***************************************** 
         * Storage
         *****************************************/
        if (general == null || general.storageSize() == Constants.UNASSIGNED_FLOAT) {
            if (specific == null || specific.storageSize() == Constants.UNASSIGNED_FLOAT) {
                // Default value
                ret = (registered.getStorageSize() == MethodResourceDescription.UNASSIGNED_FLOAT);
            } else {
                // Specific constraint value
                ret = (registered.getStorageSize() == specific.storageSize());
            }
        } else {
            if (specific == null || specific.storageSize() == Constants.UNASSIGNED_FLOAT) {
                // General constraint value
                ret = (registered.getStorageSize() == general.storageSize());
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getStorageSize() == specific.storageSize());
            }
        }
        if (!ret) {
            return "storageSize";
        }

        if (general == null || general.storageType().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.storageType().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getStorageType().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getStorageType().equals(specific.storageType()));
            }
        } else {
            if (specific == null || specific.storageType().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getStorageType().equals(general.storageType()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getStorageType().equals(specific.storageType()));
            }
        }
        if (!ret) {
            return "storageType";
        }

        /*
         * ***************************************** 
         * Operating System
         *****************************************/
        if (general == null || general.operatingSystemType().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.operatingSystemType().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getOperatingSystemType().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getOperatingSystemType().equals(specific.operatingSystemType()));
            }
        } else {
            if (specific == null || specific.operatingSystemType().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getOperatingSystemType().equals(general.operatingSystemType()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getOperatingSystemType().equals(specific.operatingSystemType()));
            }
        }
        if (!ret) {
            return "operatingSystemType";
        }

        if (general == null || general.operatingSystemDistribution().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.operatingSystemDistribution().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getOperatingSystemDistribution().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getOperatingSystemDistribution().equals(specific.operatingSystemDistribution()));
            }
        } else {
            if (specific == null || specific.operatingSystemDistribution().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getOperatingSystemDistribution().equals(general.operatingSystemDistribution()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getOperatingSystemDistribution().equals(specific.operatingSystemDistribution()));
            }
        }
        if (!ret) {
            return "operatingSystemDistribution";
        }

        if (general == null || general.operatingSystemVersion().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.operatingSystemVersion().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = (registered.getOperatingSystemVersion().equals(MethodResourceDescription.UNASSIGNED_STR));
            } else {
                // Specific constraint value
                ret = (registered.getOperatingSystemVersion().equals(specific.operatingSystemVersion()));
            }
        } else {
            if (specific == null || specific.operatingSystemVersion().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                ret = (registered.getOperatingSystemVersion().equals(general.operatingSystemVersion()));
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getOperatingSystemVersion().equals(specific.operatingSystemVersion()));
            }
        }
        if (!ret) {
            return "operatingSystemVersion";
        }

        /*
         * ***************************************** 
         * Application Software
         *****************************************/
        if (general == null || general.appSoftware().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.appSoftware().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = registered.getAppSoftware().isEmpty();
            } else {
                // Specific constraint value
                HashSet<String> values = new HashSet<String>();
                for (String s : specific.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getAppSoftware().containsAll(values);
            }
        } else {
            if (specific == null || specific.appSoftware().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                HashSet<String> values = new HashSet<String>();
                for (String s : general.appSoftware().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", ""));
                    }
                }
                ret = registered.getAppSoftware().containsAll(values);
            } else {
                // Specific constraint value merged with general value
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
            return "applicationSoftware";
        }

        /*
         * ***************************************** 
         * Host Queues
         *****************************************/
        if (general == null || general.hostQueues().equals(Constants.UNASSIGNED_STR)) {
            if (specific == null || specific.hostQueues().equals(Constants.UNASSIGNED_STR)) {
                // Default value
                ret = registered.getHostQueues().isEmpty();
            } else {
                // Specific constraint value
                HashSet<String> values = new HashSet<String>();
                for (String s : specific.hostQueues().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", "").toUpperCase());
                    }
                }
                ret = registered.getHostQueues().containsAll(values);
            }
        } else {
            if (specific == null || specific.hostQueues().equals(Constants.UNASSIGNED_STR)) {
                // General constraint value
                HashSet<String> values = new HashSet<String>();
                for (String s : general.hostQueues().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", "").toUpperCase());
                    }
                }
                ret = registered.getHostQueues().containsAll(values);
            } else {
                // Specific constraint value merged with general value
                HashSet<String> values = new HashSet<String>();
                for (String s : general.hostQueues().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", "").toUpperCase());
                    }
                }
                for (String s : specific.hostQueues().split(",")) {
                    if (s.compareTo(",") != 0) {
                        values.add(s.replaceAll(" ", "").toUpperCase());
                    }
                }
                ret = registered.getHostQueues().containsAll(values);
            }
        }
        if (!ret) {
            return "hostQueues";
        }

        /*
         * ***************************************** 
         * WallClockLimit
         *****************************************/
        if (general == null || general.wallClockLimit() == Constants.UNASSIGNED_INT) {
            if (specific == null || specific.wallClockLimit() == Constants.UNASSIGNED_INT) {
                // Default value
                ret = (registered.getWallClockLimit() == MethodResourceDescription.UNASSIGNED_INT);
            } else {
                // Specific constraint value
                ret = (registered.getWallClockLimit() == specific.wallClockLimit());
            }
        } else {
            if (specific == null || specific.wallClockLimit() == Constants.UNASSIGNED_INT) {
                // General constraint value
                ret = (registered.getWallClockLimit() == general.wallClockLimit());
            } else {
                // Specific constraint value (general is overwritten)
                ret = (registered.getWallClockLimit() == specific.wallClockLimit());
            }
        }
        if (!ret) {
            return "wallClockLimit";
        }

        /*
         * ***************************************** 
         * ALL CONSTRAINT VALUES OK
         *****************************************/
        return null;
    }

}
