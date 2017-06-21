package integratedtoolkit.util;

import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class CloudTypeManager {

    /**
     * Relation between the name of an template and its features
     */
    private HashMap<String, Type> types;

    /**
     * Relation between VM and their composing types [created, to be remove]
     */
    private HashMap<String, HashMap<String, int[]>> vmToType;

    /**
     * Constructs a new CloudImageManager
     */
    public CloudTypeManager() {
        types = new HashMap<>();
        vmToType = new HashMap<>();
    }

    /**
     * Adds a new instance type which can be used by the Cloud Provider
     *
     * @param rd Description of the resource
     */
    public void add(CloudMethodResourceDescription rd) {
        Type t = new Type(rd);
        types.put(rd.getName(), t);
    }

    /**
     * Finds all the types provided by the Cloud Provider which fulfill the
     * resource description.
     *
     * @param requested description of the features that the image must provide
     * @return The best instance type provided by the Cloud Provider which
     * fulfills the resource description
     */
    public LinkedList<CloudMethodResourceDescription> getCompatibleTypes(MethodResourceDescription requested) {
        LinkedList<CloudMethodResourceDescription> compatiblesList = new LinkedList<CloudMethodResourceDescription>();

        for (Type type : types.values()) {
            CloudMethodResourceDescription mixedDescription = new CloudMethodResourceDescription(type.getResourceDescription());
            if (mixedDescription.contains(requested)) {
                // Satisfies the constraints, add compatible
                compatiblesList.add(mixedDescription);
            }
        }

        return compatiblesList;
    }

    /**
     * Return all the image names offered by that Cloud Provider
     *
     * @return set of image names offered by that Cloud Provider
     */
    public Set<String> getAllInstanceTypeNames() {
        return types.keySet();
    }

    /**
     *
     * @param name
     * @return instance type description associated to that name
     */
    public CloudMethodResourceDescription getInstanceType(String name) {
        CloudMethodResourceDescription typeDescription = null;
        Type t = types.get(name);
        if (t != null) {
            typeDescription = new CloudMethodResourceDescription(t.rd);
        }
        return typeDescription;
    }

    public void createdVM(String resourceName, String requestType) {
        HashMap<String, int[]> vm = vmToType.get(resourceName);
        if (vm == null) {
            vm = new HashMap<>();
            for (String type : types.keySet()) {
                vm.put(type, new int[]{0});
            }
            vm.put(requestType, new int[]{1});
            vmToType.put(resourceName, vm);
        } else {
            vm.get(requestType)[0]++;
        }
    }

    public void reduceVM(String resourceName, String requestType) {
        HashMap<String, int[]> vm = vmToType.remove(resourceName);
        if (vm != null) {
            vm.get(requestType)[0]--;
            if (hasValidInstances(vm)) {
                vmToType.put(resourceName, vm);
            }
        }
    }

    public void clearAll() {
        vmToType.clear();
    }

    public CloudMethodResourceDescription getDescription(String type) {
        Type t = types.get(type);
        if (t != null) {
            return t.getResourceDescription();
        }
        return null;
    }

    public int[][] getSimultaneousImpls(String type) {
        Type t = types.get(type);
        if (t != null) {
            return t.getSlotsImpl();
        }
        return null;
    }

    // typeName->[(int[] slots that will be removed, (ResourceDescription) description of the resource that will be
    // destroyed]
    public HashMap<String, Object[]> getPossibleReductions(String name) {
        HashMap<String, Object[]> h = new HashMap<>();
        for (Entry<String, int[]> entry : vmToType.get(name).entrySet()) {
            String type = entry.getKey();
            Object[] value = new Object[2];
            int[] amount = entry.getValue();
            if (amount != null && amount[0] > 0) {
                Type t = types.get(type);
                if (t != null) {
                    value[0] = t.getSlotsCore();
                    value[1] = t.getResourceDescription();
                    h.put(type, value);
                }
            }

        }
        return h;
    }

    public void removeVM(String name) {
        vmToType.remove(name);
    }

    private boolean hasValidInstances(HashMap<String, int[]> vm) {
        int validCount = 0;
        for (int[] amounts : vm.values()) {
            validCount += amounts[0];
        }
        return validCount == 0;
    }

    public HashMap<CloudMethodResourceDescription, Integer> getComposition(String name) {
        HashMap<String, int[]> vm = vmToType.get(name);
        HashMap<CloudMethodResourceDescription, Integer> composition = new HashMap<>();
        for (Entry<String, int[]> entry : vm.entrySet()) {
            String typeName = entry.getKey();
            int[] counts = entry.getValue();
            Type type = types.get(typeName);
            if (type != null && counts[0] > 0) {
                composition.put(type.getResourceDescription(), counts[0]);
            }
        }
        return composition;
    }

    public void newCoreElementsDetected(List<Integer> newCores) {
        int coreCount = CoreManager.getCoreCount();
        for (Type type : types.values()) {
            int[] slotsC = new int[coreCount];
            int[][] slotsI = new int[coreCount][];
            // Copy actual values
            int[] slotsCore = type.getSlotsCore();
            System.arraycopy(slotsCore, 0, slotsC, 0, slotsCore.length);
            for (int i = 0; i < type.getSlotsImplLength(); ++i) {
                int[] slotsImpl = type.getSpecificSlotsImpl(i);
                slotsI[i] = new int[slotsImpl.length];
                System.arraycopy(slotsImpl, 0, slotsI[i], 0, slotsImpl.length);
            }
            // Get new values
            for (int coreId : newCores) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                int implsSize = impls.size();
                slotsI[coreId] = new int[implsSize];
                for (int implId = 0; implId < implsSize; ++implId) {
                    Implementation impl = impls.get(implId);
                    if (impl.getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription rd = (MethodResourceDescription) impl.getRequirements();
                        Integer into = type.getResourceDescription().canHostSimultaneously(rd);
                        slotsC[coreId] = Math.max(slotsC[coreId], into);
                        slotsI[coreId][implId] = into;
                    }
                }
            }
            type.setSlotsCore(slotsC);
            type.setSlotsImpl(slotsI);
        }
    }

    public String getCurrentState(String prefix) {
        int coreCount = CoreManager.getCoreCount();
        StringBuilder sb = new StringBuilder();
        // Types
        sb.append(prefix).append("TYPES = [").append("\n");
        for (java.util.Map.Entry<String, Type> type : types.entrySet()) {
            sb.append(prefix).append("\t").append("TYPE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("KEY = ").append(type.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("CORES = [").append("\n");
            for (int i = 0; i < coreCount; i++) {
                sb.append(prefix).append("\t").append("\t").append("\t").append("CORE = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SLOTS = ")
                        .append(type.getValue().getSpecificSlotsCore(i)).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");

            sb.append(prefix).append("\t").append("\t").append("IMPLEMENTATIONS = [").append("\n");
            for (int i = 0; i < coreCount; ++i) {
                for (int j = 0; j < CoreManager.getNumberCoreImplementations(i); ++j) {
                    sb.append(prefix).append("\t").append("\t").append("\t").append("IMPLEMENTATION = [").append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("IMPLID = ").append(j).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SLOTS = ")
                            .append(type.getValue().getSpecificSlotsImpl(i, j)).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
                }
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");

            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        // Virtual Instances
        sb.append(prefix).append("VIRTUAL_INSTANCES = [").append("\n");
        for (java.util.Map.Entry<String, HashMap<String, int[]>> vm : vmToType.entrySet()) {
            HashMap<String, int[]> composition = vm.getValue();
            sb.append(prefix).append("\t").append("VIRTUAL_INSTANCE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("NAME = ").append(vm.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("COMPONENTS = [").append("\n");
            for (java.util.Map.Entry<String, int[]> component : composition.entrySet()) {
                String componentName = component.getKey();
                int[] amount = component.getValue();
                sb.append(prefix).append("\t").append("\t").append("\t").append("COMPONENT = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("NAME = ").append(componentName).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("AMOUNT = ").append(amount[0]).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

    private class Type {

        private final CloudMethodResourceDescription rd;
        private int[] slotsCore;
        private int[][] slotsImpl;

        public Type(CloudMethodResourceDescription rd) {
            this.rd = rd;

            int coreCount = CoreManager.getCoreCount();
            this.slotsCore = new int[coreCount];
            this.slotsImpl = new int[coreCount][];

            // Get new values
            for (int coreId = 0; coreId < coreCount; coreId++) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                int implsSize = impls.size();
                this.slotsImpl[coreId] = new int[implsSize];
                for (int implId = 0; implId < implsSize; ++implId) {
                    Implementation impl = impls.get(implId);
                    if (impl.getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription reqs = (MethodResourceDescription) impl.getRequirements();
                        Integer into = rd.canHostSimultaneously(reqs);
                        this.slotsCore[coreId] = Math.max(this.slotsCore[coreId], into);
                        this.slotsImpl[coreId][implId] = into;
                    }
                }
            }
        }

        public CloudMethodResourceDescription getResourceDescription() {
            return this.rd;
        }

        public int[] getSlotsCore() {
            return this.slotsCore;
        }

        public int getSpecificSlotsCore(int index) {
            return this.slotsCore[index];
        }

        public void setSlotsCore(int[] slotsCore) {
            this.slotsCore = slotsCore;
        }

        public int[][] getSlotsImpl() {
            return this.slotsImpl;
        }

        public int getSlotsImplLength() {
            return this.slotsImpl.length;
        }

        public int[] getSpecificSlotsImpl(int i) {
            return this.slotsImpl[i];
        }

        public int getSpecificSlotsImpl(int i, int j) {
            return this.slotsImpl[i][j];
        }

        public void setSlotsImpl(int[][] slotsImpl) {
            this.slotsImpl = slotsImpl;
        }
    }

}
