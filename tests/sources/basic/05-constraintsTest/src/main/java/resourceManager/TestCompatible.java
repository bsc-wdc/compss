package resourceManager;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.ServiceImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.ServiceResourceDescription;
import integratedtoolkit.types.resources.ServiceWorker;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.components.Processor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import commons.Action;


/*
 * Checks the resources information stored inside the Runtime.
 * Takes the project.xml / resources information and the ITF information
 * Checks the mapping between ITF CoreElement and runnable resources (static constraint check)
 */
public class TestCompatible {
	
    //Interface data
    private static int coreCountItf;

	
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
        
        // Run Compatible Resource Manager Test
        System.out.println("[LOG] Running Compatible Resource Manager Test");
        resourceManagerTest();
    }
    
    /* **************************************
     * RESOURCE MANAGER TEST IMPLEMENTATION 
     * ************************************** */
    private static void resourceManagerTest() {
        //Check for each implementation the correctness of its resources
        System.out.println("[LOG] Number of cores = " + coreCountItf);
        for (int coreId = 0; coreId < coreCountItf; coreId++) {
            System.out.println("[LOG] Checking Core" + coreId);
            Action a = new Action(coreId);
            try {
                HashMap<Worker<?>, LinkedList<Implementation<?>>> m = a.findAvailableWorkers();
                checkCoreResources(coreId, m);
            } catch (Exception e) {
                System.out.println("Action " + a);
                e.printStackTrace();
            }
        }

    }

    private static void checkCoreResources(int coreId, HashMap<Worker<?>, LinkedList<Implementation<?>>> hm) {
        //Revert Map
        HashMap<Implementation<?>, LinkedList<Worker<?>>> hm_reverted = new HashMap<Implementation<?>, LinkedList<Worker<?>>>();
        for (Entry<Worker<?>, LinkedList<Implementation<?>>> entry_hm : hm.entrySet()) {
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
            MethodResourceDescription wDescription = (MethodResourceDescription) worker.getDescription();
            
            /* ***********************************************
             * COMPUTING UNITS
             * ***********************************************/
            if ( (iDescription.getTotalComputingUnits() != MethodResourceDescription.UNASSIGNED_INT)
            		&& (wDescription.getTotalComputingUnits() != MethodResourceDescription.UNASSIGNED_INT)
            		&& (wDescription.getTotalComputingUnits() < iDescription.getTotalComputingUnits()) ) {
            	return "computingUnits";
            }
            
            /* ***********************************************
             * PROCESSOR
             * ***********************************************/
            for (Processor ip : iDescription.getProcessors()) {
            	// Check if processor can be executed in worker
            	boolean canBeHosted = false;
            	for (Processor wp : wDescription.getProcessors()) {
            		// Static checks
            		if (wp.getSpeed() < ip.getSpeed()) {
            			continue;
            		}
            		if (!wp.getArchitecture().equals(ip.getArchitecture())) {
            			continue;
            		}
            		if ( (!ip.getPropName().equals(MethodResourceDescription.UNASSIGNED_STR))
            				&& (!ip.getPropName().equals(wp.getPropName())) ) {
            			continue;
            		}
            		if ( (!ip.getPropValue().equals(MethodResourceDescription.UNASSIGNED_STR))
            				&& (!ip.getPropValue().equals(wp.getPropValue())) ) {
            			continue;
            		}
            		
            		// Dynamic checks
            		if (wp.getComputingUnits() > ip.getComputingUnits()) {
            			canBeHosted = true;
            			break;
            		}
            	}
            	if (!canBeHosted) {
            		return "processor";
            	}
            }

            
            /* ***********************************************
             * MEMORY
             * ***********************************************/
            if ( (iDescription.getMemorySize() != MethodResourceDescription.UNASSIGNED_FLOAT)
            		&& (wDescription.getMemorySize() != MethodResourceDescription.UNASSIGNED_FLOAT)
            		&& (wDescription.getMemorySize() < iDescription.getMemorySize()) ) {
            	return "memorySize";
            }
            
            if ( (!iDescription.getMemoryType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!iDescription.getMemoryType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!wDescription.getMemoryType().equals(iDescription.getMemoryType())) ) {
            	return "memoryType";
            }
            
            /* ***********************************************
             * STORAGE
             * ***********************************************/
            if ( (iDescription.getStorageSize() != MethodResourceDescription.UNASSIGNED_FLOAT)
            		&& (wDescription.getStorageSize() != MethodResourceDescription.UNASSIGNED_FLOAT)
            		&& (wDescription.getStorageSize() < iDescription.getStorageSize()) ) {
            	return "storageSize";
            }
            
            if ( (!iDescription.getStorageType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!iDescription.getStorageType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!wDescription.getStorageType().equals(iDescription.getStorageType())) ) {
            	return "storageType";
            }
            
            /* ***********************************************
             * OPERATING SYSTEM
             * ***********************************************/
            if ( (!iDescription.getOperatingSystemType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!iDescription.getOperatingSystemType().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!wDescription.getOperatingSystemType().equals(iDescription.getOperatingSystemType())) ) {
            	return "operatingSystemType";
            }
            
            if ( (!iDescription.getOperatingSystemDistribution().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!iDescription.getOperatingSystemDistribution().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!wDescription.getOperatingSystemDistribution().equals(iDescription.getOperatingSystemDistribution())) ) {
            	return "operatingSystemDistribution";
            }
    
            if ( (!iDescription.getOperatingSystemVersion().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!iDescription.getOperatingSystemVersion().equals(MethodResourceDescription.UNASSIGNED_STR))
            		&& (!wDescription.getOperatingSystemVersion().equals(iDescription.getOperatingSystemVersion())) ) {
            	return "operatingSystemVersion";
            }
            
            /* ***********************************************
             * APPLICATION SOFTWARE
             * ***********************************************/
            if (!(iDescription.getAppSoftware().isEmpty())
                    && !(wDescription.getAppSoftware().containsAll(iDescription.getAppSoftware()))) {
                return "appSoftware";
            }
            
            /* ***********************************************
             * HOST QUEUE
             * ***********************************************/
            if (!(iDescription.getHostQueues().isEmpty())
                    && !(wDescription.getHostQueues().containsAll(iDescription.getHostQueues()))) {
                return "hostQueues";
            }

        } else if (resource.getType() == Worker.Type.SERVICE) {
            ServiceImplementation mImpl = (ServiceImplementation) impl;
            ServiceResourceDescription iDescription = mImpl.getRequirements();
            ServiceWorker worker = (ServiceWorker) resource;
            ServiceResourceDescription wDescription = (ServiceResourceDescription) worker.getDescription();

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

        
        /* ***********************************************
         * ALL CONSTAINT VALUES OK
         * ***********************************************/
        return null;
    }
    
}
