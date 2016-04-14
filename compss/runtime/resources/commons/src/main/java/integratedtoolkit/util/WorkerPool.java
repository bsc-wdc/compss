package integratedtoolkit.util;

import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Worker;
import java.util.Collection;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeSet;


public class WorkerPool {

    //Resource Sets:
    //  Static Resources (read from xml)
    private HashMap<String, Worker<?>> staticSet;
    //  Critical Resources (can't be destroyed by periodical resource policy)
    private HashMap<String, CloudMethodWorker> criticalSet;
    //  Non Critical Resources (can be destroyed by periodical resource policy)
    private HashMap<String, CloudMethodWorker> nonCriticalSet;

    //Map: coreId -> List <names of the resources where core suits>
    private LinkedList<Worker<?>>[] coreToResource;
    //Map: coreId -> maxTaskCount accepted for that core
    private int[] coreMaxTaskCount;
    //TreeSet : Priority on criticalSet based on cost
    private TreeSet<CloudMethodWorker> criticalOrder;
    //Map: coreId -> List <Implementations -> List <names of the resources where implementation suits> >

    //public static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(integratedtoolkit.log.Loggers.RESOURCES);
    public WorkerPool(int coreCount) {
    	synchronized(this) {
	        staticSet = new HashMap<String, Worker<?>>();
	        criticalSet = new HashMap<String, CloudMethodWorker>();
	        nonCriticalSet = new HashMap<String, CloudMethodWorker>();
	        coreToResource = new LinkedList[coreCount];
	        coreMaxTaskCount = new int[coreCount];
	        for (int i = 0; i < coreCount; i++) {
	            coreToResource[i] = new LinkedList<Worker<?>>();
	            coreMaxTaskCount[i] = 0;
	        }
	        criticalOrder = new TreeSet<CloudMethodWorker>();
    	}
    }

    //Adds a new Resource on the Physical list
    public void addStaticResource(Worker<?> newResource) {
    	synchronized(staticSet) {
    		staticSet.put(newResource.getName(), newResource);
    	}
    }

    //Adds a new Resource on the Critical list
    public void addDynamicResource(CloudMethodWorker newResource) {
    	synchronized(criticalSet) {
    		criticalSet.put(newResource.getName(), newResource);
    	}
    	synchronized(criticalOrder) {
    		criticalOrder.add(newResource);
    	}
    }

    public void coreElementUpdates(LinkedList<Integer> newCores) {
    	synchronized(this) {
	        int oldCoreCount = coreToResource.length;
	        int coreCount = CoreManager.getCoreCount();
	        //UPDATE STRUCTURES SIZES
	        if (coreCount >= oldCoreCount) {
	            //Increase CoreToResource
	            LinkedList<Worker<?>>[] coreToResourceTmp = new LinkedList[coreCount];
	            System.arraycopy(coreToResource, 0, coreToResourceTmp, 0, oldCoreCount);
	            for (int i = coreToResource.length; i < coreCount; i++) {
	                coreToResourceTmp[i] = new LinkedList<Worker<?>>();
	            }
	            coreToResource = coreToResourceTmp;
	
	            //Increase
	            int[] coreMaxTaskCountTmp = new int[coreCount];
	            System.arraycopy(coreMaxTaskCount, 0, coreMaxTaskCountTmp, 0, oldCoreCount);
	            coreMaxTaskCount = coreMaxTaskCountTmp;
	        }
	
	        //UPDATE STRUCTURES' VALUES
	        for (Worker<?> r : staticSet.values()) {
	            r.updatedCoreElements(newCores);
	            int[] slots = r.getSimultaneousTasks();
	            for (Integer coreId : newCores) {
	                if (slots[coreId] > 0) {
	                    coreToResource[coreId].add(r);
	                    coreMaxTaskCount[coreId] += slots[coreId];
	                }
	            }
	        }
	        for (Worker<?> r : criticalSet.values()) {
	            r.updatedCoreElements(newCores);
	            int[] slots = r.getSimultaneousTasks();
	            for (Integer coreId : newCores) {
	                if (slots[coreId] > 0) {
	                    coreToResource[coreId].add(r);
	                    coreMaxTaskCount[coreId] += slots[coreId];
	                }
	            }
	        }
	        for (Worker<?> r : nonCriticalSet.values()) {
	            r.updatedCoreElements(newCores);
	            int[] slots = r.getSimultaneousTasks();
	            for (Integer coreId : newCores) {
	                if (slots[coreId] > 0) {
	                    coreToResource[coreId].add(r);
	                    coreMaxTaskCount[coreId] += slots[coreId];
	                }
	            }
	        }
    	}
    }

    public Collection<Worker<?>> getStaticResources() {
    	synchronized (staticSet) {
    		return staticSet.values();
    	}
    }

    public Worker<?> getStaticResource(String resourceName) {
    	synchronized (staticSet) {
        	return staticSet.get(resourceName);
    	}
    }

    public CloudMethodWorker getDynamicResource(String resourceName) {
        CloudMethodWorker resource = null;
        synchronized(criticalSet) {
        	resource = criticalSet.get(resourceName);
        }
        if (resource == null) {
        	synchronized (nonCriticalSet) {
        		resource = nonCriticalSet.get(resourceName);
        	}
        }
        
        return resource;
    }

    public LinkedList<CloudMethodWorker> getDynamicResources() {
        LinkedList<CloudMethodWorker> resources = new LinkedList<CloudMethodWorker>();
        synchronized(criticalSet) {
        	resources.addAll(criticalSet.values());
        }
        synchronized(nonCriticalSet) {
        	resources.addAll(nonCriticalSet.values());
        }
        
        return resources;
    }

    //returns all the resource information
    public Worker<?> getResource(String resourceName) {
        Worker<?> resource = null;
        synchronized (staticSet) {
        	resource = staticSet.get(resourceName);
        }
        if (resource == null) {
        	synchronized (criticalSet) {
        		resource = criticalSet.get(resourceName);
        	}
        }
        if (resource == null) {
        	synchronized (nonCriticalSet) {
        		resource = nonCriticalSet.get(resourceName);
        	}
        }
        
        return resource;
    }

    public void addResourceLinks(Worker<?> res) {
        int executableImpls = 0;
        int[] slots = res.getSimultaneousTasks();
        synchronized(coreToResource) {
        	synchronized(coreMaxTaskCount) {
		        for (int coreId = 0; coreId < slots.length; coreId++) {
		            if (slots[coreId] > 0) {
		                coreToResource[coreId].add(res);
		                coreMaxTaskCount[coreId] += slots[coreId];
		                executableImpls++;
		            }
		        }
        	}
        }
        if (executableImpls == 0) {
            delete(res);
        }
    }

    public void removeResourceLinks(Worker<?> res) {
        int[] slots = res.getSimultaneousTasks();
        synchronized(coreToResource) {
        	synchronized(coreMaxTaskCount) {
		        for (int coreId = 0; coreId < slots.length; coreId++) {
		            if (slots[coreId] > 0) {
		                coreToResource[coreId].remove(res);
		                coreMaxTaskCount[coreId] -= slots[coreId];
		            }
		        }
        	}
        }
    }

    //Deletes a resource from the pool
    public void delete(Worker<?> resource) {
        String resourceName = resource.getName();
        //Remove resource from sets
        synchronized(criticalSet) {
	        if (criticalSet.remove(resourceName) == null) {
	        	synchronized(nonCriticalSet) {
	        		nonCriticalSet.remove(resourceName);
	        	}
	        }
        }

        // Remove core links & impl links
        int[] simTasks = resource.getSimultaneousTasks();
        synchronized(coreToResource) {
        	synchronized(coreMaxTaskCount) {
		        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
		            if (simTasks[coreId] > 0) {
		                coreToResource[coreId].remove(resource);
		                coreMaxTaskCount[coreId] -= simTasks[coreId];
		            }
		        }
        	}
        }
    }

    public int[] getCoreMaxTaskCount() {
    	synchronized(coreMaxTaskCount) {
    		return coreMaxTaskCount;
    	}
    }

    //Returns a list with all coreIds that can be executed by the resource res
    public List<Integer> getExecutableCores(String res) {
        Worker<?> resource = getResource(res);
        if (resource == null) {
            return new LinkedList<Integer>();
        }
        return resource.getExecutableCores();
    }

    //Selects a subset of the critical set able to execute all the cores
    public void defineCriticalSet() {
    	synchronized(this) {
	    	boolean[] runnable = new boolean[coreToResource.length];
	        for (int i = 0; i < coreToResource.length; i++) {
	            runnable[i] = false;
	        }
	
	        String resourceName;
	        for (Worker<?> res : staticSet.values()) {
	            LinkedList<Integer> cores = res.getExecutableCores();
	            for (int i = 0; i < cores.size(); i++) {
	                runnable[cores.get(i)] = true;
	            }
	        }
	        for (CloudMethodWorker resource : criticalOrder) {
	            resourceName = resource.getName();
	            boolean needed = false;
	            for (int i = 0; i < resource.getExecutableCores().size() && !needed; i++) {
	                needed = needed || !runnable[resource.getExecutableCores().get(i)];
	            }
	            if (needed) {
	                for (int i = 0; i < resource.getExecutableCores().size(); i++) {
	                    runnable[resource.getExecutableCores().get(i)] = true;
	                }
	            } else {
	            	criticalSet.remove(resourceName);
	                criticalOrder.remove(resource);
	                nonCriticalSet.put(resourceName, resource);
	            }
	        }
    	}
    }

    public Collection<CloudMethodWorker> getNonCriticalResources() {
    	synchronized(nonCriticalSet) {
    		return nonCriticalSet.values();
    	}
    }

    public Collection<CloudMethodWorker> getCriticalResources() {
    	synchronized(criticalSet) {
    		return criticalSet.values();
    	}
    }

    //Returns the name of all the resources able to execute coreId
    public List<Worker<?>> findAllResources() {
        LinkedList<Worker<?>> workers = new LinkedList<Worker<?>>();
        synchronized(staticSet) {
	        if (staticSet != null && !staticSet.isEmpty()) {
	        	Object[] arrayStaticSet = staticSet.values().toArray();
	            for (int i = 0; i < arrayStaticSet.length; i++) {
	                workers.add((Worker<?>) arrayStaticSet[i]);
	            }
	        }
        }
        synchronized(criticalSet) {
	        if (criticalSet != null && !criticalSet.isEmpty()) {
	        	Object[] arrayCriticalSet = criticalSet.values().toArray();
	            for (int i = 0; i < arrayCriticalSet.length; i++) {
	                workers.add((Worker<?>) arrayCriticalSet[i]);
	            }
	        }
        }
        
        synchronized(nonCriticalSet) {
	        if (nonCriticalSet != null && !nonCriticalSet.isEmpty()) {
	        	Object[] arrayNonCriticalSet = nonCriticalSet.values().toArray();
	            for (int i = 0; i < arrayNonCriticalSet.length; i++) {
	                workers.add((Worker<?>) arrayNonCriticalSet[i]);
	            }
	        }
        }
        
        return workers;
    }

    //return all the resources able to execute a task of the core
    public LinkedList<Worker<?>> findCompatibleResources(int coreId) {
    	synchronized(coreToResource) {
    		return (LinkedList<Worker<?>>) coreToResource[coreId].clone();
    	}
    }

    public boolean isCriticalRemovalSafe(int[][] slotReductionImpls) {
        int coreCount = CoreManager.getCoreCount();
        //Compute cores from impl
        int[] slotReductionCores = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            for (int implId = 0; implId < CoreManager.getCoreImplementations(coreId).length; ++implId) {
                if (slotReductionImpls[coreId][implId] > slotReductionCores[coreId]) {
                    slotReductionCores[coreId] = slotReductionImpls[coreId][implId];
                }
            }
        }

        int[] slots = new int[coreCount];
        synchronized (criticalSet) {
	        for (Worker<?> r : criticalSet.values()) {
	            int[] resSlots = r.getSimultaneousTasks();
	            for (int coreId = 0; coreId < coreCount; coreId++) {
	                slots[coreId] += resSlots[coreId];
	            }
	        }
        }
        
        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (slotReductionCores[coreId] > 0 && slotReductionCores[coreId] >= slots[coreId]) {
                return false;
            }
        }
        return true;
    }

    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        //Resources
        sb.append(prefix).append("RESOURCES = [").append("\n");
        for (Worker<?> r : staticSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); 				//Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Static").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (Worker<?> r : criticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); 				//Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (Worker<?> r : nonCriticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); 				//Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Non-Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        //Cores
        sb.append(prefix).append("CORES = [").append("\n");
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            sb.append(prefix).append("\t").append("CORE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("ID = ").append(i).append("\n");
            sb.append(prefix).append("\t").append("\t").append("MAXTASKCOUNT = ").append(coreMaxTaskCount[i]).append("\n");
            sb.append(prefix).append("\t").append("\t").append("TORESOURCE = [").append("\n");
            for (Worker<?> r : coreToResource[i]) {
                sb.append(prefix).append("\t").append("\t").append("\t").append("RESOURCE = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("NAME = ").append(r.getName()).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SIMTASKS = ").append(r.getSimultaneousTasks()[i]).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }
}
