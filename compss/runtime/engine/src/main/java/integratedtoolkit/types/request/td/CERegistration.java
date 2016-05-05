package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.concurrent.Semaphore;

import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.util.ResourceManager;

import java.util.LinkedList;


public class CERegistration<P extends Profile, T extends WorkerResourceDescription> extends TDRequest<P,T> {

    private Semaphore sem;
    private final String signature;
    private final String declaringClass;
    private final MethodResourceDescription mrd;
    
    public CERegistration(String signature, String declaringClass, MethodResourceDescription mrd, Semaphore sem) {
        this.signature = signature;
        this.declaringClass = declaringClass;
        this.mrd = mrd;
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done
     *
     * @return Semaphore where to synchronize until the operation is done
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the operation is done
     *
     * @param sem Semaphore where to synchronize until the operation is done
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(TaskScheduler<P,T> ts) {
        int coreId = CoreManager.getCoreId(new String[]{signature});

        int implementationId = 0; // python can just have 1 implementation due to lack of interfaces
        
        MethodImplementation me = new MethodImplementation(declaringClass, coreId, implementationId, mrd);
        Implementation<?>[] impls = new Implementation[]{me};
        
        CoreManager.registerImplementations (coreId, impls);
        
        LinkedList<Integer> newCores = new LinkedList<Integer>();
        newCores.add(coreId);
        
        ResourceManager.coreElementUpdates(newCores);
        
        ts.coreElementsUpdated();
        
        logger.debug("Data structures resized and CE-resources links updated");
        sem.release();
    }
    @Override
    public TDRequestType getType(){
        return TDRequestType.CE_REGISTRATION;
    }
}
