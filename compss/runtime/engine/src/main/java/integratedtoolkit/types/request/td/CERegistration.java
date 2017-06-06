package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.EnvironmentLoader;
import integratedtoolkit.util.ErrorManager;

import java.util.concurrent.Semaphore;

import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.implementations.BinaryImplementation;
import integratedtoolkit.types.implementations.DecafImplementation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MPIImplementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.OmpSsImplementation;
import integratedtoolkit.types.implementations.OpenCLImplementation;
import integratedtoolkit.util.ResourceManager;

import java.util.LinkedList;
import java.util.List;


public class CERegistration<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TDRequest<P, T, I> {

    private final String coreElementSignature;
    private final String implSignature;
    private final MethodResourceDescription implConstraints;
    private final MethodType implType;
    private final String[] implTypeArgs;
    private final Semaphore sem;


    /**
     * Creates a new CoreElement registration request
     * 
     * @param signature
     * @param methodName
     * @param declaringClass
     * @param mrd
     * @param sem
     */
    public CERegistration(String coreElementSignature, String implSignature, MethodResourceDescription implConstraints, MethodType implType,
            String[] implTypeArgs, Semaphore sem) {

        this.coreElementSignature = coreElementSignature;
        this.implSignature = implSignature;
        this.implConstraints = implConstraints;

        this.implType = implType;
        this.implTypeArgs = implTypeArgs;

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

    @Override
    public void process(TaskScheduler<P, T, I> ts) {
        // Register the coreElement
        Integer coreId = CoreManager.registerNewCoreElement(coreElementSignature);
        if (coreId == null) {
            // CoreElement already exists, retrieve its id
            coreId = CoreManager.getCoreId(coreElementSignature);
        }

        // Retrieve the new implementation number
        int implId = CoreManager.getNumberCoreImplementations(coreId);

        // Create the implementation
        Implementation<?> m = null;
        switch (implType) {
            case METHOD:
                if (this.implTypeArgs.length != 2) {
                    ErrorManager.error("Incorrect parameters for type METHOD on " + this.coreElementSignature);
                }
                String declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String methodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (declaringClass == null || declaringClass.isEmpty()) {
                    ErrorManager.error("Empty declaringClass annotation for method " + this.coreElementSignature);
                }
                if (methodName == null || methodName.isEmpty()) {
                    ErrorManager.error("Empty methodName annotation for method " + this.coreElementSignature);
                }
                m = new MethodImplementation(declaringClass, methodName, coreId, implId, implConstraints);
                break;
            case MPI:
                if (this.implTypeArgs.length != 3) {
                    ErrorManager.error("Incorrect parameters for type DECAF on " + this.coreElementSignature);
                }
                String mpiBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String mpiWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                String mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                if (mpiRunner == null || mpiRunner.isEmpty()) {
                    ErrorManager.error("Empty mpiRunner annotation for MPI method " + this.coreElementSignature);
                }
                if (mpiBinary == null || mpiBinary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for MPI method " + this.coreElementSignature);
                }
                m = new MPIImplementation(mpiBinary, mpiWorkingDir, mpiRunner, coreId, implId, this.implConstraints);
                break;
            case DECAF:
                if (this.implTypeArgs.length != 5) {
                    ErrorManager.error("Incorrect parameters for type MPI on " + this.coreElementSignature);
                }
                String dfScript = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String dfExecutor = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                String dfLib = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                String decafWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                String decafRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[4]);
                if (decafRunner == null || decafRunner.isEmpty()) {
                    ErrorManager.error("Empty mpiRunner annotation for DECAF method " + this.coreElementSignature);
                }
                if (dfScript == null || dfScript.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for DECAF method " + this.coreElementSignature);
                }
                m = new DecafImplementation(dfScript, dfExecutor, dfLib, decafWorkingDir, decafRunner, coreId, implId, this.implConstraints);
                break;
            case BINARY:
                if (this.implTypeArgs.length != 2) {
                    ErrorManager.error("Incorrect parameters for type BINARY on " + this.coreElementSignature);
                }
                String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String binaryWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (binary == null || binary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for BINARY method " + this.coreElementSignature);
                }
                m = new BinaryImplementation(binary, binaryWorkingDir, coreId, implId, implConstraints);
                break;
            case OMPSS:
                if (this.implTypeArgs.length != 2) {
                    ErrorManager.error("Incorrect parameters for type OMPSS on " + this.coreElementSignature);
                }
                String ompssBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String ompssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (ompssBinary == null || ompssBinary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for OmpSs method " + this.coreElementSignature);
                }
                m = new OmpSsImplementation(ompssBinary, ompssWorkingDir, coreId, implId, implConstraints);
                break;
            case OPENCL:
                if (this.implTypeArgs.length != 2) {
                    ErrorManager.error("Incorrect parameters for type OPENCL on " + this.coreElementSignature);
                }
                String openclKernel = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String openclWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (openclKernel == null || openclKernel.isEmpty()) {
                    ErrorManager.error("Empty kernel annotation for OpenCL method " + this.coreElementSignature);
                }
                m = new OpenCLImplementation(openclKernel, openclWorkingDir, coreId, implId, implConstraints);
                break;
            default:
                ErrorManager.error("Unrecognised implementation type");
                break;
        }

        // Register the new implementation
        List<Implementation<?>> newImpls = new LinkedList<>();
        newImpls.add(m);
        List<String> newSignatures = new LinkedList<>();
        newSignatures.add(implSignature);
        CoreManager.registerNewImplementations(coreId, newImpls, newSignatures);

        // Update the Resources structures
        LinkedList<Integer> newCores = new LinkedList<>();
        newCores.add(coreId);
        ResourceManager.coreElementUpdates(newCores);

        // Update the Scheduler structures
        ts.coreElementsUpdated();

        logger.debug("Data structures resized and CE-resources links updated");
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.CE_REGISTRATION;
    }

}
