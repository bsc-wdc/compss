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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;

import java.util.concurrent.Semaphore;

import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.util.ResourceManager;

import java.util.LinkedList;
import java.util.List;


public class CERegistration extends TDRequest {

    private final String coreElementSignature;
    private final String implSignature;
    private final MethodResourceDescription implConstraints;
    private final MethodType implType;
    private final String[] implTypeArgs;
    private final Semaphore sem;


    /**
     * Creates a new CoreElement registration request
     *
     * @param coreElementSignature
     * @param implSignature
     * @param implConstraints
     * @param implType
     * @param implTypeArgs
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
    public void process(TaskScheduler ts) {
        // Register the coreElement
        Integer coreId = CoreManager.registerNewCoreElement(coreElementSignature);
        if (coreId == null) {
            // CoreElement already exists, retrieve its id
            coreId = CoreManager.getCoreId(coreElementSignature);
        }

        // Retrieve the new implementation number
        Integer implId = CoreManager.getNumberCoreImplementations(coreId);

        // Create the implementation
        Implementation m = null;
        switch (implType) {
            case METHOD:
                if (this.implTypeArgs.length != MethodImplementation.NUM_PARAMS) {
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
            case BINARY:
                if (this.implTypeArgs.length != BinaryImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type BINARY on " + this.coreElementSignature);
                }
                String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String binaryWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (binary == null || binary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for BINARY method " + this.coreElementSignature);
                }
                m = new BinaryImplementation(binary, binaryWorkingDir, coreId, implId, implConstraints);
                break;
            case MPI:
                if (this.implTypeArgs.length != MPIImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type MPI on " + this.coreElementSignature);
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
            case COMPSs:
                if (this.implTypeArgs.length != COMPSsImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type MPI on " + this.coreElementSignature);
                }
                String runcompss = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String flags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                String appName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                String compssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                if (appName == null || appName.isEmpty()) {
                    ErrorManager.error("Empty appName annotation for COMPSs method " + this.coreElementSignature);
                }
                m = new COMPSsImplementation(runcompss, flags, appName, compssWorkingDir, coreId, implId, this.implConstraints);
                break;
            case DECAF:
                if (this.implTypeArgs.length != DecafImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type DECAF on " + this.coreElementSignature);
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
                    ErrorManager.error("Empty dfScript annotation for DECAF method " + this.coreElementSignature);
                }
                m = new DecafImplementation(dfScript, dfExecutor, dfLib, decafWorkingDir, decafRunner, coreId, implId,
                        this.implConstraints);
                break;
            case MULTI_NODE:
                if (this.implTypeArgs.length != MultiNodeImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type MultiNode on " + this.coreElementSignature);
                }
                String multiNodeClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String multiNodeName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (multiNodeClass == null || multiNodeClass.isEmpty()) {
                    ErrorManager.error("Empty declaringClass annotation for method " + this.coreElementSignature);
                }
                if (multiNodeName == null || multiNodeName.isEmpty()) {
                    ErrorManager.error("Empty methodName annotation for method " + this.coreElementSignature);
                }
                m = new MultiNodeImplementation(multiNodeClass, multiNodeName, coreId, implId, this.implConstraints);
                break;
            case OMPSS:
                if (this.implTypeArgs.length != OmpSsImplementation.NUM_PARAMS) {
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
                if (this.implTypeArgs.length != OpenCLImplementation.NUM_PARAMS) {
                    ErrorManager.error("Incorrect parameters for type OPENCL on " + this.coreElementSignature);
                }
                String openclKernel = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                String openclWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                if (openclKernel == null || openclKernel.isEmpty()) {
                    ErrorManager.error("Empty kernel annotation for OpenCL method " + this.coreElementSignature);
                }
                m = new OpenCLImplementation(openclKernel, openclWorkingDir, coreId, implId, implConstraints);
                break;
        }

        // Register the new implementation
        List<Implementation> newImpls = new LinkedList<>();
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

        LOGGER.debug("Data structures resized and CE-resources links updated");
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.CE_REGISTRATION;
    }

}
