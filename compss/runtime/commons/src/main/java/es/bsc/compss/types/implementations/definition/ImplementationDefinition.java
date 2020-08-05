/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.types.implementations.PythonMPIImplementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.ContainerDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.EnvironmentLoader;


/**
 * Class that contains all the necessary information to generate the actual implementation object.
 *
 * @param <T> Type of resource description compatible with the implementation
 */
public abstract class ImplementationDefinition<T extends ResourceDescription> {

    private final String signature;
    private final T constraints;


    /**
     * Creates a new implementation from the given parameters.
     *
     * @param <T> Type of resource description compatible with the implementation
     * @param implType Implementation type.
     * @param implSignature Implementation signature.
     * @param implConstraints Implementation constraints.
     * @param implTypeArgs Implementation specific arguments.
     * @return A new implementation definition from the given parameters.
     * @throws IllegalArgumentException If the number of specific parameters does not match the required number of
     *             parameters by the implementation type.
     */
    @SuppressWarnings("unchecked")
    public static final <T extends ResourceDescription> ImplementationDefinition<T>
        defineImplementation(String implType, String implSignature, T implConstraints, String... implTypeArgs)
            throws IllegalArgumentException {

        ImplementationDefinition<T> id = null;

        if (implType.toUpperCase().compareTo(TaskType.SERVICE.toString()) == 0) {
            if (implTypeArgs.length != ServiceImplementation.NUM_PARAMS) {
                throw new IllegalArgumentException("Incorrect parameters for type SERVICE on " + implSignature);
            }
            String namespace = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
            String serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
            String operation = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
            String port = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);

            id = (ImplementationDefinition<T>) new ServiceDefinition(implSignature, namespace, serviceName, operation,
                port);

        } else {
            MethodType mt;
            try {
                mt = MethodType.valueOf(implType);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unrecognised method type " + implType);
            }
            switch (mt) {

                case CONTAINER:
                    if (implTypeArgs.length != ContainerImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException(
                            "Incorrect parameters for type CONTAINER on " + implSignature);
                    }
                    String internalTypec = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String internalFuncc = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String internalBinaryc = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);

                    String binaryWorkingDirc = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    boolean binaryfailByEVc = Boolean.parseBoolean(implTypeArgs[4]);

                    ContainerDescription containerc =
                        new ContainerDescription(EnvironmentLoader.loadFromEnvironment(implTypeArgs[5]),
                            EnvironmentLoader.loadFromEnvironment(implTypeArgs[6]));

                    if ((internalBinaryc == null || internalBinaryc.isEmpty() || internalBinaryc == "[unassigned]") && internalTypec == "BINARY") {
                        throw new IllegalArgumentException(
                            "Empty binary annotation for CONTAINER method " + implSignature);
                    }

                    if ((internalFuncc == null || internalFuncc.isEmpty() || internalFuncc == "[unassigned]") && internalTypec == "PYTHON") {
                        throw new IllegalArgumentException(
                            "Empty python function annotation for CONTAINER method " + implSignature);
                    }

                    id = (ImplementationDefinition<T>) new ContainerDefinition(implSignature, internalTypec,
                        internalFuncc, internalBinaryc, binaryWorkingDirc, binaryfailByEVc, containerc,
                        (MethodResourceDescription) implConstraints);
                    break;
                case METHOD:
                    if (implTypeArgs.length != MethodImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type METHOD on " + implSignature);
                    }
                    String declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String methodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (declaringClass == null || declaringClass.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty declaringClass annotation for method " + implSignature);
                    }
                    if (methodName == null || methodName.isEmpty()) {
                        throw new IllegalArgumentException("Empty methodName annotation for method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new MethodDefinition(implSignature, declaringClass, methodName,
                        (MethodResourceDescription) implConstraints);
                    break;

                case PYTHON_MPI:
                    // 3 required parameters: declaringClass, methodName, mpi runner
                    if (implTypeArgs.length < PythonMPIImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    String pythonMPIdeclaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String pythonMPImethodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String pythonMPIWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String pythonMPIRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    String pythonMPIFlags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[4]);
                    boolean pythonMPIscaleByCU = Boolean.parseBoolean(implTypeArgs[5]);
                    boolean pythonMPIfailByEV = Boolean.parseBoolean(implTypeArgs[6]);
                    String pythonMPILayoutParam = EnvironmentLoader.loadFromEnvironment(implTypeArgs[7]);
                    int pythonMPIBlockSize = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[8]));
                    int pythonMPIBlockLen = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[9]));
                    int pythonMPIBlockStride =
                        Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[10]));
                    if (pythonMPIdeclaringClass == null || pythonMPIdeclaringClass.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty declaringClass annotation for method " + implSignature);
                    }
                    if (pythonMPImethodName == null || pythonMPImethodName.isEmpty()) {
                        throw new IllegalArgumentException("Empty methodName annotation for method " + implSignature);
                    }

                    id = (ImplementationDefinition<T>) new PythonMPIDefinition(implSignature, pythonMPIdeclaringClass,
                        pythonMPImethodName, pythonMPIWorkingDir, pythonMPIRunner, pythonMPIFlags, pythonMPIscaleByCU,
                        pythonMPIfailByEV, pythonMPILayoutParam, pythonMPIBlockSize, pythonMPIBlockLen,
                        pythonMPIBlockStride, (MethodResourceDescription) implConstraints);
                    break;

                case BINARY:
                    if (implTypeArgs.length != BinaryImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type BINARY on " + implSignature);
                    }

                    String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String binaryWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    boolean binaryfailByEV = Boolean.parseBoolean(implTypeArgs[4]);
                    
                    if (binary == null || binary.isEmpty() || binary == "[unassigned]") {
                        throw new IllegalArgumentException(
                            "Empty binary annotation for BINARY method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new BinaryDefinition(implSignature, binary, binaryWorkingDir,
                        binaryfailByEV, (MethodResourceDescription) implConstraints);
                    break;

                case MPI:
                    if (implTypeArgs.length != MPIImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    String mpiBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String mpiWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String mpiFlags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    boolean mpiScaleByCU = Boolean.parseBoolean(implTypeArgs[4]);
                    boolean mpiFailByEV = Boolean.parseBoolean(implTypeArgs[5]);
                    if (mpiRunner == null || mpiRunner.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty mpiRunner annotation for MPI method " + implSignature);
                    }
                    if (mpiBinary == null || mpiBinary.isEmpty()) {
                        throw new IllegalArgumentException("Empty binary annotation for MPI method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new MPIDefinition(implSignature, mpiBinary, mpiWorkingDir,
                        mpiRunner, mpiFlags, mpiScaleByCU, mpiFailByEV, (MethodResourceDescription) implConstraints);
                    break;

                case COMPSs:
                    if (implTypeArgs.length != COMPSsImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    String runcompss = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String flags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String appName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String workerInMaster = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    String compssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[4]);
                    boolean compssFailByEV = Boolean.parseBoolean(implTypeArgs[5]);
                    if (appName == null || appName.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty appName annotation for COMPSs method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new COMPSsDefinition(implSignature, runcompss, flags, appName,
                        workerInMaster, compssWorkingDir, compssFailByEV, (MethodResourceDescription) implConstraints);
                    break;

                case DECAF:
                    if (implTypeArgs.length != DecafImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type DECAF on " + implSignature);
                    }
                    String dfScript = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String dfExecutor = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String dfLib = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String decafWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    String decafRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[4]);
                    boolean decafFailByEV = Boolean.parseBoolean(implTypeArgs[5]);
                    if (decafRunner == null || decafRunner.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty mpiRunner annotation for DECAF method " + implSignature);
                    }
                    if (dfScript == null || dfScript.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty dfScript annotation for DECAF method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new DecafDefinition(implSignature, dfScript, dfExecutor, dfLib,
                        decafWorkingDir, decafRunner, decafFailByEV, (MethodResourceDescription) implConstraints);
                    break;

                case OMPSS:
                    if (implTypeArgs.length != OmpSsImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OMPSS on " + implSignature);
                    }
                    String ompssBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String ompssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    boolean ompssFailByEV = Boolean.parseBoolean(implTypeArgs[2]);
                    if (ompssBinary == null || ompssBinary.isEmpty()) {
                        throw new IllegalArgumentException("Empty binary annotation for OmpSs method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new OmpSsDefinition(implSignature, ompssBinary, ompssWorkingDir,
                        ompssFailByEV, (MethodResourceDescription) implConstraints);
                    break;

                case OPENCL:
                    if (implTypeArgs.length != OpenCLImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OPENCL on " + implSignature);
                    }
                    String openclKernel = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String openclWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (openclKernel == null || openclKernel.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty kernel annotation for OpenCL method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new OpenCLDefinition(implSignature, openclKernel,
                        openclWorkingDir, (MethodResourceDescription) implConstraints);
                    break;

                case MULTI_NODE:
                    if (implTypeArgs.length != MultiNodeImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException(
                            "Incorrect parameters for type MultiNode on " + implSignature);
                    }
                    String multiNodeClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String multiNodeName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (multiNodeClass == null || multiNodeClass.isEmpty()) {
                        throw new IllegalArgumentException(
                            "Empty declaringClass annotation for method " + implSignature);
                    }
                    if (multiNodeName == null || multiNodeName.isEmpty()) {
                        throw new IllegalArgumentException("Empty methodName annotation for method " + implSignature);
                    }
                    id = (ImplementationDefinition<T>) new MultiNodeDefinition(implSignature, multiNodeClass,
                        multiNodeName, (MethodResourceDescription) implConstraints);
                    break;
            }

        }
        return id;
    }

    /**
     * Constructs a new ImplementationDefinition with the signature and the requirements passed in as parameters.
     *
     * @param signature Signature of the implementation.
     * @param constraints requirements to run the implementation
     */
    protected ImplementationDefinition(String signature, T constraints) {
        this.signature = signature;
        this.constraints = constraints;
    }

    /**
     * Returns the implementation signature.
     *
     * @return The implementation signature.
     */
    public String getSignature() {
        return signature;
    }

    /**
     * Returns the requirements to run the implementation.
     *
     * @return description of the resource features required to run the implementation
     */
    public T getConstraints() {
        return constraints;
    }

    /**
     * Returns the associated implementation with core Id {@code coreId} and implementation Id {@code implId}.
     *
     * @param coreId Core Id.
     * @param implId Implementation Id.
     * @return The associated implementation.
     */
    public abstract Implementation getImpl(int coreId, int implId);
}
