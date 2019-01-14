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
package es.bsc.compss.types;

import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;

import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.EnvironmentLoader;


public abstract class ImplementationDefinition {

    private final String signature;

    public static final ImplementationDefinition defineImplementation(String implType, String implSignature, MethodResourceDescription implConstraints, String... implTypeArgs) throws IllegalArgumentException {
        ImplementationDefinition id = null;

        if (implType.toUpperCase().compareTo(TaskType.SERVICE.toString()) == 0) {
            if (implTypeArgs.length != ServiceImplementation.NUM_PARAMS) {
                throw new IllegalArgumentException("Incorrect parameters for type SERVICE on " + implSignature);
            }
            String namespace = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
            String serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
            String operation = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
            String port = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);

            id = new ServiceDefinition(implSignature, namespace, serviceName, operation, port);

        } else {
            MethodType mt;
            try {
                mt = MethodType.valueOf(implType);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unrecognised method type " + implType);
            }
            switch (mt) {
                case METHOD:
                    if (implTypeArgs.length != MethodImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type METHOD on " + implSignature);
                    }
                    String declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String methodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (declaringClass == null || declaringClass.isEmpty()) {
                        throw new IllegalArgumentException("Empty declaringClass annotation for method " + implSignature);
                    }
                    if (methodName == null || methodName.isEmpty()) {
                        throw new IllegalArgumentException("Empty methodName annotation for method " + implSignature);
                    }
                    id = new MethodDefinition(implSignature, declaringClass, methodName, implConstraints);
                    break;

                case BINARY:
                    if (implTypeArgs.length != BinaryImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type BINARY on " + implSignature);
                    }
                    String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String binaryWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (binary == null || binary.isEmpty()) {
                        throw new IllegalArgumentException("Empty binary annotation for BINARY method " + implSignature);
                    }
                    id = new BinaryDefinition(implSignature, binary, binaryWorkingDir, implConstraints);
                    break;

                case MPI:
                    if (implTypeArgs.length != MPIImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    String mpiBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String mpiWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    if (mpiRunner == null || mpiRunner.isEmpty()) {
                        throw new IllegalArgumentException("Empty mpiRunner annotation for MPI method " + implSignature);
                    }
                    if (mpiBinary == null || mpiBinary.isEmpty()) {
                        throw new IllegalArgumentException("Empty binary annotation for MPI method " + implSignature);
                    }
                    id = new MPIDefinition(implSignature, mpiBinary, mpiWorkingDir, mpiRunner, implConstraints);
                    break;

                case COMPSs:
                    if (implTypeArgs.length != COMPSsImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    String runcompss = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String flags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    String appName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[2]);
                    String compssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[3]);
                    if (appName == null || appName.isEmpty()) {
                        throw new IllegalArgumentException("Empty appName annotation for COMPSs method " + implSignature);
                    }
                    id = new COMPSsDefinition(implSignature, runcompss, flags, appName, compssWorkingDir, implConstraints);
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
                    if (decafRunner == null || decafRunner.isEmpty()) {
                        throw new IllegalArgumentException("Empty mpiRunner annotation for DECAF method " + implSignature);
                    }
                    if (dfScript == null || dfScript.isEmpty()) {
                        throw new IllegalArgumentException("Empty dfScript annotation for DECAF method " + implSignature);
                    }
                    id = new DecafDefinition(implSignature, dfScript, dfExecutor, dfLib, decafWorkingDir, decafRunner, implConstraints);
                    break;

                case OMPSS:
                    if (implTypeArgs.length != OmpSsImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OMPSS on " + implSignature);
                    }
                    String ompssBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String ompssWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (ompssBinary == null || ompssBinary.isEmpty()) {
                        throw new IllegalArgumentException("Empty binary annotation for OmpSs method " + implSignature);
                    }
                    id = new OmpSsDefinition(implSignature, ompssBinary, ompssWorkingDir, implConstraints);
                    break;
                case OPENCL:
                    if (implTypeArgs.length != OpenCLImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OPENCL on " + implSignature);
                    }
                    String openclKernel = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String openclWorkingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (openclKernel == null || openclKernel.isEmpty()) {
                        throw new IllegalArgumentException("Empty kernel annotation for OpenCL method " + implSignature);
                    }
                    id = new OpenCLDefinition(implSignature, openclKernel, openclWorkingDir, implConstraints);
                    break;
                case MULTI_NODE:
                    if (implTypeArgs.length != MultiNodeImplementation.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MultiNode on " + implSignature);
                    }
                    String multiNodeClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
                    String multiNodeName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[1]);
                    if (multiNodeClass == null || multiNodeClass.isEmpty()) {
                        throw new IllegalArgumentException("Empty declaringClass annotation for method " + implSignature);
                    }
                    if (multiNodeName == null || multiNodeName.isEmpty()) {
                        throw new IllegalArgumentException("Empty methodName annotation for method " + implSignature);
                    }
                    id = new MultiNodeDefinition(implSignature, multiNodeClass, multiNodeName, implConstraints);
                    break;
            }

        }
        return id;
    }

    private ImplementationDefinition(String signature) {
        this.signature = signature;
    }

    public String getSignature() {
        return signature;
    }

    public abstract Implementation getImpl(int coreId, int implId);


    private static class MethodDefinition extends ImplementationDefinition {

        private final String declaringClass;
        private final String methodName;
        private final MethodResourceDescription implConstraints;

        private MethodDefinition(String implSignature, String declaringClass, String methodName, MethodResourceDescription implConstraints) {
            super(implSignature);
            this.declaringClass = declaringClass;
            this.methodName = methodName;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new MethodImplementation(declaringClass, methodName, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("METHOD Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Declaring class: ").append(declaringClass).append("\n");
            sb.append("\t Method name: ").append(methodName).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }

    }


    private static class ServiceDefinition extends ImplementationDefinition {

        private final String namespace;
        private final String serviceName;
        private final String operation;
        private final String port;

        private ServiceDefinition(String signature, String namespace, String serviceName, String operation, String port) {
            super(signature);
            this.namespace = namespace;
            this.serviceName = serviceName;
            this.operation = operation;
            this.port = port;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new ServiceImplementation(coreId, namespace, serviceName, port, operation);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("SERVICE Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Namespace: ").append(namespace).append("\n");
            sb.append("\t Service name: ").append(serviceName).append("\n");
            sb.append("\t Operation: ").append(operation).append("\n");
            sb.append("\t Port: ").append(port).append("\n");
            return sb.toString();
        }
    }


    private static class MPIDefinition extends ImplementationDefinition {

        private final String binary;
        private final String workingDir;
        private final String mpiRunner;
        private final MethodResourceDescription implConstraints;

        private MPIDefinition(String signature, String binary, String workingDir, String mpiRunner, MethodResourceDescription implConstraints) {
            super(signature);
            this.binary = binary;
            this.workingDir = workingDir;
            this.mpiRunner = mpiRunner;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new MPIImplementation(binary, workingDir, mpiRunner, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("MPI Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Binary: ").append(binary).append("\n");
            sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class DecafDefinition extends ImplementationDefinition {

        private final String dfScript;
        private final String dfExecutor;
        private final String dfLib;
        private final String workingDir;
        private final String mpiRunner;
        private final MethodResourceDescription implConstraints;

        private DecafDefinition(String signature, String dfScript, String dfExecutor, String dfLib, String workingDir, String mpiRunner, MethodResourceDescription implConstraints) {
            super(signature);
            this.dfScript = dfScript;
            this.dfExecutor = dfExecutor;
            this.dfLib = dfLib;
            this.workingDir = workingDir;
            this.mpiRunner = mpiRunner;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new DecafImplementation(dfScript, dfExecutor, dfLib, workingDir, mpiRunner, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DECAF Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Decaf script: ").append(dfScript).append("\n");
            sb.append("\t Decaf executor: ").append(dfExecutor).append("\n");
            sb.append("\t Decaf lib: ").append(dfLib).append("\n");
            sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class COMPSsDefinition extends ImplementationDefinition {

        private final String runcompss;
        private final String flags;
        private final String appName;
        private final String workingDir;
        private final MethodResourceDescription implConstraints;

        private COMPSsDefinition(String signature, String runcompss, String flags, String appName, String workingDir, MethodResourceDescription implConstraints) {
            super(signature);
            this.runcompss = runcompss;
            this.flags = flags;
            this.appName = appName;
            this.workingDir = workingDir;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new COMPSsImplementation(runcompss, flags, appName, workingDir, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("COMPSs Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Flags: ").append(flags).append("\n");
            sb.append("\t Application name: ").append(appName).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class OmpSsDefinition extends ImplementationDefinition {

        private final String binary;
        private final String workingDir;
        private final MethodResourceDescription implConstraints;

        private OmpSsDefinition(String signature, String binary, String workingDir, MethodResourceDescription implConstraints) {
            super(signature);
            this.binary = binary;
            this.workingDir = workingDir;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new OmpSsImplementation(binary, workingDir, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("OmpSs Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Binary: ").append(binary).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class OpenCLDefinition extends ImplementationDefinition {

        private final String kernel;
        private final String workingDir;
        private final MethodResourceDescription implConstraints;

        private OpenCLDefinition(String signature, String kernel, String workingDir, MethodResourceDescription implConstraints) {
            super(signature);
            this.kernel = kernel;
            this.workingDir = workingDir;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new OpenCLImplementation(kernel, workingDir, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("OpenCL Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Kernel: ").append(kernel).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class BinaryDefinition extends ImplementationDefinition {

        private final String binary;
        private final String workingDir;
        private final MethodResourceDescription implConstraints;

        private BinaryDefinition(String signature, String binary, String workingDir, MethodResourceDescription implConstraints) {
            super(signature);
            this.binary = binary;
            this.workingDir = workingDir;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new BinaryImplementation(binary, workingDir, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Binary Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Binary: ").append(binary).append("\n");
            sb.append("\t Working directory: ").append(workingDir).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }


    private static class MultiNodeDefinition extends ImplementationDefinition {

        private final String multiNodeClass;
        private final String multiNodeName;
        private final MethodResourceDescription implConstraints;

        private MultiNodeDefinition(String signature, String multiNodeClass, String multiNodeName, MethodResourceDescription implConstraints) {
            super(signature);
            this.multiNodeClass = multiNodeClass;
            this.multiNodeName = multiNodeName;
            this.implConstraints = implConstraints;
        }

        @Override
        public Implementation getImpl(int coreId, int implId) {
            return new MultiNodeImplementation(multiNodeClass, multiNodeName, coreId, implId, implConstraints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("MultiNode Implementation \n");
            sb.append("\t Signature: ").append(this.getSignature()).append("\n");
            sb.append("\t Class: ").append(multiNodeClass).append("\n");
            sb.append("\t Name: ").append(multiNodeName).append("\n");
            sb.append("\t Constraints: ").append(implConstraints);
            return sb.toString();
        }
    }

}
