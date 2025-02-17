/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.implementations;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.HTTPDefinition;
import es.bsc.compss.types.implementations.definition.ImplementationDefinition;
import es.bsc.compss.types.implementations.definition.JuliaDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MpmdMPIDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.HTTPResourceDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


/**
 * Class that contains all the necessary information to generate the actual implementation object.
 *
 * @param <T> Type of resource description compatible with the implementation
 */
public class ImplementationDescription<T extends WorkerResourceDescription, D extends ImplementationDefinition>
    implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    private String signature;
    private boolean isLocal;
    private T constraints;
    private D implDefinition;

    private ExecType prolog;
    private ExecType epilog;


    /**
     * Creates a new implementation from the given parameters.
     *
     * @param <T> Type of resource description compatible with the implementation
     * @param implType Implementation type.
     * @param implSignature Implementation signature.
     * @param localProcessing Implementation must run on the local computing devices.
     * @param implConstraints Implementation constraints.
     * @param container Container description.
     * @param implTypeArgs Implementation specific arguments.
     * @return A new implementation definition from the given parameters.
     * @throws IllegalArgumentException If the number of specific parameters does not match the required number of
     *             parameters by the implementation type.
     */
    @SuppressWarnings("unchecked")
    public static final <T extends WorkerResourceDescription, D extends ImplementationDefinition>
        ImplementationDescription<T, D> defineImplementation(String implType, String implSignature,
            boolean localProcessing, T implConstraints, ExecType prolog, ExecType epilog,
            ContainerDescription container, String... implTypeArgs) throws IllegalArgumentException {

        ImplementationDescription<T, D> id = null;
        if (implType.toUpperCase().compareTo(TaskType.HTTP.toString()) == 0) {
            if (implTypeArgs.length != HTTPDefinition.NUM_PARAMS) {
                throw new IllegalArgumentException("Incorrect parameters for type HTTP on " + implSignature);
            }
            String serviceName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[0]);
            List<String> servicesList = new ArrayList<String>();
            servicesList.add(serviceName);
            id = new ImplementationDescription<>((D) new HTTPDefinition(implTypeArgs, 0), implSignature,
                localProcessing, (T) new HTTPResourceDescription(servicesList, 1), prolog, epilog);
        } else {
            MethodType mt;
            try {
                mt = MethodType.valueOf(implType);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unrecognised method type " + implType);
            }

            switch (mt) {
                case METHOD:
                    if (implTypeArgs.length != MethodDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type METHOD on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new MethodDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case PYTHON_MPI:
                    if (implTypeArgs.length < PythonMPIDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException(
                            "Incorrect parameters for type PYTHON_MPI on " + implSignature);
                    }
                    PythonMPIDefinition pyMPIDef = new PythonMPIDefinition(implTypeArgs, 0);
                    implConstraints.scaleUpBy(pyMPIDef.getPPN());
                    id = new ImplementationDescription<>((D) pyMPIDef, implSignature, localProcessing, implConstraints,
                        prolog, epilog);
                    break;

                case CONTAINER:
                    if (implTypeArgs.length != ContainerDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException(
                            "Incorrect parameters for type CONTAINER on " + implSignature);
                    }
                    id = new ImplementationDescription<>((D) new ContainerDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case BINARY:
                    if (implTypeArgs.length != BinaryDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type BINARY on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new BinaryDefinition(implTypeArgs, 0, container),
                        implSignature, localProcessing, implConstraints, prolog, epilog);
                    break;

                case MPI:
                    if (implTypeArgs.length != MPIDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPI on " + implSignature);
                    }
                    MPIDefinition mpiDef = new MPIDefinition(implTypeArgs, 0, container);
                    implConstraints.scaleUpBy(mpiDef.getPPN());
                    id = new ImplementationDescription<>((D) mpiDef, implSignature, localProcessing, implConstraints,
                        prolog, epilog);
                    break;

                case MPMDMPI:
                    if (implTypeArgs.length < MpmdMPIDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type MPMDMPI on " + implSignature);
                    }
                    MpmdMPIDefinition mpmdDef = new MpmdMPIDefinition(implTypeArgs, 0, container);
                    implConstraints.scaleUpBy(mpmdDef.getPPN());
                    id = new ImplementationDescription<>((D) mpmdDef, implSignature, localProcessing, implConstraints,
                        prolog, epilog);
                    break;

                case COMPSs:
                    if (implTypeArgs.length != COMPSsDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type COMPSS on " + implSignature);
                    }
                    id = new ImplementationDescription<>((D) new COMPSsDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case DECAF:
                    if (implTypeArgs.length != DecafDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type DECAF on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new DecafDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case JULIA:
                    if (implTypeArgs.length != JuliaDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type JULIA on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new JuliaDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case OMPSS:
                    if (implTypeArgs.length != OmpSsDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OMPSS on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new OmpSsDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case OPENCL:
                    if (implTypeArgs.length != OpenCLDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException("Incorrect parameters for type OPENCL on " + implSignature);
                    }

                    id = new ImplementationDescription<>((D) new OpenCLDefinition(implTypeArgs, 0), implSignature,
                        localProcessing, implConstraints, prolog, epilog);
                    break;

                case MULTI_NODE:
                    if (implTypeArgs.length != MultiNodeDefinition.NUM_PARAMS) {
                        throw new IllegalArgumentException(
                            "Incorrect parameters for type MultiNode on " + implSignature);
                    }
                    MultiNodeDefinition mnDef = new MultiNodeDefinition(implTypeArgs, 0);
                    implConstraints.scaleUpBy(mnDef.getPPN());

                    id = new ImplementationDescription<>((D) mnDef, implSignature, localProcessing, implConstraints,
                        prolog, epilog);
                    break;
            }
        }
        return id;
    }

    public ImplementationDescription() {
        // For serialization
    }

    /**
     * Constructs a new ImplementationDefinition with the signature and the requirements passed in as parameters.
     *
     * @param implDefinition whether the implementation has to run on the local node or not.
     * @param signature Signature of the implementation.
     * @param constraints requirements to run the implementation
     */
    public ImplementationDescription(D implDefinition, String signature, boolean localProcessing, T constraints,
        ExecType prolog, ExecType epilog) {
        this.signature = signature;
        this.isLocal = localProcessing;
        this.constraints = constraints;
        this.implDefinition = implDefinition;
        this.prolog = prolog;
        this.epilog = epilog;
    }

    /**
     * Returns the implementation signature.
     *
     * @return The implementation signature.
     */
    public String getSignature() {
        return signature;
    }

    public ExecType getProlog() {
        return this.prolog;
    }

    public ExecType getEpilog() {
        return this.epilog;
    }

    /**
     * Returns whether the implementation is to be run locally or can be offloaded.
     *
     * @return {@literal true} if the implementation is to be run locally; {@literal false} otherwise
     */
    public boolean isLocal() {
        return this.isLocal;
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
    @SuppressWarnings("unchecked")
    public Implementation getImpl(int coreId, int implId) {
        switch (implDefinition.getTaskType()) {
            case HTTP:
                return new HTTPImplementation(coreId, implId,
                    (ImplementationDescription<HTTPResourceDescription, HTTPDefinition>) this);
            default: // case METHOD
                return new AbstractMethodImplementation(coreId, implId, (ImplementationDescription<
                    MethodResourceDescription, AbstractMethodImplementationDefinition>) this);
        }
    }

    public D getDefinition() {
        return implDefinition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.signature = (String) in.readObject();
        this.isLocal = in.readBoolean();
        this.constraints = (T) in.readObject();
        this.implDefinition = (D) in.readObject();
        this.prolog = (ExecType) in.readObject();
        this.epilog = (ExecType) in.readObject();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.signature);
        out.writeBoolean(this.isLocal);
        out.writeObject(this.constraints);
        out.writeObject(this.implDefinition);
        out.writeObject(this.prolog);
        out.writeObject(this.epilog);
    }

    /**
     * Returns a JSON representation of the implementation description.
     * 
     * @return JSON representation
     */
    public final String toJSON() {
        return "{" + "\"signature\":\"" + this.signature + "\"," + "\"local\":" + this.isLocal + ","
            + "\"constraints\":" + this.constraints + "," + "\"definition\":" + this.implDefinition.toJSON() + ","
            + "\"prolog\":" + this.prolog + "," + "\"epilog\":" + this.epilog + "}";
    }

    @Override
    public String toString() {
        return this.implDefinition.toShortFormat();
    }
}
