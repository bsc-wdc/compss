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

/**
 * Enum matching the different method types.
 */
public enum MethodType {
    METHOD, // For native methods
    BINARY, // For binary methods
    MPI, // For MPI methods
    MPMDMPI, // For Multi Program MPI methods
    PYTHON_MPI, // For PYTHON MPI methods
    COMPSs, // For COMPSs nested applications
    DECAF, // For decaf methods
    JULIA, // For julia methods
    MULTI_NODE, // For native multi-node methods
    OMPSS, // For OmpSs methods
    OPENCL, // For OpenCL methods
    CONTAINER // For container methods
}
