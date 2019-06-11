package es.bsc.compss.types.implementations;

/**
 * Enum matching the different method types.
 */
public enum MethodType {
    METHOD, // For native methods
    BINARY, // For binary methods
    MPI, // For MPI methods
    COMPSs, // For COMPSs nested applications
    DECAF, // For decaf methods
    MULTI_NODE, // For native multi-node methods
    OMPSS, // For OmpSs methods
    OPENCL // For OpenCL methods
}
