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
package es.bsc.compss.types.annotations;

/**
 * Constants for the Annotations Interface.
 */
public class Constants {

    /*
     * Unassigned value for Strings.
     */
    public static final String UNASSIGNED = "[unassigned]";

    /*
     * Values for task priority.
     */
    public static final String IS_PRIORITARY_TASK = "true";

    public static final String IS_NOT_PRIORITARY_TASK = "false";

    /*
     * Replicated task execution.
     */
    public static final String IS_REPLICATED_TASK = "true";

    public static final String IS_NOT_REPLICATED_TASK = "false";

    /*
     * Distributed task execution.
     */
    public static final String IS_DISTRIBUTED_TASK = "true";

    public static final String IS_NOT_DISTRIBUTED_TASK = "false";

    /*
     * For Nested COMPSs executions.
     */
    public static final String WORKER_IN_MASTER = "true";

    public static final String WORKER_NOT_IN_MASTER = "false";

    /*
     * Fail by exit value
     */
    public static final String FAIL_BY_EV = "true";

    public static final String NOT_FAIL_BY_EV = "false";

    /*
     * CONTAINER EXECUTION
     */
    public static final String BINARY_CONTAINER_EXECUTION = "CET_BINARY";

    public static final String PYTHON_CONTAINER_EXECUTION = "CET_PYTHON";

    /*
     * Single node value.
     */
    public static final int SINGLE_NODE = 1;

    /*
     * Processor types.
     */
    public static final String CPU_TYPE = "CPU";
    public static final String GPU_TYPE = "GPU";
    public static final String FPGA_TYPE = "FPGA";
    public static final String OTHER_TYPE = "OTHER";

    /*
     * Unassigned value for processor type.
     */
    public static final String UNASSIGNED_PROCESSOR_TYPE = CPU_TYPE;

    /*
     * Available hostnames for MPI tasks.
     */
    public static final String COMPSS_HOSTNAMES = "COMPSS_HOSTNAMES";

    /*
     * Number of available workers for MPI tasks.
     */
    public static final String COMPSS_NUM_NODES = "COMPSS_NUM_NODES";

    /*
     * Number of available cores for OmpSs tasks.
     */
    public static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";

    /*
     * Empty prefix for parameters.
     */
    public static final String PREFIX_EMPTY = "null";
    public static final String NAME_EMPTY = "";

    /*
     * Skip prefix for parameters.
     */
    public static final String PREFIX_SKIP = "#";


    // Private constructor to avoid instantiation
    private Constants() {
        throw new UnsupportedOperationException();
    }

}
