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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.log.Loggers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CommonMPIDefinition {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    public static final String IB_SUFFIX = "-ib0";
    private static final String ERROR_MPI_RUNNER = "ERROR: Empty runner annotation for MPI method";
    private static final String ERROR_MPI_PPN = "ERROR: Processes per node is lower than 1 for MPI method";
    private static final String DEFAULT_HOSTFILE = "true";
    private static final String DEFAULT_SINGLE_HOST = "true";
    private static final String DEFAULT_HOSTS_FLAG = "-hostfile";
    private static final String DEFAULT_PROCS_SEPARATOR = ""; // Empty means repeating
    private static final String DEFAULT_HOSTS_SEPARATOR = "\n";
    private static final int DEFAULT_PPN = 1;
    protected String mpiRunner;
    protected int ppn = DEFAULT_PPN;
    protected String mpiFlags;
    protected String workingDir;
    protected boolean scaleByCU;
    protected boolean failByEV;

    private String hostsFlag = DEFAULT_HOSTS_FLAG;
    private boolean hostfile = Boolean.parseBoolean(DEFAULT_HOSTFILE);
    private String processesSeparator = DEFAULT_PROCS_SEPARATOR;
    private String hostsSeparator = DEFAULT_HOSTS_SEPARATOR;
    private boolean singleHostNoProcesses = Boolean.parseBoolean(DEFAULT_SINGLE_HOST);


    public CommonMPIDefinition() {
    } // Default constructor

    /**
     * Creates a new MPIImplementation instance from the given parameters.
     * 
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param ppn Processes per node
     * @param scaleByCU Scale by computing units property.
     * @param failByEV Flag to enable failure with EV.
     */
    public CommonMPIDefinition(String workingDir, String mpiRunner, int ppn, String mpiFlags, boolean scaleByCU,
        boolean failByEV) {
        this.mpiRunner = mpiRunner;
        this.mpiFlags = mpiFlags;
        this.workingDir = workingDir;
        this.ppn = ppn;
        this.scaleByCU = scaleByCU;
        this.failByEV = failByEV;
        checkArguments();
    }

    /**
     * Setting the MPI runner properties.
     * 
     * @param installDir COMPSs Installation dir in the execution environment.
     */
    public void setRunnerProperties(String installDir) {
        if (this.mpiRunner.endsWith("srun")) {
            loadMPIType(installDir + COMPSsPaths.REL_MPI_CFGS_DIR + "slurm.properties");
        } else {
            String type = System.getenv(COMPSsConstants.COMPSS_MPIRUN_TYPE);
            if (type != null && !type.isEmpty()) {
                LOGGER.info("Loading MPIRUN type: " + type);
                if (type.startsWith(File.separator)) {
                    loadMPIType(type);
                } else {
                    loadMPIType(installDir + COMPSsPaths.REL_MPI_CFGS_DIR + type + ".properties");
                }
            } else {
                LOGGER.warn("Loading default MPIRUN type. You can modify with " + COMPSsConstants.COMPSS_MPIRUN_TYPE
                    + " environment variable.");
            }
        }
    }

    private void loadMPIType(String file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(fis);
            this.hostfile = Boolean.parseBoolean(loadProperty(props, "hostfile", DEFAULT_HOSTFILE));
            this.hostsFlag = loadProperty(props, "hosts.flag", DEFAULT_HOSTS_FLAG);
            this.processesSeparator = loadProperty(props, "processes.separator", DEFAULT_PROCS_SEPARATOR);
            this.hostsSeparator = loadProperty(props, "hosts.separator", DEFAULT_HOSTS_SEPARATOR);
            this.singleHostNoProcesses =
                Boolean.parseBoolean(loadProperty(props, "single.hosts.no.processes", DEFAULT_SINGLE_HOST));
        } catch (Exception e) {
            LOGGER.warn("Can't load MPIRUN type in " + file + ".\nReason: " + e.getMessage());
        }
    }

    protected String loadProperty(Properties props, String key, String defaultValue) {
        String propValue = props.getProperty(key);
        if (propValue == null) {
            LOGGER.warn("Property " + key + " not found setting default value: " + defaultValue);
            propValue = defaultValue;
        }
        return propValue.replaceAll("\"", "");
    }

    /**
     * Returns the binary working directory.
     * 
     * @return The binary working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    /**
     * Returns the path to the MPI command.
     * 
     * @return The path to the MPI command.
     */
    public String getMpiRunner() {
        return this.mpiRunner;
    }

    /**
     * Returns the flags for the MPI command.
     * 
     * @return Flags for the MPI command.
     */
    public String getMpiFlags() {
        return this.mpiFlags;
    }

    /**
     * Returns the scale by computing units property.
     * 
     * @return scale by computing units property value.
     */
    public boolean getScaleByCU() {
        return this.scaleByCU;
    }

    /**
     * Check if fail by exit value is enabled.
     * 
     * @return True is fail by exit value is enabled.
     */
    public boolean isFailByEV() {
        return failByEV;
    }

    public String getHostsFlag() {
        return hostsFlag;
    }

    public boolean isHostfile() {
        return hostfile;
    }

    public String getProcessesSeparator() {
        return processesSeparator;
    }

    public String getHostsSeparator() {
        return hostsSeparator;
    }

    public int getPPN() {
        return ppn;
    }

    /**
     * Checks if properties of the MPI execution are correct.
     * 
     * @throws IllegalArgumentException When argument is not correct
     */
    public void checkArguments() {
        if (mpiRunner == null || mpiRunner.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MPI_RUNNER);
        }
        if (ppn < 1) {
            throw new IllegalArgumentException(ERROR_MPI_PPN);
        }

    }

    /**
     * Writes the given list of workers to a hostfile inside the given task sandbox.
     *
     * @param taskSandboxWorkingDir task execution sandbox directory
     * @param workers List of workers in mpi hostfile style
     * @return Returns the generated hostfile location inside the task sandbox
     * @throws IOException Exception writting hostfile
     */
    private static String writeHostfile(File taskSandboxWorkingDir, String workers) throws IOException {
        String uuid = UUID.randomUUID().toString();
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + uuid + ".hostfile";
        // Write hostfile
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(workers);
        } catch (IOException ioe) {
            throw ioe;
        }
        return filename;
    }

    /**
     * Writes the given list of workers to a hostfile inside the given task sandbox.
     *
     * @param taskSandboxWorkingDir Task execution sandbox directory.
     * @param workers list of workers in mpi hostfile style.
     * @param computingUnits Number of computing units.
     * @param ppn Number of processes per node.
     * @param procSeparator Hostname-processors separator.
     * @param hostSeparator Deparator between host definitions.
     * @param singleHost Flag to indicate if there is a single host no processors must be set.
     * @return location of the generated hostfile inside the task sandbox.
     * @throws IOException Exception writting hostfile.
     */
    protected static String writeHostfile(File taskSandboxWorkingDir, List<String> workers, int computingUnits, int ppn,
        String procSeparator, String hostSeparator, boolean singleHost) throws IOException {
        String workersStr = buildHostsString(workers, computingUnits, ppn, procSeparator, hostSeparator, singleHost);
        return writeHostfile(taskSandboxWorkingDir, workersStr);
    }

    /**
     * Generate the definition of the hosts used in the mpi (hostfile or argument) according to the configuration.
     * 
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param hostnames List of workers in mpi hostfile style.
     * @param computingUnits Processes per host.
     * @return location of the generated hostfile or the host definition string for the commad line argument.
     * @throws IOException Exception writting hostfile.
     */
    public String generateHostsDefinition(File taskSandboxWorkingDir, List<String> hostnames, int computingUnits)
        throws IOException {
        if (this.hostfile) {
            if (this.scaleByCU) {
                return writeHostfile(taskSandboxWorkingDir, hostnames, computingUnits, ppn, processesSeparator,
                    hostsSeparator, singleHostNoProcesses);
            } else {
                return writeHostfile(taskSandboxWorkingDir, hostnames, 1, ppn, processesSeparator, hostsSeparator,
                    singleHostNoProcesses);

            }
        } else {
            if (this.scaleByCU) {
                return buildHostsString(hostnames, computingUnits, ppn, processesSeparator, hostsSeparator,
                    singleHostNoProcesses);
            } else {
                return buildHostsString(hostnames, 1, ppn, processesSeparator, hostsSeparator, singleHostNoProcesses);

            }
        }
    }

    protected static String buildHostsString(List<String> hostnames, int computingUnits, int ppn, String procSeparator,
        String hostSeparator, boolean singleHost) {
        Map<String, Integer> hosts = new HashMap<String, Integer>();

        for (String hostname : hostnames) {
            // Remove infiniband suffix
            if (hostname.endsWith(IB_SUFFIX)) {
                hostname = hostname.substring(0, hostname.lastIndexOf(IB_SUFFIX));
            }
            hosts.put(hostname, hosts.getOrDefault(hostname, 0) + (computingUnits * ppn));
        }
        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        if (singleHost && hosts.size() == 1) {
            return hosts.keySet().iterator().next();
        }
        for (Entry<String, Integer> e : hosts.entrySet()) {
            String hostStr = genHostString(procSeparator, hostSeparator, e);
            // Add one host name per process to launch
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(hostStr);
            } else {
                hostnamesSTR.append(hostSeparator + hostStr);
            }
        }
        return hostnamesSTR.toString();
    }

    private static String genHostString(String procSeparator, String hostSeparator, Entry<String, Integer> e) {
        String hostname = e.getKey();
        StringBuilder hostnameStr = new StringBuilder(hostname);
        if (procSeparator == null || procSeparator.isEmpty()) {
            // Add the host as many times as in value
            for (int i = 1; i < e.getValue(); i++) {
                hostnameStr.append(hostSeparator + hostname);
            }
        } else {
            hostnameStr.append(procSeparator + e.getValue());
        }
        return hostnameStr.toString();
    }

    /**
     * Generate the argument for number of MPI processes according to the MPI definition.
     * 
     * @param numWorkers Number of workers.
     * @param computingUnits Number of Computing Units per worker.
     * @return Argument to put in the number of MPI processes.
     */
    public String generateNumberOfProcesses(int numWorkers, int computingUnits) {
        if (scaleByCU) {
            return String.valueOf(numWorkers * computingUnits);
        } else if (ppn > 1) {
            return String.valueOf(numWorkers * ppn);
        } else {
            return String.valueOf(numWorkers);
        }
    }

}
