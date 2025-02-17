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

import static es.bsc.compss.types.implementations.definition.ContainerDescription.ContainerEngine.SINGULARITY;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.types.MPIProgram;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


public class MpmdMPIDefinition extends CommonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 5;
    public static final String SIGNATURE = "mpmdmpi.MPMDMPI";

    private static final String ERROR_MPI_BINARY = "ERROR: Empty binary annotation for MPMDMPI method";
    private MPIProgram[] programs;

    private static final String DUMMY_SEPARATOR = "_<<>>_";
    private boolean totalNopInCMD = false;
    private boolean binaryInCmd = false;
    private boolean hostStringInCmd = false;
    private boolean nopInCmd = false;
    private boolean configFile = false;
    private boolean hostfile = false;
    private boolean assignPPN = false;

    private String totalNopFlag = "-n";
    private String programsSeparator = ":";
    private String hostFileFlag = "--hostfile";
    private String hostFlag = "--host";
    private String hostSeparator = ",";
    private String ppnSeparator = " ";
    private String nopFlag = "-n";
    private boolean isNopByRank = false;
    private String configFlag = "--config-file";

    private ContainerDescription container;


    /**
     * Creates a new MPIImplementation for serialization.
     */
    public MpmdMPIDefinition() {
        // For externalizable
    }

    /**
     * Creates a new MPIImplementation instance from the given parameters.
     *
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param ppn Process per node.
     * @param failByEV Flag to enable failure with EV.
     * @param programs program definitions.
     */
    public MpmdMPIDefinition(String workingDir, String mpiRunner, int ppn, boolean failByEV, MPIProgram[] programs) {
        super(workingDir, mpiRunner, ppn, "", false, failByEV);
        this.programs = programs;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     * @param container Container description.
     */
    public MpmdMPIDefinition(String[] implTypeArgs, int offset, ContainerDescription container) {
        this.mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.ppn = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]));
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 3]);

        // Multi Program
        int numOfProgs = Integer.parseInt(implTypeArgs[offset + 4]);
        this.programs = new MPIProgram[numOfProgs];

        for (int i = 0; i < numOfProgs; i++) {
            int index = offset + NUM_PARAMS + (i * MPIProgram.NUM_OF_PARAMS);
            String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[index]);
            String params = EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 1]);
            int procs = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 2]));
            this.programs[i] = new MPIProgram(binary, params, procs);
        }

        this.container = container;

        checkArguments();
    }

    public MPIProgram[] getPrograms() {
        return this.programs;
    }

    @Override
    public void checkArguments() {
        super.checkArguments();
        for (MPIProgram mpiProgram : this.programs) {
            if (mpiProgram.isEmpty()) {
                throw new IllegalArgumentException(ERROR_MPI_BINARY);
            }
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.mpiRunner);
        lArgs.add(this.workingDir);
        lArgs.add(Integer.toString(this.ppn));
        lArgs.add(Boolean.toString(failByEV));
        lArgs.add(Integer.toString(this.programs.length));
        for (MPIProgram program : this.programs) {
            lArgs.add(program.getBinary());
            // if each program should run on a separate container
            if (this.container != null) {
                // <container_engine> <container_exec_command> <container_options>[] <container_image> binary args
                lArgs.add(this.container.getEngine().name().toLowerCase());
                lArgs.add(this.container.getEngine().equals(SINGULARITY) ? "exec" : "run");
                for (String tmp : this.container.getOptions().split(" ")) {
                    lArgs.add(tmp);
                }
                lArgs.add(this.container.getImage());
            }
            lArgs.add(program.getParams());
            lArgs.add(Integer.toString(program.getProcesses()));
        }
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPMDMPI;
    }

    /**
     * Returns the container.
     *
     * @return The container implementation.
     */
    public ContainerDescription getContainer() {
        return this.container;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"MPMD_MPI\",");
        sb.append("\"mpi_runner\":\"").append(this.mpiRunner).append("\",");
        sb.append("\"working_dir\":\"").append(this.workingDir).append("\",");
        sb.append("\"mpi_ppn\":").append(this.ppn).append(",");
        sb.append("\"fail_by_ev\":").append(this.failByEV).append(",");
        sb.append("\"container\":").append(this.container == null ? null : this.container.toJSON()).append(",");
        sb.append("\"programs\":[");
        for (MPIProgram program : this.getPrograms()) {
            sb.append("\"").append(program.toString()).append("\",");
        }
        sb.append("]}");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "MPMDMPI Method with MPIRunner " + this.mpiRunner + ", and " + this.programs.length + " programs.";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.mpiRunner = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.ppn = in.readInt();
        this.failByEV = in.readBoolean();
        this.programs = (MPIProgram[]) in.readObject();
        this.container = (ContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.mpiRunner);
        out.writeObject(this.workingDir);
        out.writeInt(this.ppn);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.programs);
        out.writeObject(this.container);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MPMDMPI Implementation \n");
        sb.append("\t MPI runner: ").append(this.mpiRunner).append("\n");
        sb.append("\t Working directory: ").append(this.workingDir).append("\n");
        sb.append("\t MPI PPN: ").append(this.ppn).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Programs: ").append("\n");
        sb.append("\t Container: ").append(this.container).append("\n");

        for (MPIProgram prog : this.programs) {
            sb.append("\t\t").append(prog).append("\n");
        }
        return sb.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public String generateNumberOfProcesses(int numWorkers, int computingUnits) {
        // not required for MPMD MPI tasks.
        return null;
    }

    /**
     * Generates CMD to execute this MPMD MPI, including host, config, nop etc.
     *
     * @param taskSandboxWorkingDir sandbox dir.
     * @param hostnames names of the hosts this MPMD MPI command can use.
     * @return full CMD as string[].
     */
    public String[] generateCMD(File taskSandboxWorkingDir, List<String> hostnames, int computingUnits,
        ContainerDescription container) throws IOException {
        // Remove infiniband suffix
        for (int i = 0; i < hostnames.size(); i++) {
            String tmp = hostnames.get(i);
            if (tmp.endsWith(IB_SUFFIX)) {
                hostnames.set(i, tmp.substring(0, tmp.lastIndexOf(IB_SUFFIX)));
            }
        }

        StringBuilder cmd = new StringBuilder();

        String masterContImage = System.getenv(COMPSsConstants.MASTER_CONTAINER_IMAGE);
        if (masterContImage != null && !masterContImage.isEmpty()) {
            String script = System.getenv(COMPSsConstants.MPI_RUNNER_SCRIPT);
            cmd.append(script).append(DUMMY_SEPARATOR);
        }

        cmd.append(this.mpiRunner).append(DUMMY_SEPARATOR);

        // total # of processes for this MPMD
        if (this.totalNopInCMD) {
            cmd.append(this.totalNopFlag).append(DUMMY_SEPARATOR);
            cmd.append(getTotalNumOfProcesses()).append(DUMMY_SEPARATOR);
        }

        // hostfile
        if (this.hostfile) {
            String content = buildHostFileString(hostnames, computingUnits);
            String fileName = writeToFile(taskSandboxWorkingDir, content, ".hostfile");
            cmd.append(this.hostFileFlag).append(DUMMY_SEPARATOR).append(fileName).append(DUMMY_SEPARATOR);
        }

        // binary in CMD, generate with a loop
        if (this.binaryInCmd) {
            List<String> fullCmd = new ArrayList<>(Arrays.asList(cmd.toString().split(DUMMY_SEPARATOR)));
            for (MPIProgram program : this.programs) {
                // build single program part without args
                String tmp = buildSPString(program, hostnames);
                fullCmd.addAll(Arrays.asList(tmp.split(DUMMY_SEPARATOR)));
                if (program.hasParamsString()) {
                    fullCmd.addAll(Arrays.asList(program.getParamsArray()));
                }
                fullCmd.add(this.programsSeparator);
            }
            String[] ret = new String[fullCmd.size()];
            return fullCmd.toArray(ret);
        }

        if (this.hostStringInCmd) {
            cmd.append(buildHostsString(hostnames)).append(DUMMY_SEPARATOR);
        }

        if (this.configFile) {
            String content = buildConfigFileString(hostnames);
            String fileName = writeToFile(taskSandboxWorkingDir, content, ".config");
            cmd.append(this.configFlag).append(DUMMY_SEPARATOR).append(fileName);
        }

        return cmd.toString().split(DUMMY_SEPARATOR);
    }

    private String buildHostFileString(List<String> hostnames, int computingUnits) {
        return buildHostsString(hostnames, computingUnits, this.ppn, this.ppnSeparator, this.hostSeparator, false);
    }

    private String buildConfigFileString(List<String> hostnames) {
        StringBuilder content = new StringBuilder();
        int offset = 0;
        for (MPIProgram program : this.programs) {

            // host list if necessary
            if (!this.hostStringInCmd) {
                content.append(this.hostFlag).append(" ");
                for (int i = 0; i < hostnames.size(); i++) {
                    content.append(hostnames.get(i));
                    if (i < hostnames.size() - 1) {
                        content.append(this.hostSeparator);
                    }
                }
                content.append(" ");
            }

            // nop
            if (!this.nopInCmd) {
                content.append(buildNopString(program, offset)).append(" ");
                if (this.isNopByRank) {
                    offset += program.getProcesses();
                }
            }

            // binary
            if (!this.binaryInCmd) {
                // container args come right before the binary

                // if each program should run on a separate container
                if (this.container != null) {
                    // <container_engine> <container_exec_command> <container_options>[] <container_image> binary args
                    content.append(this.container.getEngine().name().toLowerCase()).append(" ");
                    content.append(this.container.getEngine().equals(SINGULARITY) ? "exec" : "run").append(" ");
                    if (!container.getOptions().isEmpty() && !container.getOptions().equals(Constants.UNASSIGNED)) {
                        for (String tmp : this.container.getOptions().split(" ")) {
                            content.append(tmp).append(" ");
                        }
                    }
                    content.append(this.container.getImage()).append(" ");
                }

                content.append(program.getBinary());
            }

            // args
            if (program.hasParamsString()) {
                content.append(" ").append(program.getParams());
            }

            // next program will start with a new line
            content.append("\n");
        }

        return content.toString();
    }

    /**
     * Given the file content and extension, creates a new unique file and writes the content inside.
     *
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param content content to be written in the file.
     * @param extension file extension.
     * @return location of the generated file.
     * @throws IOException Exception writing hostfile.
     */
    private static String writeToFile(File taskSandboxWorkingDir, String content, String extension) throws IOException {
        String uuid = UUID.randomUUID().toString();
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + uuid + extension;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(content);
        } catch (IOException ioe) {
            throw ioe;
        }
        return filename;
    }

    /**
     * Get total number of processes by summing up NOP from each program of this MPMD.
     *
     * @return total NOP.
     */
    private int getTotalNumOfProcesses() {
        int ret = 0;
        for (MPIProgram program : this.programs) {
            ret += program.getProcesses();
        }
        return ret;
    }

    /**
     * generates execution cmd for a Single Program to be executed from CMD line within MPMD MPI.
     *
     * @param program MPI Program to create the execution command for.
     * @param hostnames names of the hosts this MPMD MPI command can use.
     * @return formatted string including all arguments for the given program.
     */
    private String buildSPString(MPIProgram program, List<String> hostnames) {

        StringBuilder ret = new StringBuilder();

        // hosts
        if (this.hostStringInCmd) {
            ret.append(buildHostsString(hostnames)).append(DUMMY_SEPARATOR);
        }

        // nop
        if (this.nopInCmd) {
            ret.append(this.nopFlag).append(DUMMY_SEPARATOR);
            ret.append(program.getProcesses()).append(DUMMY_SEPARATOR);
        }

        // if each program should run on a separate container
        if (this.container != null) {
            // <container_engine> <container_exec_command> <container_options>[] <container_image> binary args
            ret.append(this.container.getEngine().name().toLowerCase());
            ret.append(this.container.getEngine().equals(SINGULARITY) ? "exec" : "run");
            for (String tmp : this.container.getOptions().split(" ")) {
                ret.append(tmp);
            }
            ret.append(this.container.getImage());
        }

        // binary
        ret.append(program.getBinary());

        // args should be added as an array, not by space, so do not add here!

        return ret.toString();
    }

    /**
     * build 'hosts' string for this MPMD MPI.
     *
     * @param hostnames names of the hosts this MPMD MPI command can use.
     * @return formatted string including all arguments for the given program.
     */
    private String buildHostsString(List<String> hostnames) {
        StringBuilder ret = new StringBuilder();
        ret.append(this.hostFlag);
        for (int i = 0; i < hostnames.size(); i++) {
            ret.append(hostnames.get(i));
            if (i < hostnames.size() - 1) {
                ret.append(this.hostSeparator);
            }
        }
        return ret.toString();
    }

    /**
     * build Number Of Processes string for a given MPIProgram of this MPMD MPI.
     *
     * @param program names of the hosts this MPMD MPI command can use.
     * @param offSet offset in case NOP string is in ranges.
     * @return formatted string including all arguments for the given program.
     */
    private String buildNopString(MPIProgram program, int offSet) {
        StringBuilder ret = new StringBuilder();

        if (this.nopFlag != null && !this.nopFlag.isEmpty()) {
            ret.append(this.nopFlag).append(" ");
        }

        if (this.isNopByRank) {
            for (int i = 0; i < program.getProcesses(); i++) {
                ret.append(offSet + i);
                if (i < program.getProcesses() - 1) {
                    ret.append(",");
                }
            }
        } else {
            ret.append(program.getProcesses());
        }
        return ret.toString();
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

            // todo: tidy up these strings
            this.totalNopInCMD = Boolean.parseBoolean(loadProperty(props, "mpmd.total.nop.in.cmd", "false"));
            this.binaryInCmd = Boolean.parseBoolean(loadProperty(props, "mpmd.binary.in.cmd", "false"));
            this.hostStringInCmd = Boolean.parseBoolean(loadProperty(props, "mpmd.hosts.string.in.cmd", "false"));
            this.hostfile = Boolean.parseBoolean(loadProperty(props, "mpmd.hostfile", "false"));
            this.nopInCmd = Boolean.parseBoolean(loadProperty(props, "mpmd.nop.string.in.cmd", "false"));
            this.configFile = Boolean.parseBoolean(loadProperty(props, "mpmd.config.file", "false"));
            this.assignPPN = Boolean.parseBoolean(loadProperty(props, "mpmd.assign.ppn", "false"));

            this.programsSeparator = loadProperty(props, "mpmd.programs.separator", ":");
            this.hostFlag = loadProperty(props, "mpmd.hosts.flag", "--host");
            this.hostFileFlag = loadProperty(props, "mpmd.hostfile.flag", "--hostfile");
            this.hostSeparator = loadProperty(props, "mpmd.hosts.separator", ",");
            this.ppnSeparator = loadProperty(props, "mpmd.ppn.separator", " ");
            this.totalNopFlag = loadProperty(props, "mpmd.total.nop.flag", "-n");
            this.nopFlag = loadProperty(props, "mpmd.nop.flag", "-n");
            this.isNopByRank = Boolean.parseBoolean(loadProperty(props, "mpmd.nop.is.rank", "false"));
            this.configFlag = loadProperty(props, "mpmd.config.flag", "-n");

        } catch (Exception e) {
            LOGGER.warn("Can't load MPIRUN type in " + file + ".\nReason: " + e.getMessage());
        }
    }

}
