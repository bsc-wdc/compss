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
package es.bsc.compss.invokers.binary;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.BinaryRunner.StreamSTD;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;


public class COMPSsInvoker extends Invoker {

    private static final String RELATIVE_PATH_XML_GENERATION = "Runtime" + File.separator + "scripts" + File.separator
            + "system" + File.separator + "xmls" + File.separator;
    private static final String GENERATE_PROJECT_SCRIPT = "generate_project.sh";
    private static final String GENERATE_RESOURCES_SCRIPT = "generate_resources.sh";

    private static final int NUM_BASE_COMPSS_ARGS = 5;

    private static final String ERROR_RUNCOMPSS = "ERROR: Invalid runcompss";
    private static final String ERROR_APP_NAME = "ERROR: Invalid appName";
    private static final String ERROR_TARGET_PARAM = "ERROR: COMPSs Execution doesn't support target parameters";

    private final String runcompss;
    private final String extraFlags;
    private final String appName;
    private final String workerInMaster;


    public COMPSsInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        // Get method definition properties
        COMPSsImplementation compssImpl = null;
        try {
            compssImpl = (COMPSsImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(
                    ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // COMPSs specific flags
        this.runcompss = compssImpl.getRuncompss();
        this.extraFlags = compssImpl.getFlags();
        this.appName = compssImpl.getAppName();
        this.workerInMaster = compssImpl.getWorkerInMaster();
    }

    private void checkArguments() throws JobExecutionException {
        if (this.runcompss == null || this.runcompss.isEmpty()) {
            throw new JobExecutionException(ERROR_RUNCOMPSS);
        }
        if (this.appName == null || this.appName.isEmpty()) {
            throw new JobExecutionException(ERROR_APP_NAME);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.appName + " in " + this.context.getHostName());
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Check results
        for (InvocationParam np : this.invocation.getResults()) {
            if (np.getType() == DataType.FILE_T) {
                serializeBinaryExitValue(np, retValue);
            } else {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
            }
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        System.out.println("");
        System.out.println("[COMPSs INVOKER] Begin COMPSs call to " + appName);
        System.out.println("[COMPSs INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to:
        // export OMP_NUM_THREADS=1
        // runcompss
        // --project=tmp_proj.xml
        // --resources=tmp_res.xml
        // --specific_log_dir=nestedLogDir
        // [-extra_flags]
        // appName appArgs

        // Retrieve workers information
        HashMap<String, Integer> hostnames2cus = new HashMap<>();
        for (String hostname : this.invocation.getSlaveNodesNames()) {
            if (hostnames2cus.containsKey(hostname)) {
                int accumComputingUnits = hostnames2cus.get(hostname) + this.computingUnits;
                hostnames2cus.put(hostname, accumComputingUnits);
            } else {
                hostnames2cus.put(hostname, this.computingUnits);
            }
        }
        if (!Boolean.parseBoolean(this.workerInMaster)) {
            // User has selected a separated master, take resources from slaveNodes
            String hostname = this.context.getHostName();
            if (hostnames2cus.containsKey(hostname)) {
                int accumComputingUnits = hostnames2cus.remove(hostname);
                accumComputingUnits = accumComputingUnits - this.computingUnits;
                if (accumComputingUnits > 0) {
                    hostnames2cus.put(hostname, accumComputingUnits);
                }
            } else {
                System.err.println("[WARN] Cannot reserve master CUs because hostname does not appear in slavenodes");
            }

            // Check that we have not run out of resources
            if (hostnames2cus.isEmpty()) {
                throw new InvokeExecutionException("Error no remaining resources after reserving nested master");
            }
        }

        StringBuilder workersInfoBuilder = new StringBuilder();
        for (Entry<String, Integer> entry : hostnames2cus.entrySet()) {
            String hostname = entry.getKey();
            Integer cus = entry.getValue();
            System.out.println("[COMPSs INVOKER] Slave hostname " + hostname + " with " + String.valueOf(cus) + " cus");

            workersInfoBuilder.append(hostname);
            workersInfoBuilder.append(":").append(String.valueOf(cus));
            workersInfoBuilder.append(":").append(this.context.getInstallDir());
            workersInfoBuilder.append(":").append(this.taskSandboxWorkingDir.getAbsolutePath());
            workersInfoBuilder.append(" ");
        }
        String workersInfo = workersInfoBuilder.toString();

        // Generate XML files
        String uuid = UUID.randomUUID().toString();
        String projectXml = "project_" + uuid + ".xml";
        String resourcesXml = "resources_" + uuid + ".xml";

        // Generate project.xml
        System.out.println("[COMPSs INVOKER] Generate project.xml at: " + projectXml);
        String generateProjectScript = this.context.getInstallDir() + RELATIVE_PATH_XML_GENERATION
                + GENERATE_PROJECT_SCRIPT;
        String[] cmdProject = new String[] { generateProjectScript, projectXml, workersInfo };
        try {
            int ev = xmlGenerationScript(cmdProject);
            if (ev != 0) {
                throw new InvokeExecutionException("Error generating project.xml file (ev = " + ev + ")");
            }
        } catch (IOException ioe) {
            throw new InvokeExecutionException("Error generating project.xml file", ioe);
        } catch (InterruptedException ie) {
            throw new InvokeExecutionException("Error generating project.xml file", ie);
        }

        // Generate resources.xml
        System.out.println("[COMPSs INVOKER] Generate resources.xml at: " + resourcesXml);
        String generateResourcesScript = this.context.getInstallDir() + RELATIVE_PATH_XML_GENERATION
                + GENERATE_RESOURCES_SCRIPT;
        String[] cmdResources = new String[] { generateResourcesScript, resourcesXml, workersInfo };
        try {
            int ev = xmlGenerationScript(cmdResources);
            if (ev != 0) {
                throw new InvokeExecutionException("Error generating resources.xml file (ev = " + ev + ")");
            }
        } catch (IOException ioe) {
            throw new InvokeExecutionException("Error generating resources.xml file", ioe);
        } catch (InterruptedException ie) {
            throw new InvokeExecutionException("Error generating resources.xml file", ie);
        }

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(invocation.getParams(),
                invocation.getTarget(), streamValues);

        // Prepare and purge runcompss extra flags
        String nestedLogDir = this.taskSandboxWorkingDir.getAbsolutePath() + File.separator + "nestedCOMPSsLog";
        JavaParams javaParams = (JavaParams) this.context.getLanguageParams(Lang.JAVA);
        String classpathFlag = "--classpath=" + javaParams.getClasspath();
        List<String> extraFlagsList = new ArrayList<>();
        if (this.extraFlags != null && !this.extraFlags.isEmpty()) {
            // Covert STR to partial list and add only the purged values to the final argument list
            List<String> partialExtraFlagsList = Arrays.asList(this.extraFlags.split(" "));
            for (String flag : partialExtraFlagsList) {
                if (flag.startsWith("--base_log_dir=")) {
                    // Capturing base log dir and creating UUID random entry
                    // This avoids collisions when running multiple nested
                    System.out.println("[COMPSs INVOKER] Capturing flag " + flag);
                    System.out
                            .println("[COMPSs INVOKER] Generating random UUID as specific_log_dir inside base_log_dir");
                    int beginPos = "--base_log_dir=".length();
                    nestedLogDir = flag.substring(beginPos) + File.separator + UUID.randomUUID();
                } else if (flag.startsWith("--specific_log_dir=")) {
                    // Purged flag
                    System.out.println("[COMPSs INVOKER] Ommitting flag " + flag + " on nested runcompss command");
                } else if (flag.startsWith("--classpath=")) {
                    // Append user classpath to worker classpath
                    int beginPos = "--classpath=".length();
                    classpathFlag = classpathFlag + ":" + flag.substring(beginPos);
                } else {
                    // Add regular flag to final list
                    extraFlagsList.add(flag);
                }
            }
        }

        // Create a directory to store the nested COMPSs logs
        System.out.println("[COMPSs INVOKER] Creating nested log dir in: " + nestedLogDir);
        if (!new File(nestedLogDir).mkdir()) {
            throw new InvokeExecutionException("Error creating COMPSs nested log directory " + nestedLogDir);
        }
        String nestedLogDirFlag = "--specific_log_dir=" + nestedLogDir;

        // Prepare command (the +1 is for the appName)
        String[] cmd = new String[NUM_BASE_COMPSS_ARGS + extraFlagsList.size() + 1 + binaryParams.size()];
        cmd[0] = runcompss;
        cmd[1] = "--project=" + projectXml;
        cmd[2] = "--resources=" + resourcesXml;
        cmd[3] = nestedLogDirFlag;
        cmd[4] = classpathFlag;
        int i = NUM_BASE_COMPSS_ARGS;
        for (String flag : extraFlagsList) {
            cmd[i++] = flag;
        }
        cmd[i++] = appName;
        for (String param : binaryParams) {
            cmd[i++] = param;
        }

        // Debug command
        System.out.print("[COMPSs INVOKER] COMPSs CMD: ");
        for (int index = 0; index < cmd.length; ++index) {
            System.out.print(cmd[index] + " ");
        }
        System.out.println("");
        System.out.println("[COMPSs INVOKER] COMPSs STDIN: " + streamValues.getStdIn());
        System.out.println("[COMPSs INVOKER] COMPSs STDOUT: " + streamValues.getStdOut());
        System.out.println("[COMPSs INVOKER] COMPSs STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(),
                context.getThreadErrStream());
    }

    private int xmlGenerationScript(String[] cmd) throws IOException, InterruptedException {
        // Prepare command execution
        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.directory(this.taskSandboxWorkingDir);
        builder.environment().remove(Tracer.LD_PRELOAD);

        // Launch command
        Process process = builder.start();

        // Disable inputs to process
        process.getOutputStream().close();

        // Wait and return exit value
        return process.waitFor();

    }
}
