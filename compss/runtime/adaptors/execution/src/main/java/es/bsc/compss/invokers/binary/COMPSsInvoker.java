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
package es.bsc.compss.invokers.binary;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;


public class COMPSsInvoker extends Invoker {

    private static final String RELATIVE_PATH_XML_GENERATION =
        "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator + "xmls" + File.separator;
    private static final String GENERATE_PROJECT_SCRIPT = "generate_project.sh";
    private static final String GENERATE_RESOURCES_SCRIPT = "generate_resources.sh";

    private static final int NUM_BASE_COMPSS_ARGS = 5;

    private static final String ERROR_RUNCOMPSS = "ERROR: Invalid runcompss";
    private static final String ERROR_APP_NAME = "ERROR: Invalid appName";
    private static final String ERROR_TARGET_PARAM = "ERROR: COMPSs Execution doesn't support target parameters";

    private COMPSsDefinition compssDef;
    private BinaryRunner br;


    /**
     * COMPSs Invoker constructor.
     *
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the COMPSs invoker.
     */
    public COMPSsInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);
        // Get method definition properties
        try {
            this.compssDef = (COMPSsDefinition) this.invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }
        // Internal binary runner
        this.br = null;
    }

    private void checkArguments() throws JobExecutionException {
        String runcompss = this.compssDef.getRuncompss();
        if (runcompss == null || runcompss.isEmpty()) {
            throw new JobExecutionException(ERROR_RUNCOMPSS);
        }
        String appName = this.compssDef.getAppName();
        if (appName == null || appName.isEmpty()) {
            throw new JobExecutionException(ERROR_APP_NAME);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.compssDef.getAppName() + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                // Python interpreter for direct access on stream property calls
                String pythonInterpreter = null;
                LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
                if (lp instanceof PythonParams) {
                    PythonParams pp = (PythonParams) lp;
                    pythonInterpreter = pp.getPythonInterpreter();
                }
                this.br.closeStreams(this.invocation.getParams(), pythonInterpreter);
            }
        } catch (StreamCloseException se) {
            LOGGER.error("Exception closing binary streams", se);
            throw new JobExecutionException(se);
        }

        // Update binary results
        for (InvocationParam np : this.invocation.getResults()) {
            if (np.getType() == DataType.FILE_T) {
                serializeBinaryExitValue(np, retValue);
            } else {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
            }
        }
    }

    private String computeNodeDescription(String hostname, int cus) {
        StringBuilder nodeInfo = new StringBuilder();
        nodeInfo.append(hostname);
        nodeInfo.append(":").append(String.valueOf(cus));
        nodeInfo.append(":").append(this.context.getInstallDir());
        nodeInfo.append(":").append(this.sandBox.getFolder().getAbsolutePath());
        return nodeInfo.toString();
    }

    private Object runInvocation() throws InvokeExecutionException {
        System.out.println("");
        System.out.println("[COMPSs INVOKER] Begin COMPSs call to " + this.compssDef.getAppName());
        System.out.println("[COMPSs INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
        System.out.println("[COMPSs INVOKER] Worker in Master : " + this.compssDef.getWorkerInMaster());

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

        String masterName = this.context.getHostName();
        if (this.compssDef.getWorkerInMaster().equalsIgnoreCase("false")) {
            System.out.println("[COMPSs INVOKER] Worker in master is false: Removing master from slave nodes...");
            // User has selected a separated master, take resources from slaveNodes
            if (hostnames2cus.containsKey(masterName)) {
                int accumComputingUnits = hostnames2cus.remove(masterName);
                accumComputingUnits = accumComputingUnits - this.computingUnits;
                if (accumComputingUnits > 0) {
                    hostnames2cus.put(masterName, accumComputingUnits);
                }
            } else {
                System.err.println("[WARN] Cannot reserve master CUs because hostname does not appear in slavenodes");
            }

            // Check that we have not run out of resources
            if (hostnames2cus.isEmpty()) {
                throw new InvokeExecutionException("Error no remaining resources after reserving nested master");
            }
        } else {
            if (!hostnames2cus.containsKey(masterName)) {
                System.out.println("[COMPSs INVOKER] Adding master to slave nodes");
                hostnames2cus.put(masterName, this.computingUnits);
            }
        }

        StringBuilder masterInfoBuilder = new StringBuilder();
        StringBuilder workersInfoBuilder = new StringBuilder();
        for (Entry<String, Integer> entry : hostnames2cus.entrySet()) {
            String hostname = entry.getKey();
            Integer cus = entry.getValue();
            System.out.println("[COMPSs INVOKER] Slave hostname " + hostname + " with " + String.valueOf(cus) + " cus");

            String nodeInfo = computeNodeDescription(hostname, cus);
            if (hostname.compareTo(masterName) == 0 && !this.compssDef.getWorkerInMaster().equalsIgnoreCase("worker")) {
                System.out.println("[COMPSs INVOKER] Adding master node info");
                masterInfoBuilder.append(nodeInfo);
            } else {
                if (workersInfoBuilder.length() > 0) {
                    workersInfoBuilder.append(" ");
                }
                workersInfoBuilder.append(nodeInfo);
            }
        }
        String masterInfo = masterInfoBuilder.toString();
        String workersInfo = workersInfoBuilder.toString();

        // Generate XML files
        String uuid = UUID.randomUUID().toString();
        String projectXml = "project_" + uuid + ".xml";
        String resourcesXml = "resources_" + uuid + ".xml";

        // Generate project.xml
        System.out.println("[COMPSs INVOKER] Generate project.xml at: " + projectXml);
        String generateProjectScript =
            this.context.getInstallDir() + RELATIVE_PATH_XML_GENERATION + GENERATE_PROJECT_SCRIPT;
        String[] cmdProject = new String[] { generateProjectScript,
            projectXml,
            masterInfo,
            workersInfo };
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
        String generateResourcesScript =
            this.context.getInstallDir() + RELATIVE_PATH_XML_GENERATION + GENERATE_RESOURCES_SCRIPT;
        String[] cmdResources = new String[] { generateResourcesScript,
            resourcesXml,
            workersInfo };
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

        // Prepare and purge runcompss extra flags
        String envScriptFlag = "";
        if (this.context.getEnvironmentScript() != null && !this.context.getEnvironmentScript().isEmpty()) {
            envScriptFlag = "--env_script=" + this.context.getEnvironmentScript();
        }
        String nestedLogDir = this.sandBox.getFolder().getAbsolutePath() + File.separator + "nestedCOMPSsLog";

        JavaParams javaParams = (JavaParams) this.context.getLanguageParams(Lang.JAVA);
        String classpathFlag = "--classpath=" + javaParams.getClasspath();

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.getPythonInterpreter();
        }

        List<String> extraFlagsList = new ArrayList<>();
        String extraFlags = this.compssDef.getFlags();
        if (extraFlags != null && !extraFlags.isEmpty()) {
            String[] extraFlagsArray =
                BinaryRunner.buildAppParams(this.invocation.getParams(), extraFlags, pythonInterpreter);
            // Covert STR to partial list and add only the purged values to the final argument list
            List<String> partialExtraFlagsList = Arrays.asList(extraFlagsArray);
            for (String flag : partialExtraFlagsList) {
                if (flag.startsWith("--base_log_dir=")) {
                    // Capturing base log dir and creating UUID random entry
                    // This avoids collisions when running multiple nested
                    System.out.println("[COMPSs INVOKER] Capturing flag " + flag);
                    System.out
                        .println("[COMPSs INVOKER] Generating random UUID as specific_log_dir inside base_log_dir");
                    int beginPos = "--base_log_dir=".length();
                    nestedLogDir = flag.substring(beginPos) + File.separator + UUID.randomUUID();
                } else {
                    if (flag.startsWith("--specific_log_dir=")) {
                        // Purged flag
                        System.out.println("[COMPSs INVOKER] Ommitting flag " + flag + " on nested runcompss command");
                    } else {
                        if (flag.startsWith("--classpath=")) {
                            // Append user classpath to worker classpath
                            int beginPos = "--classpath=".length();
                            classpathFlag = classpathFlag + ":" + flag.substring(beginPos);
                        } else {
                            // Add regular flag to final list
                            extraFlagsList.add(flag);
                        }
                    }
                }
            }
        }

        // Create a directory to store the nested COMPSs logs
        System.out.println("[COMPSs INVOKER] Creating nested log dir in: " + nestedLogDir);
        if (!new File(nestedLogDir).mkdir()) {
            throw new InvokeExecutionException("Error creating COMPSs nested log directory " + nestedLogDir);
        }

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        String[] appParams = new String[0];
        int appParamsSize = binaryParams.size();
        if (this.compssDef.hasParamsString()) {
            appParams = BinaryRunner.buildAppParams(this.invocation.getParams(), this.compssDef.getAppParams(),
                pythonInterpreter);
            appParamsSize = appParams.length;
        }

        // Prepare command (the +1 is for the appName)
        //
        String[] cmd = new String[NUM_BASE_COMPSS_ARGS + extraFlagsList.size() + 1 + appParamsSize];
        cmd[0] = this.compssDef.getRuncompss();
        cmd[1] = "--project=" + projectXml;
        cmd[2] = "--resources=" + resourcesXml;

        String nestedLogDirFlag = "--specific_log_dir=" + nestedLogDir;
        cmd[3] = nestedLogDirFlag;
        cmd[4] = classpathFlag;
        int i = NUM_BASE_COMPSS_ARGS;
        for (String flag : extraFlagsList) {
            cmd[i++] = flag;
        }
        cmd[i++] = genAppName(pythonInterpreter);
        if (this.compssDef.hasParamsString()) {
            for (String appParam : appParams) {
                cmd[i++] = appParam;
            }
        } else {
            for (String binParam : binaryParams) {
                cmd[i++] = binParam;
            }
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
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.compssDef.isFailByEV());
    }

    private String genAppName(String pythonInterpreter) throws InvokeExecutionException {
        String[] appNameArray =
            BinaryRunner.buildAppParams(this.invocation.getParams(), this.compssDef.getAppName(), pythonInterpreter);
        return appNameArray[0];
    }

    private int xmlGenerationScript(String[] cmd) throws IOException, InterruptedException {
        // Prepare command execution
        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.directory(this.sandBox.getFolder());

        // Setup process environment -- Tracing entries
        Tracer.prepareEnvironment(builder.environment(), false);

        // Launch command
        Process process = builder.start();

        // Disable inputs to process
        process.getOutputStream().close();

        // Wait and return exit value
        return process.waitFor();
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling COMPSs process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
