package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutor extends ExternalExecutor {

    private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
    private static final String COMMONS_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "commons" + File.separator
            + "lib";
    private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";

    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";


    public CExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe, TaskResultReader resultReader) {
        super(nw, pool, queue, writePipe, resultReader);
    }

    @Override
    public ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs) {
        ArrayList<String> lArgs = new ArrayList<>();

        logger.debug("getting the task execution command");
        logger.debug("mida interna de la memoria de la gpu: " + nt.getResourceDescription().getProcessors().get(0).getInternalMemory());
        if (nt.getResourceDescription().getProcessors().size() > 1)
            logger.debug(
                    "mida interna de la memoria de la gpu-2: " + nt.getResourceDescription().getProcessors().get(1).getInternalMemory());

        // NX_ARGS string built from the Resource Description
        StringBuilder reqs = new StringBuilder();
        int numCUs = nt.getResourceDescription().getTotalCPUComputingUnits();
        reqs.append("NX_ARGS='--smp-cpus=").append(numCUs);

        // Debug mode on
        if (workerDebug) {
            reqs.append(" --summary");
        }

        StringBuilder cuda_visible = new StringBuilder();
        StringBuilder opencl_visible = new StringBuilder();

        if (assignedGPUs.length > 0) {
            String quotes = "\"";
            cuda_visible.append("CUDA_VISIBLE_DEVICES=").append(quotes);
            opencl_visible.append("GPU_DEVICE_ORDINAL=").append(quotes);
            reqs.append(" --gpu-warmup=no");
            for (int i = 0; i < (assignedGPUs.length - 1); i++) {
                cuda_visible.append(assignedGPUs[i]).append(",");
                opencl_visible.append(assignedGPUs[i]).append(",");
            }
            cuda_visible.append(assignedGPUs[assignedGPUs.length - 1]).append(quotes).append(" ");
            opencl_visible.append(assignedGPUs[assignedGPUs.length - 1]).append(quotes).append(" ");

            for (int j = 0; j < nt.getResourceDescription().getProcessors().size(); j++) {
                Processor p = nt.getResourceDescription().getProcessors().get(j);
                // logger.debug("processor type is " + p.getType());
                // logger.debug("processor memory is " + p.getInternalMemory());
                // boolean test1 = (p.getType().equals("GPU"));
                // boolean test2 = (p.getInternalMemory() > 0.00001);
                // boolean test3 = (p.getType() == "GPU" && p.getInternalMemory() > 100);
                // boolean test4 = ((p.getType() == "GPU") && (p.getInternalMemory() > 100));
                // logger.debug("test1 is " + test1);
                // logger.debug("test2 is " + test2);
                // logger.debug("test3 is " + test3);
                // logger.debug("test4 is " + test4);
                if (p.getType().equals("GPU") && p.getInternalMemory() > 0.00001) {
                    float bytes = p.getInternalMemory() * 1048576; // MB to byte conversion
                    int b_int = Math.round(bytes);
                    reqs.append(" --gpu-max-memory=").append(b_int);
                }
            }

        } else {
            reqs.append(" --disable-cuda=yes");
            reqs.append(" --disable-opencl=yes");
        }

        reqs.append("' ");

        // Taskset string to bind the job
        StringBuilder taskset = new StringBuilder();
        taskset.append("taskset -c ");
        for (int i = 0; i < (numCUs - 1); i++) {
            taskset.append(assignedCoreUnits[i]).append(",");
        }

        taskset.append(assignedCoreUnits[numCUs - 1]).append(" ");

        lArgs.add(cuda_visible.toString() + opencl_visible.toString() + reqs.toString() + taskset.toString() + nw.getAppDir()
                + WORKER_C_RELATIVE_PATH);

        return lArgs;
    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        Map<String, String> env = new HashMap<>();
        String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }

        // Add C and commons libs
        ldLibraryPath = ldLibraryPath.concat(":" + nw.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath = ldLibraryPath.concat(":" + nw.getInstallDir() + COMMONS_LIB_RELATIVE_PATH);

        env.put("LD_LIBRARY_PATH", ldLibraryPath);
        return env;
    }

}
