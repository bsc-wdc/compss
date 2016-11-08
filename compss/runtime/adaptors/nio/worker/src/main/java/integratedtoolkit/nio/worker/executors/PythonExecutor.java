package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class PythonExecutor extends ExternalExecutor {

    public static final String PYCOMPSS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "python";

    private static final String WORKER_PYTHON_RELATIVE_PATH = PYCOMPSS_RELATIVE_PATH + File.separator + "pycompss" + File.separator
            + "worker" + File.separator + "worker.py";

    public static final boolean pythonPersistentWorker = false;

    private static final String EXTRAE_RELATIVE_PATH = File.separator + "Dependencies" + File.separator + "extrae";
    private static final String LIBEXEC_EXTRAE_RELATIVE_PATH = EXTRAE_RELATIVE_PATH + File.separator + "libexec";
    private static final String LIB_EXTRAE_RELATIVE_PATH = EXTRAE_RELATIVE_PATH + File.separator + "lib";


    public PythonExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe, TaskResultReader resultReader) {
        super(nw, pool, queue, writePipe, resultReader);
    }

    @Override
    public ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits, int[] assignedGPUs) {
        ArrayList<String> lArgs = new ArrayList<>();

        if (pythonPersistentWorker) {
            // The execution command in python is empty (the handler adds the pre-command and the application args)
        } else {
            lArgs.add("python");
            lArgs.add("-u");
            lArgs.add(nw.getInstallDir() + WORKER_PYTHON_RELATIVE_PATH);
        }
        
        //int numCUs = nt.getResourceDescription().getTotalCPUComputingUnits();
           
        // Taskset string to bind the job
		StringBuilder taskset = new StringBuilder();
		taskset.append("taskset -c ");
		taskset.append(assignedCoreUnits[0]);
		for (int i = 1; i < assignedCoreUnits.length; i++){
			taskset.append(",").append(assignedCoreUnits[i]);
		}		
		taskset.append(" ");

		//lArgs.add(taskset.toString());
        //lArgs.add(taskset.toString() + nw.getAppDir());
        //lArgs.add(nw.getAppDir());
        return lArgs;
    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        // PyCOMPSs HOME
        Map<String, String> env = new HashMap<>();
        String pycompssHome = nw.getInstallDir() + PYCOMPSS_RELATIVE_PATH;
        env.put("PYCOMPSS_HOME", pycompssHome);

        // PYTHONPATH
        String pythonPath = System.getenv("PYTHONPATH");
        if (pythonPath == null) {
            pythonPath = pycompssHome + ":" + nw.getPythonpath() + ":" + nw.getAppDir();
        } else {
            pythonPath = pycompssHome + ":" + nw.getPythonpath() + ":" + nw.getAppDir() + pythonPath;
        }

        // Add pyextrae to PYTHONPATH if tracing
        if (NIOTracer.isActivated()) {
            String libexec_extrae_path = nw.getInstallDir() + LIBEXEC_EXTRAE_RELATIVE_PATH;
            String lib_extrae_path = nw.getInstallDir() + LIB_EXTRAE_RELATIVE_PATH;
            pythonPath += ":" + libexec_extrae_path + ":" + lib_extrae_path;
        }

        env.put("PYTHONPATH", pythonPath);

        // LD_LIBRARY_PATH
        String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }
        env.put("LD_LIBRARY_PATH", ldLibraryPath);

        return env;
    }

}
