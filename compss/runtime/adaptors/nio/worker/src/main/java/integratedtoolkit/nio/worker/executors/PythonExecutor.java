package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class PythonExecutor extends ExternalExecutor {
	
	private static final String PYCOMPSS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "python";
	private static final String WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker" + File.separator + "worker.py";

	@Override
	public ArrayList<String> getLaunchCommand(NIOTask nt) {
		ArrayList<String> lArgs = new ArrayList<String>();
		String pycompssHome = nt.getInstallDir() + PYCOMPSS_RELATIVE_PATH;
		/*lArgs.add("/bin/bash");
		lArgs.add("-e");
		lArgs.add("-c");*/
		lArgs.add("export EXTRAE_CONFIG_FILE=INSERT_PATH_HERE/extrae_task.xml;");
		lArgs.add("python");
		lArgs.add("-u");
		lArgs.add(pycompssHome + WORKER_PY_RELATIVE_PATH);
		return lArgs;
	}

	@Override
	public Map<String, String> getEnvironment(NIOTask nt) {
		// PyCOMPSs HOME
		Map<String, String> env = new HashMap<String, String>();
		String pycompssHome = nt.getInstallDir() + PYCOMPSS_RELATIVE_PATH;
		env.put("PYCOMPSS_HOME", pycompssHome);
		
		// PYTHONPATH
		String pythonPath = System.getenv("PYTHONPATH");
		if (pythonPath == null) {
			pythonPath = pycompssHome + ":" + nt.getPythonPath() + ":" + nt.getAppDir();
		} else {
			pythonPath = pycompssHome + ":" + nt.getPythonPath() + ":" + nt.getAppDir() + pythonPath;
		}
		env.put("PYTHONPATH", pythonPath);
		
		// LD_LIBRARY_PATH
		String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
		if (ldLibraryPath == null) {
			ldLibraryPath = nt.getLibPath();
		} else {
			ldLibraryPath = ldLibraryPath.concat(":" + nt.getLibPath());
		}
		env.put("LD_LIBRARY_PATH", ldLibraryPath);
               
		return env;
	}

}
