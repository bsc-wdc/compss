package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class PythonExecutor extends ExternalExecutor {

	@Override
	public ArrayList<String> getLaunchCommand(NIOTask nt) {
		ArrayList<String> lArgs = new ArrayList<String>();
		String pycompssHome = nt.installDir + "/../../../Bindings/python";
		/*lArgs.add("/bin/bash");
		lArgs.add("-e");
		lArgs.add("-c");*/
		lArgs.add("python");
		lArgs.add("-u");
		lArgs.add(pycompssHome + "/pycompss/worker/worker.py");
		return lArgs;
	}

	@Override
	public Map<String, String> getEnvironment(NIOTask nt) {
		/*
		 * export PYCOMPSS_HOME=`dirname $0`/../../bindings/pythonport
		 * PYTHONPATH=$app_dir:$py_path:$PYCOMPSS_HOME
		 */
		Map<String, String> env = new HashMap<String, String>();
		String pycompssHome = nt.installDir + "/../../../Bindings/python";
		env.put("PYCOMPSS_HOME", pycompssHome);
		String pythonPath = System.getenv("PYTHONPATH");
		if (pythonPath == null) {
			pythonPath = nt.appDir + ":" + nt.classPath + ":" + pycompssHome;
		} else {
			pythonPath = pythonPath.concat(":" + nt.appDir + ":" + nt.classPath + ":" + pycompssHome);
		}
		env.put("PYTHONPATH", pythonPath);
		String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
		if (ldLibraryPath == null) {
			ldLibraryPath = nt.libPath;
		} else {
			ldLibraryPath = ldLibraryPath.concat(":" + nt.libPath);
		}
		env.put("LD_LIBRARY_PATH", ldLibraryPath);
               
		return env;
	}

}
