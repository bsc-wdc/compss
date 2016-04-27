package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutor extends ExternalExecutor {
	
	private static final String C_LIB_RELATIVE_PATH = File.separator + ".." + File.separator + "Bindings" 
														+ File.separator + "c" + File.separator + "lib";
	private static final String COMMONS_LIB_RELATIVE_PATH = File.separator + ".." + File.separator + "Bindings" 
			+ File.separator + "commons" + File.separator + "lib";
	

    public ArrayList<String> getLaunchCommand(NIOTask nt) {
        ArrayList<String> lArgs = new ArrayList<String>();
        lArgs.add(nt.appDir + "/worker/worker_c");
        lArgs.add(nt.appDir);
        lArgs.add(nt.classPath);
        return lArgs;
    }

    public Map<String, String> getEnvironment(NIOTask nt) {
        //export LD_LIBRARY_PATH=$scriptDir/../../bindings/c/lib:$scriptDir/../../bindings/bindings-common/lib:$LD_LIBRARY_PATH
    	
    	Map<String, String> env = new HashMap<String, String>();
        String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
        if (ldLibraryPath == null) {
            ldLibraryPath = nt.libPath;
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nt.libPath);
        }
        
        // Add C and commons libs
        ldLibraryPath.concat(":" + nt.installDir + C_LIB_RELATIVE_PATH);
        ldLibraryPath.concat(":" + nt.installDir + COMMONS_LIB_RELATIVE_PATH);
        
        env.put("LD_LIBRARY_PATH", ldLibraryPath);
        return env;
    }
}
