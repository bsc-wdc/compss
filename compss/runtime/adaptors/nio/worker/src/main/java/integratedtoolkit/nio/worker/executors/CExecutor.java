package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutor extends ExternalExecutor {
	
	private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
	private static final String COMMONS_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "commons" + File.separator + "lib";
	private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";
	
	
	public CExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue,
			String writePipe, TaskResultReader resultReader) {
		
		super(nw, pool, queue, writePipe, resultReader);
	}

	@Override
    public ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox) {
        ArrayList<String> lArgs = new ArrayList<String>();
        lArgs.add(nw.getAppDir() + WORKER_C_RELATIVE_PATH);
        return lArgs;
    }
    
    public static Map<String, String> getEnvironment(NIOWorker nw) {
        //export LD_LIBRARY_PATH=$scriptDir/../../bindings/c/lib:$scriptDir/../../bindings/bindings-common/lib:$LD_LIBRARY_PATH
    	
    	Map<String, String> env = new HashMap<String, String>();
        String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }
        
        // Add C and commons libs
        ldLibraryPath.concat(":" + nw.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath.concat(":" + nw.getInstallDir() + COMMONS_LIB_RELATIVE_PATH);
        
        env.put("LD_LIBRARY_PATH", ldLibraryPath);
        return env;
    }
    
}
