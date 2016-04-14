package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutor extends ExternalExecutor {

    public ArrayList<String> getLaunchCommand(NIOTask nt) {
        ArrayList<String> lArgs = new ArrayList<String>();
        lArgs.add(nt.appDir + "/worker/worker_c");
        lArgs.add(nt.appDir);
        lArgs.add(nt.classPath);
        return lArgs;
    }

    public Map<String, String> getEnvironment(NIOTask nt) {
        //export LD_LIBRARY_PATH=$scriptDir/../../bindings/c/lib:$scriptDir/../../bindings/bindings-common/lib:$LD_LIBRARY_PATH// TODO Auto-generated method stub
        Map<String, String> env = new HashMap<String, String>();
        String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
        if (ldLibraryPath == null) {
            ldLibraryPath = nt.libPath;
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nt.libPath);
        }
        env.put("LD_LIBRARY_PATH", nt.installDir + "/../../bindings/c/lib:" + nt.installDir + "/../../bindings/bindings-common/lib:" + System.getenv("LD_LIBRARY_PATH") + ":" + nt.libPath);
        return env;
    }
}
