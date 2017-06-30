package integratedtoolkit.types;

import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import java.util.HashMap;

public class ExtendedCloudMethodWorker extends CloudMethodWorker {

    private boolean terminated;

    public ExtendedCloudMethodWorker(String name, CloudProvider provider, CloudMethodResourceDescription description, COMPSsWorker worker, int limitOfTasks, HashMap<String, String> sharedDisks) {
        super(name, provider, description, worker, limitOfTasks, sharedDisks);
        terminated = false;
    }

    public void terminate() {
        this.terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

}
