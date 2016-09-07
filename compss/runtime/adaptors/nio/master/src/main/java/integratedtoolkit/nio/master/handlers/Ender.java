package integratedtoolkit.nio.master.handlers;

import integratedtoolkit.nio.master.NIOWorkerNode;
import integratedtoolkit.nio.master.WorkerStarter;


public class Ender extends Thread {

    private WorkerStarter workerStarter;
    private NIOWorkerNode node;
    private int pid;


    public Ender(WorkerStarter workerStarter, NIOWorkerNode node, int pid) {
        this.workerStarter = workerStarter;
        this.node = node;
        this.pid = pid;
    }

    public void run() {
        workerStarter.ender(node, pid);
    }

}