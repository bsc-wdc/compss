package integratedtoolkit.nio.master.handlers;

import integratedtoolkit.nio.master.NIOWorkerNode;
import integratedtoolkit.nio.master.WorkerStarter;


public class Ender extends Thread {
	
    private NIOWorkerNode node;
    private int pid;

    public Ender(NIOWorkerNode node, int pid) {
        this.node = node;
        this.pid = pid;
    }

    public void run() {
    	WorkerStarter.ender(node, pid);
    }
    
}