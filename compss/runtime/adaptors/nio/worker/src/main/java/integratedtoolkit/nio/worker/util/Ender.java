package integratedtoolkit.nio.worker.util;


public class Ender extends Thread {
	
    private ExternalThreadPool etp;

    public Ender(ExternalThreadPool etp) {
        this.etp = etp;
    }

    public void run() {
    	ExternalThreadPool.ender(etp);
    }

    
}