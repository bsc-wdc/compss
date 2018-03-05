package es.bsc.compss.nio.worker.util;

public class Ender extends Thread {

    private ExternalThreadPool etp;


    public Ender(ExternalThreadPool etp) {
        this.etp = etp;
    }

    @Override
    public void run() {
        ExternalThreadPool.ender(etp);
    }

}
