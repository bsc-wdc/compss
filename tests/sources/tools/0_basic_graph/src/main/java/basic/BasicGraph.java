package basic;

import es.bsc.compss.api.COMPSs;


public class BasicGraph {

    public static void main(String[] args) {

        Integer inToken = 10;
        Integer inoutToken = 20;
        Integer[] outTokens = new Integer[2];

        // Launch tasks
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inTask();
            BasicGraphImpl.inTask(true);
            BasicGraphImpl.inTask(inToken);
        }
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inoutTask(inoutToken);
        }
        for (int i = 0; i < 2; ++i) {
            outTokens[i] = BasicGraphImpl.outTask();
        }

        // Sync
        System.out.println(inoutToken);
        System.out.println(outTokens[0]);

        // Launch tasks
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inTask();
            BasicGraphImpl.inTask(true);
            BasicGraphImpl.inTask(inToken);
        }
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inoutTask(inoutToken);
        }
        for (int i = 0; i < 2; ++i) {
            outTokens[i] = BasicGraphImpl.outTask();
        }

        // Barrier
        COMPSs.barrier();

        // Launch tasks
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inTask();
            BasicGraphImpl.inTask(true);
            BasicGraphImpl.inTask(inToken);
        }
        for (int i = 0; i < 2; ++i) {
            BasicGraphImpl.inoutTask(inoutToken);
        }
        for (int i = 0; i < 2; ++i) {
            outTokens[i] = BasicGraphImpl.outTask();
        }
    }

}
