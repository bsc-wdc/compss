package es.bsc.compss.executor.types;

public class InvocationResources {

    private final int[] cpus;
    private final int[] gpus;
    private final int[] fpgas;


    /**
     * Invocation resources constructor.
     * 
     * @param cpus Assigned CPUs array
     * @param gpus Assigned GPUs array
     * @param fpgas Assigned FPGAs arrays
     */
    public InvocationResources(int[] cpus, int[] gpus, int[] fpgas) {
        this.cpus = cpus;
        this.gpus = gpus;
        this.fpgas = fpgas;
    }

    public int[] getAssignedCPUs() {
        return this.cpus;
    }

    public int[] getAssignedGPUs() {
        return this.gpus;
    }

    public int[] getAssignedFPGAs() {
        return this.fpgas;
    }

}
