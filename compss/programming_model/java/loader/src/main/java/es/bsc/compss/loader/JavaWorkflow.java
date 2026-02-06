package es.bsc.compss.loader;

import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.worker.COMPSsException;


public class JavaWorkflow implements Workflow {

    private final Workflow workflow;
    private final ObjectRegistry oReg;


    public JavaWorkflow(COMPSsRuntime runtime) {
        this(runtime, null, null);
    }

    public JavaWorkflow(COMPSsRuntime runtime, String parallelismSource, ApplicationRunner runner) {
        this.workflow = runtime.registerWorkflow(parallelismSource, runner);
        this.oReg = null;
    }

    /**
     * Access to the additional state associated with this workflow.
     */
    public ObjectRegistry getObjectRegistry() {
        return this.oReg;
    }

    @Override
    public Long getId() {
        return workflow.getId();
    }

    @Override
    public void deregister() {
        workflow.deregister();
    }

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier) {
        workflow.openTaskGroup(groupName, implicitBarrier);
    }

    @Override
    public void closeTaskGroup(String groupName) {
        workflow.closeTaskGroup(groupName);
    }

    @Override
    public void cancelTaskGroup(String groupName) throws COMPSsException {
        workflow.cancelTaskGroup(groupName);
    }

    @Override
    public void cancelApplicationTasks() {
        workflow.cancelApplicationTasks();
    }

    @Override
    public void noMoreTasks() {
        workflow.noMoreTasks();
    }

    @Override
    public void barrier() {
        workflow.barrier();
    }

    @Override
    public void barrier(boolean noMoreTasks) {
        workflow.barrier(noMoreTasks);
    }

    @Override
    public void barrierGroup(String groupName) throws COMPSsException {
        workflow.barrierGroup(groupName);
    }

    @Override
    public void snapshot() {
        workflow.snapshot();
    }

}
