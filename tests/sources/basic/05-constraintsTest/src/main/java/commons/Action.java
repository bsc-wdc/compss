package commons;

import java.util.HashMap;
import java.util.LinkedList;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;


@SuppressWarnings({ "rawtypes", "unchecked" })
public class Action extends AllocatableAction {

    final int coreId;

    public Action(int coreId) {
        super(new SchedulingInformation());
        this.coreId = coreId;
    }

	@Override
    protected boolean areEnoughResources() {
    	Worker r = selectedResource.getResource();
        return r.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
    	Worker r = selectedResource.getResource();
        r.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
    	Worker r = selectedResource.getResource();
        r.endTask(selectedImpl.getRequirements());
    }

    @Override
    public LinkedList<ResourceScheduler<?>> getCompatibleWorkers() {
        return getCoreElementExecutors(coreId);
    }

    @Override
    public LinkedList<Implementation<?>> getCompatibleImplementations(ResourceScheduler<?> r) {
        return r.getExecutableImpls(coreId);
    }

    @Override
    public Implementation<?>[] getImplementations() {
        return CoreManager.getCoreImplementations(coreId);
    }

    @Override
    public boolean isCompatible(Worker<?> r) {
        return r.canRun(coreId);
    }

    @Override
    protected void doAction() {

    }

    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doError() throws FailedActionException {

    }

    @Override
    protected void doFailed() {

    }

    @Override
    public Score schedulingScore(TaskScheduler ts) {
        return new Score(0, 0, 0);
    }

    @Override
    public Score schedulingScore(ResourceScheduler<?> targetWorker, Score actionScore) {
        return new Score(0, 0, 0);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<?> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    public HashMap<Worker<?>, LinkedList<Implementation<?>>> findAvailableWorkers() {
        HashMap<Worker<?>, LinkedList<Implementation<?>>> m = new HashMap<Worker<?>, LinkedList<Implementation<?>>>();
        LinkedList<ResourceScheduler<?>> compatibleWorkers = getCoreElementExecutors(coreId);
        for (ResourceScheduler<?> ui : compatibleWorkers) {
            LinkedList<Implementation<?>> compatibleImpls = ui.getExecutableImpls(coreId);
            LinkedList<Implementation<?>> runnableImpls = new LinkedList<Implementation<?>>();
            for (Implementation<?> impl : compatibleImpls) {
            	Worker r = ui.getResource();
                if (r.canRunNow(impl.getRequirements())) {
                    runnableImpls.add(impl);
                }
            }
            if (runnableImpls.size() > 0) {
                m.put(ui.getResource(), runnableImpls);
            }
        }
        return m;
    }

    @Override
    public Integer getCoreId() {
        return coreId;
    }

}