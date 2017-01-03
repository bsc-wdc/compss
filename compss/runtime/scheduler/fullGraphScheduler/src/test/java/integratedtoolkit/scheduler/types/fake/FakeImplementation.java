package integratedtoolkit.scheduler.types.fake;

import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class FakeImplementation<T extends WorkerResourceDescription> extends Implementation<T> {

    @SuppressWarnings("unchecked")
    public FakeImplementation(int coreId, int implementationId, WorkerResourceDescription annot) {
        super(coreId, implementationId, (T) annot);
    }

    @Override
    public TaskType getTaskType() {
        return null;
    }

    @Override
    public String toString() {
        return " fakeImplementation that requires " + getRequirements();
    }

}
