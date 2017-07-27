package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.Implementation;


public class FakeImplementation extends Implementation<FakeResourceDescription> {

    public FakeImplementation(int coreId, int implementationId, FakeResourceDescription annot) {
        super(coreId, implementationId, annot);
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
