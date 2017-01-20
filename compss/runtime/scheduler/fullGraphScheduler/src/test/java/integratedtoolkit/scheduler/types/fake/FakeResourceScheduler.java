package integratedtoolkit.scheduler.types.fake;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.types.resources.Worker;


public class FakeResourceScheduler extends FullGraphResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> {

    private long fakeLastGapStart;


    public FakeResourceScheduler(Worker<FakeResourceDescription, FakeImplementation> w,
            ActionOrchestrator<FakeProfile, FakeResourceDescription, FakeImplementation> orchestrator, long fakeLastGapStart) {

        super(w, orchestrator);
        this.fakeLastGapStart = fakeLastGapStart;
    }

    @Override
    public FakeProfile generateProfileForAllocatable() {
        return new FakeProfile(0);
    }

    @Override
    public long getLastGapExpectedStart() {
        return fakeLastGapStart;
    }

}