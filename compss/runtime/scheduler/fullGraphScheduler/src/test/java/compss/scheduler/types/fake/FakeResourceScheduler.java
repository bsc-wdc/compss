package es.bsc.compss.scheduler.types.fake;

import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.resources.Worker;


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
