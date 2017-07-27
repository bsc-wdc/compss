package es.bsc.compss.scheduler.types.fake;

import es.bsc.es.bsc.compss.scheduler.types.Profile;


public class FakeProfile extends Profile {

    public FakeProfile(long avgTime) {
        super();
        super.averageTime = avgTime;
    }

    public void setStartTime(long start) {
        super.startTime = start;
    }
}
