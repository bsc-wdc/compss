package integratedtoolkit.types.fake;

import integratedtoolkit.types.Profile;


public class FakeProfile extends Profile {

    public FakeProfile(long avgTime) {
        super();
        super.averageTime = avgTime;
    }

    public void setStartTime(long start) {
        super.startTime = start;
    }
}
