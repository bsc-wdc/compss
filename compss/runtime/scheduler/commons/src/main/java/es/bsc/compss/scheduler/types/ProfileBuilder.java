package es.bsc.compss.scheduler.types;

public class ProfileBuilder {

    private long minExecutionTime = Long.MAX_VALUE;
    private long maxExecutionTime = Long.MIN_VALUE;
    private long avgExecutionTime = Profile.DEFAULT_EXECUTION_TIME;
    private long executions = 0;


    /**
     * Builds a new ProfileBuilder.
     */
    public ProfileBuilder() {
    }

    /**
     * Sets a new number of executions.
     * 
     * @param executions New number of executions.
     */
    public void setExecutions(long executions) {
        this.executions = executions;
    }

    /**
     * Sets a new minimum execution time.
     * 
     * @param minExecutionTime The new minimum execution time.
     */
    public void setMinExecutionTime(long minExecutionTime) {
        this.minExecutionTime = minExecutionTime;
    }

    /**
     * Sets a new average execution time.
     * 
     * @param avgExecutionTime The new average execution time.
     */
    public void setAvgExecutionTime(long avgExecutionTime) {
        this.avgExecutionTime = avgExecutionTime;
    }

    /**
     * Sets a new maximum execution time.
     * 
     * @param maxExecutionTime The new maximum execution time.
     */
    public void setMaxExecutionTime(long maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    /**
     * Builds a profile from the currently registered information.
     * 
     * @return The built profile.
     */
    public Profile build() {
        Profile p = new Profile();
        update(p);
        return p;
    }

    protected <P extends Profile> void update(P p) {
        Profile profile = (Profile) p;
        profile.setExecutions(this.executions);
        profile.setMinTime(this.minExecutionTime);
        profile.setAvgTime(this.avgExecutionTime);
        profile.setMaxTime(this.maxExecutionTime);
    }

}
