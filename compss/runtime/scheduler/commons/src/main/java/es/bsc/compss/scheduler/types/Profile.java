/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.types;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Class used to register the characteristics of the execution of one execution or generate a statistic summary of a set
 * of executions.
 * <p>
 * To obtain the characteristics of a single execution, a new Profile is created. At the beginning of the execution, the
 * setSubmissionTime method is invoked to obtain the necessary measurements to characterize the execution. Upon the end
 * of the execution, the end() method is called and the Profile class collects the necessary values, and analyzes their
 * initial value to prepare the execution summary.
 * </p>
 * <p>
 * To obtain the statistic report of several executions, their profiles need to be merged using the accumulate method.
 * (Accumulate overrides the internal values of the Profile, it is recommended to accumulate all the values on an empty
 * Profile). To generate a Profile instance with customized values, Profile.Builder enables the creation of a new
 * profile instance and set its initial values.
 * </p>
 * <p>
 * Data currently provided:
 * <ul>
 * <li>Execution time
 * </ul>
 * </p>
 */
public class Profile {

    protected static final long DEFAULT_EXECUTION_TIME = 100L;

    // One job Timestamps
    private long submitTS;
    private long arrivalTS;
    private long fetchedDataTS;
    private long executionStartTS;
    private long executionEndTS;
    private long endNotificationTS;
    private long endTS;

    // Global Statistics
    private long executions;
    private long minTime;
    private long averageTime;
    private long maxTime;


    /**
     * Creates a new profile instance.
     */
    public Profile() {
        this.executions = 0;
        this.minTime = Long.MAX_VALUE;
        this.averageTime = DEFAULT_EXECUTION_TIME;
        this.maxTime = Long.MIN_VALUE;
    }

    /**
     * Creates a new profile instance copying the given profile.
     * 
     * @param p Profile to copy.
     */
    public Profile(Profile p) {
        this.executions = p.executions;
        this.minTime = p.minTime;
        this.averageTime = p.averageTime;
        this.maxTime = p.maxTime;
    }

    /**
     * Creates a new profile instance loading the values from the given JSONObject.
     * 
     * @param json JSONObject containing the profile information.
     */
    public Profile(JSONObject json) {
        if (json != null) {
            try {
                this.executions = json.getLong("executions");
            } catch (JSONException je) {
                this.executions = 0;
            }
            try {
                this.minTime = json.getLong("minTime");
            } catch (JSONException je) {
                this.minTime = Long.MAX_VALUE;
            }
            try {
                this.averageTime = json.getLong("avgTime");
            } catch (JSONException je) {
                this.averageTime = DEFAULT_EXECUTION_TIME;
            }
            try {
                this.maxTime = json.getLong("maxTime");
            } catch (JSONException je) {
                this.maxTime = Long.MIN_VALUE;
            }
        } else {
            this.executions = 0;
            this.minTime = Long.MAX_VALUE;
            this.averageTime = DEFAULT_EXECUTION_TIME;
            this.maxTime = Long.MIN_VALUE;
        }
    }

    /**
     * Marks the job submission Time.
     *
     * @param ts submission timestamp
     */
    public final void setSubmissionTime(long ts) {
        this.submitTS = ts;
    }

    /**
     * Marks the job arrival time.
     *
     * @param ts arrival timestamp
     */
    public final void setArrivalTime(long ts) {
        this.arrivalTS = ts;
    }

    /**
     * Marks the moment when all the input data of the job has been fetched.
     *
     * @param ts data fetching timestamp
     */
    public final void setDataFetchingTime(long ts) {
        this.fetchedDataTS = ts;
    }

    /**
     * Marks the job execution start time.
     *
     * @param ts execution start timestamp
     */
    public final void setExecutionStartTime(long ts) {
        this.executionStartTS = ts;
    }

    /**
     * Marks the job execution end time.
     *
     * @param ts execution end timestamp
     */
    public final void setExecutionEndTime(long ts) {
        this.executionEndTS = ts;
    }

    /**
     * Marks the job end notification time.
     *
     * @param ts end notification timestamp
     */
    public final void setEndNotificationTime(long ts) {
        this.endNotificationTS = ts;
    }

    /**
     * Marks the job end.
     * 
     * @param ts end timestamp
     */
    public void end(long ts) {
        this.executions = 1;
        this.endTS = ts;
        this.averageTime = endTS - submitTS;
        this.minTime = this.averageTime;
        this.maxTime = this.averageTime;
    }

    /**
     * Returns the number of executions.
     * 
     * @return The number of executions.
     */
    public long getExecutionCount() {
        return this.executions;
    }

    /**
     * Returns the setSubmissionTime time.
     * 
     * @return The setSubmissionTime time.
     */
    public long getStartTime() {
        return this.submitTS;
    }

    /**
     * Returns the minimum execution time.
     * 
     * @return The minimum execution time.
     */
    public long getMinExecutionTime() {
        return this.minTime;
    }

    /**
     * Returns the average execution time.
     * 
     * @return The average execution time.
     */
    public long getAverageExecutionTime() {
        return this.averageTime;
    }

    /**
     * Returns the maximum execution time.
     * 
     * @return The maximum execution time.
     */
    public long getMaxExecutionTime() {
        return this.maxTime;
    }

    /**
     * Clears the number of executions.
     */
    public void clearExecutionCount() {
        this.executions = 0;
    }

    /**
     * Sets a new value for the number of executions.
     * 
     * @param executions The new number of executions.
     */
    public void setExecutions(long executions) {
        this.executions = executions;
    }

    /**
     * Sets a new min time.
     * 
     * @param minTime The new min time.
     */
    public void setMinTime(long minTime) {
        this.minTime = minTime;
    }

    /**
     * Sets a new max time.
     * 
     * @param maxTime The new max time.
     */
    public void setMaxTime(long maxTime) {
        this.maxTime = maxTime;
    }

    /**
     * Sets a new average time.
     * 
     * @param averageTime The new average time.
     */
    public void setAvgTime(long averageTime) {
        this.averageTime = averageTime;
    }

    /**
     * Accumulates the given profile into the current one.
     * 
     * @param p Profile to accumulate.
     */
    public <P extends Profile> void accumulate(P p) {
        Profile profile = (Profile) p;
        long totalExecutions = this.executions + profile.executions;
        if (totalExecutions > 0) {
            if (this.executions == 0) {
                this.minTime = profile.minTime;
                this.maxTime = profile.maxTime;
            } else {
                this.minTime = Math.min(this.minTime, profile.minTime);
                this.maxTime = Math.max(this.maxTime, profile.maxTime);
            }
            this.averageTime =
                (profile.averageTime * profile.executions + this.executions * this.averageTime) / totalExecutions;
            this.executions = totalExecutions;
        }
    }

    /**
     * Dumps the current profile to a JSON object.
     * 
     * @return A JSONObject containing the information of the current profile.
     */
    public JSONObject toJSONObject() {
        JSONObject jo = new JSONObject();
        jo.put("executions", this.executions);
        jo.put("minTime", this.minTime);
        jo.put("avgTime", this.averageTime);
        jo.put("maxTime", this.maxTime);
        return jo;
    }

    /**
     * Updates the given JSON object with the information in the current profile.
     * 
     * @param jo JSONObject representing a profile.
     * @return The updated JSON Object with the information in the current profile.
     */
    public JSONObject updateJSON(JSONObject jo) {
        long oldExecutions = 0;
        if (jo.has("executions")) {
            oldExecutions = jo.getLong("executions");
        }

        long oldAvg = 0;
        if (jo.has("avgTime")) {
            oldAvg = jo.getLong("avgTime");
        }

        jo.put("executions", this.executions);
        jo.put("minTime", this.minTime);
        jo.put("avgTime", this.averageTime);
        jo.put("maxTime", this.maxTime);

        JSONObject difference = new JSONObject();
        difference.put("executions", this.executions - oldExecutions);
        difference.put("minTime", this.minTime);
        difference.put("avgTime", this.averageTime);
        long oldTime = oldAvg * oldExecutions;
        long newTime = this.averageTime * this.executions;
        long newExecutions = this.executions - oldExecutions;
        if (newExecutions > 0) {
            difference.put("avgTime", (newTime - oldTime) / newExecutions);
        } else {
            difference.put("avgTime", 0);
        }

        return difference;
    }

    /**
     * Accumulates the information in the given JSONObject.
     * 
     * @param jo JSONObject representing a profile.
     */
    public void accumulateJSON(JSONObject jo) {
        long oldExecutions = 0;
        long oldMin = Long.MAX_VALUE;
        long oldMax = Long.MIN_VALUE;
        long oldAvg = 0;

        if (jo.has("executions")) {
            oldExecutions = jo.getLong("executions");
        }
        if (jo.has("minTime")) {
            oldMin = jo.getLong("minTime");
        }
        if (jo.has("maxTime")) {
            oldMax = jo.getLong("maxTime");
        }
        if (jo.has("avgTime")) {
            oldAvg = jo.getLong("avgTime");
        }

        jo.put("executions", this.executions + oldExecutions);

        if (this.executions > 0) {
            jo.put("minTime", Math.min(minTime, oldMin));
            jo.put("maxTime", Math.max(maxTime, oldMax));
            jo.put("avgTime", (oldAvg * oldExecutions + averageTime * executions) / (this.executions + oldExecutions));
        }

    }

    /**
     * Copies the current profile.
     * 
     * @return A copy of the current profile.
     */
    public Profile copy() {
        return new Profile(this);
    }

    /**
     * Returns a string representing the profile content.
     * 
     * @return A string representing the profile content.
     */
    protected String getContent() {
        return "executions=" + executions + " minTime=" + minTime + " avgTime=" + averageTime + " maxTime=" + maxTime;
    }

    @Override
    public String toString() {
        return "[Profile " + getContent() + "]";
    }

}
