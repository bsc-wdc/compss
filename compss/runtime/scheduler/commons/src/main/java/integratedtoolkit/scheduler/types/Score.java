package integratedtoolkit.scheduler.types;

import java.util.Set;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;


/**
 * Action score representation
 *
 */
public class Score implements Comparable<Score> {

    protected double actionScore; // Action Priority
    protected double resourceScore; // Resource Priority
    protected double waitingScore; // Resource Blocked Priority
    protected double implementationScore; // Implementation Priority


    /**
     * Constructor
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public Score(double actionScore, double res, double waiting, double impl) {
        this.actionScore = actionScore;
        this.resourceScore = res;
        this.waitingScore = waiting;
        this.implementationScore = impl;
    }

    /**
     * Clone
     * 
     * @param clone
     */
    public Score(Score clone) {
        this.actionScore = clone.actionScore;
        this.resourceScore = clone.resourceScore;
        this.waitingScore = clone.waitingScore;
        this.implementationScore = clone.implementationScore;
    }

    /**
     * Returns the action priority
     * 
     * @return
     */
    public double getActionScore() {
        return this.actionScore;
    }

    /**
     * Returns the estimated time of wait in the resource
     * 
     * @return
     */
    public double getWaitingScore() {
        return this.waitingScore;
    }

    /**
     * Returns the score of the resource (number of data in that resource)
     * 
     * @return
     */
    public double getResourceScore() {
        return this.resourceScore;
    }

    /**
     * Returns the implementation score
     * 
     * @return
     */
    public double getImplementationScore() {
        return this.implementationScore;
    }

    /**
     * Checks whether a score is better than another. Returns true if @a is better than @b
     * 
     * @param a
     * @param b
     * @return
     */
    public static final boolean isBetter(Score a, Score b) {
        if (a == null) {
            return false;
        }
        if (b == null) {
            return true;
        }
        return a.isBetter(b);
    }

    /**
     * Checks if the current score is better than the given. Returns true if @implicit is better than @other
     * 
     * @param other
     * @return
     */
    public boolean isBetter(Score other) {
        if (this.actionScore != other.actionScore) {
            return this.actionScore > other.actionScore;
        }
        if (this.resourceScore != other.resourceScore) {
            return this.resourceScore > other.resourceScore;
        }
        if (this.waitingScore != other.waitingScore) {
            return this.waitingScore > other.waitingScore;
        }
        return this.implementationScore > other.implementationScore;
    }

    @Override
    public int hashCode() {
        return this.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Score) {
            Score other = (Score) obj;
            return (this.actionScore == other.actionScore && this.resourceScore == other.resourceScore
                    && this.waitingScore == other.waitingScore && this.implementationScore == other.implementationScore);
        }

        return false;
    }

    @Override
    public int compareTo(Score other) {
        if (this.equals(other)) {
            return 0;
        } else if (this.isBetter(other)) {
            return 1;
        } else {
            return -1;
        }
    }

    public double calculateResourceScore(TaskDescription params, Worker<?, ?> w) {
        long resourceScore = 0;
        if (params != null) {
            Parameter[] parameters = params.getParameters();

            // Obtain the scores for the host: number of task parameters that
            // are located in the host
            for (Parameter p : parameters) {
                if (p instanceof DependencyParameter && p.getDirection() != Direction.OUT) {
                    DependencyParameter dp = (DependencyParameter) p;
                    DataInstanceId dId = null;
                    switch (dp.getDirection()) {
                        case IN:
                            DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dp.getDataAccessId();
                            dId = raId.getReadDataInstance();
                            break;
                        case INOUT:
                            DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) dp.getDataAccessId();
                            dId = rwaId.getReadDataInstance();
                            break;
                        case OUT:
                            // Cannot happen because of previous if
                            break;
                    }

                    // Get hosts for resource score
                    if (dId != null) {
                        LogicalData dataLD = Comm.getData(dId.getRenaming());
                        if (dataLD != null) {
                            Set<Resource> hosts = dataLD.getAllHosts();
                            for (Resource host : hosts) {
                                if (host == w) {
                                    resourceScore++;
                                }
                            }
                        }
                    }
                }
            }
        }
        return resourceScore;
    }

    @Override
    public String toString() {
        return "[Score = [action:" + this.actionScore + ", resource:" + this.resourceScore + ", load:" + this.waitingScore
                + ", implementation:" + this.implementationScore + "]" + "]";
    }

}
