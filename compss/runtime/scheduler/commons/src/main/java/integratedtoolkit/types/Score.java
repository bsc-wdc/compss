package integratedtoolkit.types;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;

import java.util.HashSet;


public class Score {

    protected double actionScore; //Action Priority
    protected double waitingScore; // Resource Ready Priority 
    protected double resourceScore; // Resource Priority
    protected double implementationScore; //Implementation Priority


    public Score(double actionScore, double waiting, double res, double impl) {
        this.actionScore = actionScore;
        this.waitingScore = waiting;
        this.resourceScore = res;
        this.implementationScore = impl;
    }

    public Score(Score s, double waiting, double resource, double impl) {
        actionScore = s.actionScore;
        waitingScore = waiting;
        resourceScore = resource;
        implementationScore = impl;
    }

    public Score(Score s, double waiting, double impl) {
        actionScore = s.actionScore;
        resourceScore = s.resourceScore;
        waitingScore = waiting;
        implementationScore = impl;
    }
    
    public Score(Score s, double impl) {
        actionScore = s.actionScore;
        resourceScore = s.resourceScore;
        waitingScore = s.waitingScore;
        implementationScore = impl;
    }

    public boolean isBetter(Score other) {
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        if (resourceScore != other.resourceScore) {
            return resourceScore > other.resourceScore;
        }
        if (waitingScore != other.waitingScore) {
            return waitingScore > other.waitingScore;
        }
        return this.implementationScore > other.implementationScore;
    }

    public static final boolean isBetter(Score a, Score b) {
        if (a == null) {
            return false;
        }
        if (b == null) {
            return true;
        }
        return a.isBetter(b);
    }

    public static long getLocalityScore(TaskDescription params, Worker<?> w) {
        long resourceScore = 0;
        if (params != null) {
            Parameter[] parameters = params.getParameters();

            // Obtain the scores for the host: number of task parameters that are located in the host
            for (Parameter p : parameters) {
                if (p instanceof DependencyParameter && p.getDirection() != DataDirection.OUT) {
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
                        HashSet<Resource> hosts = Comm.getData(dId.getRenaming()).getAllHosts();
                        for (Resource host : hosts) {
                        	if (host == w) {
                                resourceScore++;
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
        return "[action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore +", implementation:" + implementationScore + "]";
    }

}
