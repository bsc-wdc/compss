package integratedtoolkit.types;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.SCOParameter;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ResourceManager;

import java.util.HashSet;
import java.util.List;


public class Score {

    protected long actionScore;
    protected long resourceScore;
    protected long implementationScore;

    public Score(long actionScore, long res, long impl) {
        this.actionScore = actionScore;
        this.resourceScore = res;
        this.implementationScore = impl;
    }

    public Score(Score s, long resource, long impl) {
        actionScore = s.actionScore;
        resourceScore = resource;
        implementationScore = impl;
    }

    public Score(Score s, long impl) {
        actionScore = s.actionScore;
        resourceScore = s.resourceScore;
        implementationScore = impl;
    }

    public boolean isBetter(Score other) {
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        if (resourceScore != other.resourceScore) {
            return resourceScore > other.resourceScore;
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

    public static long getLocalityScore(TaskParams params, Worker<?> w) {
        long resourceScore = 0;
        if (params != null) {
            Parameter[] parameters = params.getParameters();

            // Obtain the scores for the host: number of task parameters that are located in the host
            for (Parameter p : parameters) {
                if (p instanceof DependencyParameter && p.getDirection() != DataDirection.OUT) {

                    DataType type = p.getType();

                    if (type == DataType.SCO_T) {
                        SCOParameter scop = (SCOParameter) p;
                        PSCOId pscoId = Comm.getPSCOId(scop.getCode());
                        if (pscoId != null) {
                            scop.setType(DataType.PSCO_T);
                            scop.setValue(pscoId);
                        }
                    }

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
                            break;
                    }
                    if (dId != null) {
                        if (type == DataType.PSCO_T) {
                            List<String> backends = Comm.getPSCOLocations((SCOParameter) p);
                            for (String backendID : backends) {
                                Resource host = ResourceManager.getWorker(backendID);
                                if (host == w) {
                                    resourceScore++;
                                }
                            }

                        } else {
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
        }
        return resourceScore;
    }

    public String toString() {
        return "[action:" + actionScore + ", resource:" + resourceScore + ", implementation:" + implementationScore + "]";
    }
}
