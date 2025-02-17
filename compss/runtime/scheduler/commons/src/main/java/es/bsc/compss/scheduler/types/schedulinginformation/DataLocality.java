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
package es.bsc.compss.scheduler.types.schedulinginformation;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.LocationScoreMonitor;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LocationMonitor;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import java.util.List;
import java.util.Set;


public class DataLocality extends SchedulingInformation {

    /**
     * Creates a new Scheduling Information instance.
     * 
     * @param enforcedTargetResource Enforced resource.
     * @param params Parameters accessed by the action
     * @param coreId core element Id
     */
    public <T extends WorkerResourceDescription> DataLocality(ResourceScheduler<T> enforcedTargetResource,
        List<? extends Parameter> params, Integer coreId) {
        super(enforcedTargetResource);
        if (enforcedTargetResource == null && coreId != null) {
            if (params != null) {
                registerLocalityScoreMonitoring(params);
            }
        }
    }

    private void registerLocalityScoreMonitoring(List<? extends Parameter> params) {
        for (Parameter p : params) {
            registerLocalityScoreMonitoring(p);
        }
    }

    private void registerLocalityScoreMonitoring(Parameter p) {
        if (p.isPotentialDependency() && p.getDirection() != Direction.OUT) {
            if (p.isCollective()) {
                CollectiveParameter cp = (CollectiveParameter) p;
                registerLocalityScoreMonitoring(cp.getElements());
            } else {
                DependencyParameter dp = (DependencyParameter) p;
                DataInstanceId dId = null;
                DataAccessId access = dp.getDataAccessId();
                if (access != null && access.isRead()) {
                    ReadingDataAccessId raId = (ReadingDataAccessId) access;
                    dId = raId.getReadDataInstance();
                }
                if (dId != null) {
                    LogicalData dataLD = dId.getData();

                    if (dataLD != null) {
                        // Update current locality score
                        Set<Resource> hosts = dataLD.getAllHosts();
                        increasePreregisteredScores(hosts, p.getWeight());
                        LocationMonitor monitor = new LocationScoreMonitor(this, p.getWeight());
                        // Register future score monitoring
                        dataLD.registerLocationMonitor(monitor);
                    }

                }
            }
        }
        // Basic types and outputs have 0 locality score. Ignore them.
    }

}
