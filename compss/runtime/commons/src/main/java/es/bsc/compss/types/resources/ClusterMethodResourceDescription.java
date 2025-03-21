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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.resources.components.Processor;

import java.util.Iterator;


public class ClusterMethodResourceDescription extends MethodResourceDescription {

    private int limitOfTasks;
    private int numClusters;


    @Override
    public MethodResourceType getMethodType() {
        return MethodResourceType.CLUSTER;
    }

    public int getNumClusters() {
        return numClusters;
    }

    public void setNumClusters(int numClusters) {
        this.numClusters = numClusters;
    }

    public void setLimitOfTasks(int lot) {
        this.limitOfTasks = lot;
    }

    public int getLimitOfTasks() {
        return this.limitOfTasks;
    }

    protected void dumpContent(StringBuilder sb) {
        super.dumpContent(sb);
        sb.append(",");
        sb.append("\"cluster\":{");
        sb.append("\"num_clusters\":").append(this.numClusters).append(",");
        sb.append("\"limit_of_tasks\":").append(this.limitOfTasks);
        sb.append("}");

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }
}
