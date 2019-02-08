/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.rest.types.messages;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "endApplication")
public class EndApplicationNotification {

    private String jobId;
    private JobEndStatus endStatus;
    private DataType[] paramTypes;
    private String[] paramLocations;

    public EndApplicationNotification() {
    }

    public EndApplicationNotification(String jobId, JobEndStatus status, DataType[] paramTypes, String[] paramLocations) {
        this.jobId = jobId;
        this.endStatus = status;
        this.paramTypes = paramTypes;
        this.paramLocations = paramLocations;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setEndStatus(JobEndStatus endStatus) {
        this.endStatus = endStatus;
    }

    public JobEndStatus getEndStatus() {
        return endStatus;
    }

    public DataType[] getParamTypes() {
        return paramTypes;
    }

    public void setParamTypes(DataType[] paramTypes) {
        this.paramTypes = paramTypes;
    }

    public String[] getParamLocations() {
        return paramLocations;
    }

    public void setParamLocations(String[] paramLocations) {
        this.paramLocations = paramLocations;
    }

}
