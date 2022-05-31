/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.job.JobEndStatus;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * Class containing all the information required to notify the end of an application.
 */
@XmlRootElement(name = "endApplication")
public class EndApplicationNotification {

    private String jobId;
    private JobEndStatus endStatus;
    private DataType[] paramTypes;
    private String[] paramLocations;
    private TaskProfile profile;


    public EndApplicationNotification() {
        // Nothing to do
    }

    /**
     * Constructs a new End application notification.
     *
     * @param jobId job of the ended application
     * @param status end status of the application execution
     * @param paramTypes array containing the types of the parameters of the job
     * @param paramLocations locations where to find the values of the job parameters
     * @param profile Profiling information related to the job execution
     */
    public EndApplicationNotification(String jobId, JobEndStatus status, DataType[] paramTypes, String[] paramLocations,
        TaskProfile profile) {

        this.jobId = jobId;
        this.endStatus = status;
        this.paramTypes = paramTypes;
        this.paramLocations = paramLocations;
        this.profile = profile;
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

    public TaskProfile getProfile() {
        return profile;
    }

    public void setProfile(TaskProfile profile) {
        this.profile = profile;
    }

}
