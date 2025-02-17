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
package es.bsc.compss.types.job;

import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.worker.COMPSsException;


/**
 * Abstract Representation of a listener for the job execution.
 */
public interface JobListener<P extends Parameter> {

    /**
     * Actions to perform when the job stageIn is finished.
     */
    public void stageInCompleted();

    /**
     * Actions to perform when the job stageIn has finished with failed copies.
     *
     * @param numErrors number of failed copies
     */
    public void stageInFailed(int numErrors);

    /**
     * Actions to perform when the job has been submitted to the target worker.
     *
     * @param job submitted job
     */
    public void submitted(Job<?> job);

    /**
     * Actions to perform when the job has been submitted to the target worker at a given time.
     *
     * @param job submitted job
     * @param timestamp when the job arrived
     */
    public void submittedAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when the job has just arrived to the target worker.
     *
     * @param job submitted job
     */
    public void arrived(Job<?> job);

    /**
     * Actions to perform when the job has arrived to the target worker at a given time.
     *
     * @param job submitted job
     * @param timestamp when the job arrived
     */
    public void arrivedAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when all input data has just been copied into the executing worker.
     *
     * @param job job whose data has been copied
     */
    public void allInputDataOnWorker(Job<?> job);

    /**
     * Actions to perform when all input data was copied into the executing worker at the indicated time.
     *
     * @param job job whose data has been copied
     * @param timestamp when the job arrived
     */
    public void allInputDataOnWorkerAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when a job execution has just started.
     *
     * @param job job whose execution just started
     */
    public void startingExecution(Job<?> job);

    /**
     * Actions to perform when a job execution started at a given time.
     *
     * @param job job whose execution just started
     * @param timestamp when the job started executing
     */
    public void startingExecutionAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when a job execution just ended.
     *
     * @param job job whose execution just ended
     */
    public void endedExecution(Job<?> job);

    /**
     * Actions to perform when a job execution ended at a given time.
     *
     * @param job job whose execution just ended
     * @param timestamp when the job ended executing
     */
    public void endedExecutionAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when a job end notification has just been sent.
     *
     * @param job job whose execution end has just been notified
     */
    public void endNotified(Job<?> job);

    /**
     * Actions to perform when a job end notification was sent at a given time.
     *
     * @param job job whose execution end has just been notified
     * @param timestamp when the job end was sent
     */
    public void endNotifiedAt(Job<?> job, long timestamp);

    /**
     * Actions to perform when a job result is available.
     * 
     * @param p Produced parameter
     * @param dataName name of the data produced
     */
    public void resultAvailable(P p, String dataName);

    /**
     * Actions when job has successfully been cancelled.
     *
     * @param job Cancelled job
     */
    public void jobCancelled(Job<?> job);

    /**
     * Actions to perform when job has successfully ended.
     * 
     * @param job Completed job.
     */
    public void jobCompleted(Job<?> job);

    /**
     * Actions to perform when a job has failed.
     * 
     * @param job Failed job
     * @param status Failure status
     */
    public void jobFailed(Job<?> job, JobEndStatus status);

    /**
     * Actions to perform when a job raises an exception.
     * 
     * @param job Job that raised the exception
     * @param e Exception raised
     */
    public void jobException(Job<?> job, COMPSsException e);

}
