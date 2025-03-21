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
package es.bsc.compss.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Scheduler hints definition
 */
public @interface SchedulerHints {

    /**
     * Returns if the task must be replicated to all active workers.
     * 
     * @return if the task must be replicated to all active workers.
     */
    String isReplicated() default Constants.IS_NOT_REPLICATED_TASK;

    /**
     * Returns if the task must be evenly distributed among workers.
     * 
     * @return if the task must be evenly distributed among workers.
     */
    String isDistributed() default Constants.IS_NOT_DISTRIBUTED_TASK;

}
