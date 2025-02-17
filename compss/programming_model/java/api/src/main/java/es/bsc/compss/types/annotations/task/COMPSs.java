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
package es.bsc.compss.types.annotations.task;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.task.repeatables.MultiCOMPSs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(MultiCOMPSs.class)
/**
 * Methods definition
 */
public @interface COMPSs {

    /*
     * COMPSs DEFINITION
     * 
     */

    /**
     * Returns the runcompss command path.
     * 
     * @return the runcompss command path.
     */
    String runcompss() default Constants.UNASSIGNED;

    /**
     * Returns the COMPSs application main class.
     * 
     * @return the COMPSs application main class.
     */
    String appName() default Constants.UNASSIGNED;

    /**
     * Returns the COMPSs application main class.
     * 
     * @return the COMPSs application main class.
     */
    String appArgs() default Constants.UNASSIGNED;

    /*
     * COMPSs PROPERTIES
     * 
     */

    /**
     * Returns the runcompss flags.
     * 
     * @return the runcompss flags.
     */
    String flags() default Constants.UNASSIGNED;

    /**
     * Returns if a worker can run on the master resource or not.
     * 
     * @return whether the worker can run on the master resource or not.
     */
    String workerInMaster() default Constants.WORKER_IN_MASTER;

    /**
     * Returns the working directory of the binary.
     * 
     * @return the binary working directory.
     */
    String workingDir() default Constants.UNASSIGNED;

    /**
     * Returns the number of computing nodes required.
     * 
     * @return the number of computing nodes required.
     */
    String computingNodes() default Constants.UNASSIGNED;

    /**
     * Indicates if the task will fail because of an exit value different from 0.
     * 
     * @return True if task will fail if exit value different from 0.
     */
    boolean failByExitValue() default false;

    /*
     * COMMON PROPERTIES
     * 
     */

    /**
     * Returns if the method has priority or not.
     * 
     * @return if the method has priority or not.
     */
    String priority() default Constants.IS_NOT_PRIORITARY_TASK;

    /**
     * Returns the method specific constraints.
     * 
     * @return the method specific constraints.
     */
    Constraints constraints() default @Constraints();

}
