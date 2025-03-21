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
import es.bsc.compss.types.annotations.task.repeatables.Binaries;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Binaries.class)
/**
 * Methods definition
 */
public @interface Binary {

    /*
     * Binary DEFINITION
     * 
     */

    /**
     * Returns the binary name.
     * 
     * @return the binary name.
     */
    String binary() default Constants.UNASSIGNED;

    /**
     * Returns the working directory of the binary.
     * 
     * @return the binary working directory.
     */
    String workingDir() default Constants.UNASSIGNED;

    /**
     * Returns the arguments string of the binary.
     *
     * @return the arguments definition string.
     */
    String args() default Constants.UNASSIGNED;

    /**
     * Indicates if the task will fail because of an exit value different from 0.
     * 
     * @return True if task will fail if exit value different from 0.
     */
    String failByExitValue() default Constants.NOT_FAIL_BY_EV;

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
