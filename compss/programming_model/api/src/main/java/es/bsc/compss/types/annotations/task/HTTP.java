/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
// @Repeatable(HTTP.class) //TODO Aldo
/**
 * Services definition
 */
public @interface HTTP {

    // nm: new string payload
    /**
     * Returns the method type of the Request.
     * 
     * @return the method type of the Request.
     */
    String methodType();

    /**
     * Returns the method type of the Request.
     *
     * @return the method type of the Request.
     */
    String jsonPayload() default Constants.UNASSIGNED;

    /**
     * Returns the method type of the Request.
     *
     * @return the method type of the Request.
     */
    String reproduces() default Constants.UNASSIGNED;

    /**
     * Returns the base URL.
     *
     * @return the base URL.
     */
    String baseUrl();

    /*
     * COMMON PROPERTIES
     *
     */

    /**
     * Returns Whether task is IO.
     * 
     * @return Whether task is IO.
     */
    boolean isIO() default false;

    /**
     * Returns if the method has priority or not.
     * 
     * @return if the method has priority or not.
     */
    String priority() default Constants.IS_NOT_PRIORITARY_TASK;

    /**
     * Returns the declaring class.
     *
     * @return the declaring class.
     */
    String declaringClass();
}
