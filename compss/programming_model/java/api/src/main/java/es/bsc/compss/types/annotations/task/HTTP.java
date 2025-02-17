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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
// @Repeatable(HTTP.class) //TODO Aldo
/**
 * HTTP Task definition
 */
public @interface HTTP {

    /**
     * Request type (GET, POST, etc.).
     * 
     * @return the request type of the Request.
     */
    String request() default "GET";

    /**
     * Payload string of the Request.
     *
     * @return the JSON Payload string of the Request.
     */
    String payload() default Constants.UNASSIGNED;

    /**
     * Payload string type.
     *
     * @return the JSON Payload string of the Request.
     */
    String payloadType() default "application/json";

    /**
     * Returns formatted Produces string of the Request.
     *
     * @return the method type of the Request.
     */
    String produces() default Constants.UNASSIGNED;

    /**
     * Resource of the service. i.e: /print?{{message}}
     *
     * @return the resource string.
     */
    String resource();

    /**
     * HTTP service name defined in the resources file.
     *
     * @return the Http service name string.
     */
    String serviceName();

    /**
     * Formatted string to update the defined key of the response JSON .
     *
     * @return formatted updates string.
     */
    String updates() default Constants.UNASSIGNED;

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
     * Returns if the task has priority or not.
     * 
     * @return if the task has priority or not.
     */
    String priority() default Constants.IS_NOT_PRIORITARY_TASK;

    /**
     * Returns the declaring class.
     *
     * @return the declaring class.
     */
    String declaringClass();

    /**
     * Returns the default return value in case of failures.
     *
     * @return default return string.
     */
    String defReturn() default Constants.UNASSIGNED;
}
