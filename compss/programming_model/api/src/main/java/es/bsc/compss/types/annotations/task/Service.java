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
package es.bsc.compss.types.annotations.task;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.task.repeatables.Services;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Services.class)
/**
 * Services definition
 */
public @interface Service {

    /*
     * SERVICE DEFINITION
     */

    /**
     * Returns the namespace of the Service.
     * 
     * @return the namespace of the Service.
     */
    String namespace();

    /**
     * Returns the name of the Service.
     * 
     * @return the name of the Service.
     */
    String name();

    /*
     * SERVICE PROPERTIES
     * 
     */

    /**
     * Returns the port of the Service.
     * 
     * @return the port of the Service.
     */
    String port();

    /**
     * Returns Whether task is IO.
     * 
     * @return Whether task is IO.
     */
    boolean isIO() default false;

    /**
     * Returns the operation name of the Service.
     * 
     * @return the operation name of the Service.
     */
    String operation() default Constants.UNASSIGNED;

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

}
