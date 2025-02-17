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
public @interface Processor {

    /**
     * Returns the required processor computing units for the resource to run the CE.
     *
     * @return the required processor name to run the CE.
     */
    String computingUnits() default Constants.UNASSIGNED;

    /**
     * Returns the required processor name for the resource to run the CE.
     *
     * @return the required processor name to run the CE.
     */
    String name() default Constants.UNASSIGNED;

    /**
     * Returns the processor speed required for the resource to run the CE.
     *
     * @return the required processor speed to run the CE.
     */
    String speed() default Constants.UNASSIGNED;

    /**
     * Returns the required processor architecture for the resource to run the CE.
     *
     * @return the required architecture for the processor to run the CE.
     */
    String architecture() default Constants.UNASSIGNED;

    /**
     * Returns the required processor type for the resource to run the CE.
     *
     * @return the required type for the processor to run the CE.
     */
    String type() default Constants.UNASSIGNED;

    /**
     * Returns the processor internal memory required for the resource to run the CE.
     *
     * @return the required processor internal memory to run the CE.
     */
    String internalMemorySize() default Constants.UNASSIGNED;

    /**
     * Returns the name of the processor property required for the resource to run the CE.
     *
     * @return the name of the required processor property to run the CE.
     */
    String propertyName() default Constants.UNASSIGNED;

    /**
     * Returns the value of the processor property required for the resource to run the CE.
     *
     * @return the value of the required processor property to run the CE.
     */
    String propertyValue() default Constants.UNASSIGNED;
}
