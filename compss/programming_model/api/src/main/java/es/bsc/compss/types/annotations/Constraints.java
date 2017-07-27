package es.bsc.compss.types.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Constraints definition
 * 
 */
public @interface Constraints {

    /**
     * Returns the processors required for the resource to run the CE
     *
     */
    Processor[] processors() default {};

    /**
     * Returns the number of computing units required for the resource to run the CE
     * 
     * @return the required computing units to run the CE
     */
    String computingUnits() default Constants.UNASSIGNED;

    /**
     * Returns the required processor name for the resource to run the CE
     * 
     * @return the required processor name to run the CE
     */
    String processorName() default Constants.UNASSIGNED;

    /**
     * Returns the processor speed required for the resource to run the CE
     * 
     * @return the required processor speed to run the CE
     */
    String processorSpeed() default Constants.UNASSIGNED;

    /**
     * Returns the required processor architecture for the resource to run the CE
     *
     * @return the required architecture for the processor to run the CE
     */
    String processorArchitecture() default Constants.UNASSIGNED;

    /**
     * Returns the required processor internal memory for the resource to run the CE
     *
     * @return the required internal memory size for the processor to run the CE
     */
    String processorInternalMemorySize() default Constants.UNASSIGNED;

    /**
     * Returns the required processor type for the resource to run the CE
     *
     * @return the required type for the processor to run the CE
     */
    String processorType() default Constants.UNASSIGNED_PROCESSOR_TYPE;

    /**
     * Returns the name of the processor property required for the resource to run the CE
     * 
     * @return the name of the required processor property to run the CE
     */
    String processorPropertyName() default Constants.UNASSIGNED;

    /**
     * Returns the value of the processor property required for the resource to run the CE
     * 
     * @return the value of the required processor property to run the CE
     */
    String processorPropertyValue() default Constants.UNASSIGNED;

    /**
     * Returns the memory size required for the resource to run the CE
     * 
     * @return the required memory size to run the CE
     */
    String memorySize() default Constants.UNASSIGNED;

    /**
     * Returns the memory type required for the resource to run the CE
     * 
     * @return the required memory type to run the CE
     */
    String memoryType() default Constants.UNASSIGNED;

    /**
     * Returns the storage size required for the resource to run the CE
     * 
     * @return the required storage size to run the CE
     */
    String storageSize() default Constants.UNASSIGNED;

    /**
     * Returns the storage type required for the resource to run the CE
     * 
     * @return the required storage type to run the CE
     */
    String storageType() default Constants.UNASSIGNED;

    /**
     * Returns the Operating System type required for the resource to run the CE
     * 
     * @return the required Operating System type to run the CE
     */
    String operatingSystemType() default Constants.UNASSIGNED;

    /**
     * Returns the Operating System distribution required for the resource to run the CE
     * 
     * @return the required Operating System distribution to run the CE
     */
    String operatingSystemDistribution() default Constants.UNASSIGNED;

    /**
     * Returns the Operating System version required for the resource to run the CE
     * 
     * @return the required Operating System version to run the CE
     */
    String operatingSystemVersion() default Constants.UNASSIGNED;

    /**
     * Returns the software applications required for the resource to run the CE
     * 
     * @return the required software applications to run the CE
     */
    String appSoftware() default Constants.UNASSIGNED;

    /**
     * Returns the host queues where the CE can be run
     * 
     * @return the host queues where to run the CE
     */
    String hostQueues() default Constants.UNASSIGNED;

    /**
     * Returns the wallClockLimit required for the resource to run the CE
     * 
     * @return the required wallClockLimit to run the CE
     */
    String wallClockLimit() default Constants.UNASSIGNED;

}
