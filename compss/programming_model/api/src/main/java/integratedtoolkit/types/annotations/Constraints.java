package integratedtoolkit.types.annotations;

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
     * Returns the number of computing units required for the resource to run the CE
     * 
     * @return the required computing units to run the CE
     */
    int computingUnits() default Constants.UNASSIGNED_INT;

    /**
     * Returns the number of computing nodes required to run the CE
     * 
     * @return the required computing nodes to run the CE
     */
    int computingNodes() default Constants.UNASSIGNED_INT;

    /**
     * Returns the required processor name for the resource to run the CE
     * 
     * @return the required processor name to run the CE
     */
    String processorName() default Constants.UNASSIGNED_STR;

    /**
     * Returns the processor speed required for the resource to run the CE
     * 
     * @return the required processor speed to run the CE
     */
    float processorSpeed() default Constants.UNASSIGNED_FLOAT;

    /**
     * Returns the required processor architecture for the resource to run the CE
     *
     * @return the required architecture for the processor to run the CE
     */
    String processorArchitecture() default Constants.UNASSIGNED_STR;

    /**
     * Returns the name of the processor property required for the resource to run the CE
     * 
     * @return the name of the required processor property to run the CE
     */
    String processorPropertyName() default Constants.UNASSIGNED_STR;

    /**
     * Returns the value of the processor property required for the resource to run the CE
     * 
     * @return the value of the required processor property to run the CE
     */
    String processorPropertyValue() default Constants.UNASSIGNED_STR;

    /**
     * Returns the memory size required for the resource to run the CE
     * 
     * @return the required memory size to run the CE
     */
    float memorySize() default Constants.UNASSIGNED_FLOAT;

    /**
     * Returns the memory type required for the resource to run the CE
     * 
     * @return the required memory type to run the CE
     */
    String memoryType() default Constants.UNASSIGNED_STR;

    /**
     * Returns the storage size required for the resource to run the CE
     * 
     * @return the required storage size to run the CE
     */
    float storageSize() default Constants.UNASSIGNED_FLOAT;

    /**
     * Returns the storage type required for the resource to run the CE
     * 
     * @return the required storage type to run the CE
     */
    String storageType() default Constants.UNASSIGNED_STR;

    /**
     * Returns the Operating System type required for the resource to run the CE
     * 
     * @return the required Operating System type to run the CE
     */
    String operatingSystemType() default Constants.UNASSIGNED_STR;

    /**
     * Returns the Operating System distribution required for the resource to run the CE
     * 
     * @return the required Operating System distribution to run the CE
     */
    String operatingSystemDistribution() default Constants.UNASSIGNED_STR;

    /**
     * Returns the Operating System version required for the resource to run the CE
     * 
     * @return the required Operating System version to run the CE
     */
    String operatingSystemVersion() default Constants.UNASSIGNED_STR;

    /**
     * Returns the software applications required for the resource to run the CE
     * 
     * @return the required software applications to run the CE
     */
    String appSoftware() default Constants.UNASSIGNED_STR;

    /**
     * Returns the host queues where the CE can be run
     * 
     * @return the host queues where to run the CE
     */
    String hostQueues() default Constants.UNASSIGNED_STR;

    /**
     * Returns the wallClockLimit required for the resource to run the CE
     * 
     * @return the required wallClockLimit to run the CE
     */
    int wallClockLimit() default Constants.UNASSIGNED_INT;

}
