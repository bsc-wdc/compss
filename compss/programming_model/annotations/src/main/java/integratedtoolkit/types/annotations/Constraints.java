package integratedtoolkit.types.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 *
 */
public @interface Constraints {

    public static final String UNASSIGNED_STR 	= "[unassigned]";
    public static final int UNASSIGNED_INT 		= -1;
    public static final float UNASSIGNED_FLOAT 	= (float) -1.0;
    
    /**
     * Returns the number of computing units required for the resource to run the CE
     * 
     * @return the required computing units to run the CE
     */
    int computingUnits() default UNASSIGNED_INT;
    
    /**
     * Returns the required processor name for the resource to run the CE
     * 
     * @return the required processor name to run the CE
     */
    String processorName() default UNASSIGNED_STR;
    
    /**
     * Returns the processor speed required for the resource to run the CE
     * 
     * @return the required processor speed to run the CE
     */
    float processorSpeed() default UNASSIGNED_FLOAT;
    
    /**
     * Returns the required processor architecture for the resource to run the CE
     *
     * @return the required architecture for the processor to run the CE
     */
    String processorArchitecture() default UNASSIGNED_STR;
    
    /**
     * Returns the name of the processor property required for the resource to run the CE
     * 
     * @return the name of the required processor property to run the CE
     */
    String processorPropertyName() default UNASSIGNED_STR;
    
    /**
     * Returns the value of the processor property required for the resource to run the CE
     * 
     * @return the value of the required processor property to run the CE
     */
    String processorPropertyValue() default UNASSIGNED_STR;
    
    /**
     * Returns the memory size required for the resource to run the CE
     * 
     * @return the required memory size to run the CE
     */
    float memorySize() default UNASSIGNED_FLOAT;
    
    /**
     * Returns the memory type required for the resource to run the CE
     * 
     * @return the required memory type to run the CE
     */
    String memoryType() default UNASSIGNED_STR;
    
    /**
     * Returns the storage size required for the resource to run the CE
     * 
     * @return the required storage size to run the CE
     */
    float storageSize() default UNASSIGNED_FLOAT;
    
    /**
     * Returns the storage type required for the resource to run the CE
     * 
     * @return the required storage type to run the CE
     */
    String storageType() default UNASSIGNED_STR;
    
    /**
     * Returns the Operating System type required for the resource to run the CE
     * 
     * @return the required Operating System type to run the CE
     */
    String operatingSystemType() default UNASSIGNED_STR;
    
    /**
     * Returns the Operating System distribution required for the resource to run the CE
     * 
     * @return the required Operating System distribution to run the CE
     */
    String operatingSystemDistribution() default UNASSIGNED_STR;
    
    /**
     * Returns the Operating System version required for the resource to run the CE
     * 
     * @return the required Operating System version to run the CE
     */
    String operatingSystemVersion() default UNASSIGNED_STR;
    
    /**
     * Returns the software applications required for the resource to run the CE
     * 
     * @return the required software applications to run the CE
     */
    String appSoftware() default UNASSIGNED_STR;
    
    /**
     * Returns the host queues where the CE can be run
     * 
     * @return the host queues where to run the CE
     */
    String hostQueues() default UNASSIGNED_STR;
    
    /**
     * Returns the wallClockLimit required for the resource to run the CE
     * 
     * @return the required wallClockLimit to run the CE
     */
    int wallClockLimit() default UNASSIGNED_INT;
    
}
