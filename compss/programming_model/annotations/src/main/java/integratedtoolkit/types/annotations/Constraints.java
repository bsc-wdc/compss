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

    public static final String UNASSIGNED = "[unassigned]";

    /**
     * Returns the required processor architecture for the resource to run the
     * CE.
     *
     * @return the required architecture for the processor to run the CE
     */
    String processorArchitecture() default UNASSIGNED;

    /**
     * Returns the required number of CPUs for the host to run the CE.
     *
     * @return the required number of CPUs for the host to run the CE.
     */
    int processorCPUCount() default 0;

    /**
     * Returns the required number of Cores for the host to run the CE.
     *
     * @return the required number of cores for the host to run the CE.
     */
    int processorCoreCount() default 0;

    /**
     * Returns the required processor frequency for the host to run the CE in
     * GHz.
     *
     * @return the required processor frequency for the host to run the CE in
     * GHz
     */
    float processorSpeed() default 0;

    /**
     * Returns the required physical memory size in GBs for the host to run the
     * CE.
     *
     * @return the required physical memory size in GBs for the host to run the
     * CE.
     */
    float memoryPhysicalSize() default 0;

    /**
     * Returns the required virtual memory size in GBs for the host to run the
     * CE.
     *
     * @return the required virtual memory size in GBs for the host to run the
     * CE.
     */
    float memoryVirtualSize() default 0;

    /**
     * Returns the top memory access time in nanoseconds for the host to run the
     * CE.
     *
     * @return the top memory access time in nanoseconds for the host to run the
     * CE.
     */
    float memoryAccessTime() default 0;

    /**
     * Returns the minimal memory bandwith in GB/s for the host to run the CE.
     *
     * @return the minimal memory bandwith in GB/s for the host to run the CE.
     */
    float memorySTR() default 0;

    /**
     * Returns the amount of required storage space in GB for the host to run
     * the CE.
     *
     * @return the amount of required storage space in GB for the host to run
     * the CE.
     */
    float storageElemSize() default 0;

    /**
     * Returns the top access time to the storage system in milliseconds for the
     * host to run the CE.
     *
     * @return the top access time to the storage system in milliseconds for the
     * host to run the CE.
     */
    float storageElemAccessTime() default 0;

    /**
     * Returns the minimal storage bandwith in MB/s for the host to run the CE.
     *
     * @return the minimal storage bandwith in MB/s for the host to run the CE.
     */
    float storageElemSTR() default 0;

    /**
     * Returns the required operative system for the resource to run the CE.
     *
     * @return the required operative system for the resource to run the CE.
     */
    String operatingSystemType() default UNASSIGNED;

    /**
     * Returns the required queues for the resource to run the CE.
     *
     * @return the required queues for the resource to run the CE.
     */
    String hostQueue() default UNASSIGNED;

    /**
     * Returns the required applications for the resource to run the CE.
     *
     * @return the required applications for the resource to run the CE.
     */
    String appSoftware() default UNASSIGNED;
    
    /**
     * Returns the wallClockLimit for running a CE in a resource.
     *
     * @return the required applications for the resource to run the CE.
     */
    int wallClockLimit() default 0;

}
