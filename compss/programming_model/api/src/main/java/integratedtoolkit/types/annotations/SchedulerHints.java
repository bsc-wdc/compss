package integratedtoolkit.types.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Scheduler hints definition
 *
 */
public @interface SchedulerHints {

    /**
     * Returns if the task must be replicated to all active workers
     * 
     * @return if the task must be replicated to all active workers
     */
    boolean isReplicated() default !Constants.REPLICATED_TASK;

    /**
     * Returns if the task must be evenly distributed among workers
     * 
     * @return if the task must be evenly distributed among workers
     */
    boolean isDistributed() default !Constants.DISTRIBUTED_TASK;

}
