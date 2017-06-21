package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.request.exceptions.ShutdownException;

/**
 * The ActionUpdate class represents the notification of an update on the state
 * of an allocatable action.
 */
public class ActionUpdate extends TDRequest {

    /**
     * The updated allocatable action
     */
    private final AllocatableAction action;

    /**
     * Possible Updates applied to the action.
     */
    public static enum Update {

        /**
         * There has been an error during the execution of the action.
         */
        ERROR,
        /**
         * The action execution has succedded.
         */
        COMPLETED
    }

    /**
     * Update to be notified.
     */
    private final Update update;

    /**
     * Constructs a new NotifyAllocatableActionEnd for the task
     *
     * @param action
     * @param update update to be notified
     */
    public ActionUpdate(AllocatableAction action, Update update) {
        this.action = action;
        this.update = update;
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.ACTION_UPDATE;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        if (update == Update.COMPLETED) {
            ts.actionCompleted(action);
        } else {
            ts.errorOnAction(action);
        }
    }

}
