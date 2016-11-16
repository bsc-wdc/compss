package integratedtoolkit.scheduler.types;

public interface ActionOrchestrator {

    public void actionCompletion(AllocatableAction<?, ?> action);

    public void actionError(AllocatableAction<?, ?> action);
}