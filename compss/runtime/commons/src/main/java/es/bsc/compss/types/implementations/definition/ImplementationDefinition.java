package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.TaskType;

import java.io.Externalizable;

public interface ImplementationDefinition extends Externalizable {

    public TaskType getTaskType();

    public String toShortFormat();

}
