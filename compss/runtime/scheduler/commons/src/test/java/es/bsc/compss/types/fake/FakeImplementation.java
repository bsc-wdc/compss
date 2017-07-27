package es.bsc.compss.types.fake;

import es.bsc.compss.types.implementations.Implementation;

public class FakeImplementation extends Implementation {

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
