package integratedtoolkit.types.fake;

import integratedtoolkit.types.implementations.Implementation;

public class FakeImplementation extends Implementation {

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
