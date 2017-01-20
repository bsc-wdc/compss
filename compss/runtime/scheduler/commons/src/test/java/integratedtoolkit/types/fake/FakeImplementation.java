package integratedtoolkit.types.fake;

import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.MethodResourceDescription;


public class FakeImplementation extends Implementation<MethodResourceDescription> {

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
