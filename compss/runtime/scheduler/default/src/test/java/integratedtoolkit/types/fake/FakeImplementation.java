package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class FakeImplementation<T extends WorkerResourceDescription> extends Implementation<T> {

    public FakeImplementation(int coreId, int implementationId, WorkerResourceDescription annot) {
        super(coreId, implementationId, (T) annot);
    }

    @Override
    public Type getType() {
        return null;
    }

    public String toString() {
        return " fakeImplementation that requires " + getRequirements();
    }

}
