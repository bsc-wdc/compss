package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class FakeImplementation extends Implementation {

    public FakeImplementation(int coreId, int implementationId, WorkerResourceDescription annot) {
        super(coreId, implementationId, annot);
    }

    @Override
    public Type getType() {
        return null;
    }

    public String toString() {
        return " fakeImplementation that requires " + getRequirements();
    }

}
