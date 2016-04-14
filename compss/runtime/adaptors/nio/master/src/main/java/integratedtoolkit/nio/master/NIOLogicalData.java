package integratedtoolkit.nio.master;

import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.URI;

public class NIOLogicalData extends LogicalData {

    public NIOLogicalData(String name) {
        super(name);
    }

    public void replicate(URI dest) {
        //Do nothing. It will be done later on, during the task execution
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Logical Data name: ").append(this.name).append(":\n");
        sb.append("\t Locations:\n");
        for (DataLocation dl : locations) {
            sb.append("\t\t * ").append(dl).append("\n");
        }
        return sb.toString();
    }
}
