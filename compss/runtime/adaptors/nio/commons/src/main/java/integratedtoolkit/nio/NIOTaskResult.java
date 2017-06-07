package integratedtoolkit.nio;

import integratedtoolkit.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


public class NIOTaskResult implements Externalizable {

    private int taskId;

    private List<DataType> paramTypes = new LinkedList<>();
    // ATTENTION: Parameter Values will be empty if it doesn't contain a PSCO Id
    private List<Object> paramValues = new LinkedList<>();


    /**
     * Only for externalization
     */
    public NIOTaskResult() {
        // All attributes are initialized statically
    }

    /**
     * New task result from a given set of {@code params}
     * 
     * @param taskId
     * @param params
     */
    public NIOTaskResult(int taskId, LinkedList<NIOParam> params) {
        this.taskId = taskId;

        for (NIOParam np : params) {
            this.paramTypes.add(np.getType());

            switch (np.getType()) {
                case PSCO_T:
                case EXTERNAL_OBJECT_T:
                    this.paramValues.add(np.getValue());
                    break;
                default:
                    // We add a NULL for any other type
                    this.paramValues.add(null);
                    break;
            }
        }
    }

    /**
     * Returns the task id associated to the result
     * 
     * @return
     */
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the parameter types
     * 
     * @return
     */
    public List<DataType> getParamTypes() {
        return this.paramTypes;
    }

    /**
     * Returns the value of the parameter {@code i}
     * 
     * @param i
     * @return
     */
    public Object getParamValue(int i) {
        return this.paramValues.get(i);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.taskId = in.readInt();
        this.paramTypes = (LinkedList<DataType>) in.readObject();
        this.paramValues = (LinkedList<Object>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.taskId);
        out.writeObject(this.paramTypes);
        out.writeObject(this.paramValues);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK_RESULT ");
        sb.append("[TASK ID= ").append(this.taskId).append("]");
        sb.append("[PARAM_TYPES");
        for (DataType param : this.paramTypes) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("[PARAM_VALUES");
        for (Object param : this.paramValues) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

}
