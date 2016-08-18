package integratedtoolkit.nio;

import integratedtoolkit.api.COMPSsRuntime.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class NIOTaskResult implements Externalizable {

	private int taskId;
	
    private LinkedList<DataType> paramTypes = new LinkedList<DataType>();
    
    // ATTENTION: Param Values will be empty if it doesn't contain a PSCO Id
    private LinkedList<Object> paramValues = new LinkedList<Object>();
    
    
    public NIOTaskResult() {
    	
    }
    
    public NIOTaskResult(int taskId, LinkedList<NIOParam> params) {
        this.taskId = taskId;
        
        for (NIOParam np : params) {
        	paramTypes.add(np.getType());
        	if (np.getType().equals(DataType.PSCO_T)) {
        		paramValues.add(np.getValue());
        	} else {
        		paramValues.add(null);
        	}
        }
    }

	public int getTaskId() {
		return taskId;
	}
	
	public LinkedList<DataType> getParamTypes() {
		return this.paramTypes;
	}
	
	public Object getParamValue(int i) {
		return this.paramValues.get(i);
	}

	@SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		taskId = in.readInt();
		paramTypes = (LinkedList<DataType>) in.readObject();
		paramValues = (LinkedList<Object>) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
    	out.writeInt(taskId);
    	out.writeObject(paramTypes);
    	out.writeObject(paramValues);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK_RESULT ");
        sb.append("[TASK ID= ").append(taskId).append("]");
        sb.append("[PARAM_TYPES ");
        for (DataType param : paramTypes) {
            sb.append(param);
        }
        sb.append("]");
        sb.append("[PARAM_VALUES");
        for (Object param : paramValues) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }
}
