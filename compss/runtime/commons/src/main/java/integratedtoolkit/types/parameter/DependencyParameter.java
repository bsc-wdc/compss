package integratedtoolkit.types.parameter;

import integratedtoolkit.api.ITExecution.*;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.Transferable;


public class DependencyParameter extends Parameter implements Transferable {
	/**
	 * Serializable objects Version UID are 1L in all Runtime
	 */
	private static final long serialVersionUID = 1L;
	
    private DataAccessId daId;
    private Object dataSource;
    private String dataTarget;

    public DependencyParameter(ParamType type, ParamDirection direction) {
        super(type, direction);
    }

    public DataAccessId getDataAccessId() {
        return daId;
    }

    public void setDataAccessId(DataAccessId daId) {
        this.daId = daId;
    }

    public Object getDataSource() {
        return dataSource;
    }

    public void setDataSource(Object dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataTarget() {
        return this.dataTarget;
    }

    public void setDataTarget(String target) {
        this.dataTarget = target;
    }
}
