package integratedtoolkit.types.parameter;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.Transferable;


public class DependencyParameter extends Parameter implements Transferable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private DataAccessId daId;
    private Object dataSource;
    private String dataTarget; // Full path with PROTOCOL


    public DependencyParameter(DataType type, DataDirection direction) {
        super(type, direction);
    }

    public DataAccessId getDataAccessId() {
        return daId;
    }

    public void setDataAccessId(DataAccessId daId) {
        this.daId = daId;
    }

    @Override
    public Object getDataSource() {
        return dataSource;
    }

    @Override
    public void setDataSource(Object dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String getDataTarget() {
        return this.dataTarget;
    }

    @Override
    public void setDataTarget(String target) {
        this.dataTarget = target;
    }

}
