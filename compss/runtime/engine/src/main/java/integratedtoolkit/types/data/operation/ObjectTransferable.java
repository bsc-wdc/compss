package integratedtoolkit.types.data.operation;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.data.Transferable;


public class ObjectTransferable implements Transferable {

    private Object source;
    private String target;


    public ObjectTransferable() {
    }

    @Override
    public Object getDataSource() {
        return source;
    }

    @Override
    public void setDataSource(Object dataSource) {
        this.source = dataSource;
    }

    @Override
    public String getDataTarget() {
        return target;
    }

    @Override
    public void setDataTarget(String target) {
        this.target = target;
    }

    @Override
    public DataType getType() {
        return DataType.FILE_T;
    }

}
