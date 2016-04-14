package integratedtoolkit.types.data.operation;

import integratedtoolkit.api.ITExecution.ParamType;
import integratedtoolkit.types.data.Transferable;


public class ObjectTransferable implements Transferable {

    Object source;
    String target;

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
    public ParamType getType() {
        return ParamType.FILE_T;
    }

}
