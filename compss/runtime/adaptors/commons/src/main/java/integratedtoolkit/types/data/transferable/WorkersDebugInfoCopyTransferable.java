package integratedtoolkit.types.data.transferable;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.data.Transferable;


public class WorkersDebugInfoCopyTransferable implements Transferable {

    private Object dataSource;
    private String dataTarget;


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
        return dataTarget;
    }

    @Override
    public void setDataTarget(String target) {
        this.dataTarget = target;
    }

    @Override
    public DataType getType() {
        return DataType.FILE_T;
    }

}
