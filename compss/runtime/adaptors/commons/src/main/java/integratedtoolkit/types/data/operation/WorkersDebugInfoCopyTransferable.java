package integratedtoolkit.types.data.operation;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.types.data.Transferable;


public class WorkersDebugInfoCopyTransferable  implements Transferable {

    Object dataSource;
    String dataTarget;

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
    public ITExecution.ParamType getType() {
        return ITExecution.ParamType.FILE_T;
    }

}
