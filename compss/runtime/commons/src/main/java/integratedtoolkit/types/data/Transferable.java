package integratedtoolkit.types.data;

import integratedtoolkit.api.ITExecution.ParamType;


public interface Transferable {

    Object getDataSource();

    void setDataSource(Object dataSource);

    String getDataTarget();

    void setDataTarget(String target);
    
    ParamType getType();
}
