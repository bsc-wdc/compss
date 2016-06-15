package integratedtoolkit.types.data;

import integratedtoolkit.api.COMPSsRuntime.DataType;


public interface Transferable {

    Object getDataSource();

    void setDataSource(Object dataSource);

    String getDataTarget();

    void setDataTarget(String target);
    
    DataType getType();
}
